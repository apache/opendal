// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str;
use std::str::FromStr;

use async_tls::TlsConnector;
use async_trait::async_trait;
use bb8::PooledConnection;
use bb8::RunError;
use futures::io::copy;
use futures::AsyncReadExt;
use http::Uri;
use log::debug;
use suppaftp::list::File;
use suppaftp::types::FileType;
use suppaftp::types::Response;
use suppaftp::FtpError;
use suppaftp::FtpStream;
use suppaftp::Status;
use time::OffsetDateTime;
use tokio::sync::OnceCell;

use super::dir_stream::DirStream;
use super::dir_stream::ReadDir;
use super::util::FtpReader;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// FTP and FTPS services support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [ ] ~~scan~~
/// - [ ] ~~presign~~
/// - [ ] ~~multipart~~
/// - [ ] blocking
///
/// # Configuration
///
/// - `endpoint`: set the endpoint for connection
/// - `root`: Set the work directory for backend
/// - `credential`:  login credentials
/// - `tls`: tls mode
///
/// You can refer to [`FtpBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Ftp;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Ftp::default();
///
///     builder.endpoint("127.0.0.1");
///
///     let op: Operator = Operator::create(builder)?.finish();
///     let _obj: Object = op.object("test_file");
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct FtpBuilder {
    endpoint: Option<String>,
    root: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl Debug for FtpBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish()
    }
}

impl FtpBuilder {
    /// set endpoint for ftp backend.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// set root path for ftp backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// set user for ftp backend.
    pub fn user(&mut self, user: &str) -> &mut Self {
        self.user = if user.is_empty() {
            None
        } else {
            Some(user.to_string())
        };

        self
    }

    /// set password for ftp backend.
    pub fn password(&mut self, password: &str) -> &mut Self {
        self.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };

        self
    }
}

impl Builder for FtpBuilder {
    const SCHEME: Scheme = Scheme::Ftp;
    type Accessor = FtpBackend;

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("ftp backend build started: {:?}", &self);
        let endpoint = match &self.endpoint {
            None => {
                return Err(Error::new(
                    ErrorKind::BackendConfigInvalid,
                    "endpoint is empty",
                ))
            }
            Some(v) => v,
        };

        let endpoint_uri = match endpoint.parse::<Uri>() {
            Err(e) => {
                return Err(
                    Error::new(ErrorKind::BackendConfigInvalid, "endpoint is invalid")
                        .with_context("endpoint", endpoint)
                        .set_source(e),
                );
            }
            Ok(uri) => uri,
        };

        let host = endpoint_uri.host().unwrap_or("127.0.0.1");
        let port = endpoint_uri.port_u16().unwrap_or(21);

        let endpoint = format!("{host}:{port}");

        let enable_secure = match endpoint_uri.scheme_str() {
            Some("ftp") => false,
            // if the user forgot to add a scheme prefix
            // treat it as using secured scheme
            Some("ftps") | None => true,

            Some(s) => {
                return Err(Error::new(
                    ErrorKind::BackendConfigInvalid,
                    "endpoint is unsupported or invalid",
                )
                .with_context("endpoint", s));
            }
        };

        let root = normalize_root(&self.root.take().unwrap_or_default());

        let user = match &self.user {
            None => "".to_string(),
            Some(v) => v.clone(),
        };

        let password = match &self.password {
            None => "".to_string(),
            Some(v) => v.clone(),
        };

        debug!("ftp backend finished: {:?}", &self);

        Ok(FtpBackend {
            endpoint,
            root,
            user,
            password,
            enable_secure,
            pool: OnceCell::new(),
        })
    }

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = FtpBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("user").map(|v| builder.user(v));
        map.get("password").map(|v| builder.password(v));

        builder
    }
}

pub struct Manager {
    endpoint: String,
    root: String,
    user: String,
    password: String,
    enable_secure: bool,
}

#[async_trait]
impl bb8::ManageConnection for Manager {
    type Connection = FtpStream;
    type Error = FtpError;

    async fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        let stream = FtpStream::connect(&self.endpoint).await?;

        // switch to secure mode if ssl/tls is on.
        let mut ftp_stream = if self.enable_secure {
            stream
                .into_secure(TlsConnector::default().into(), &self.endpoint)
                .await?
        } else {
            stream
        };

        // login if needed
        if !self.user.is_empty() {
            ftp_stream.login(&self.user, &self.password).await?;
        }

        // change to the root path
        match ftp_stream.cwd(&self.root).await {
            Err(FtpError::UnexpectedResponse(e)) if e.status == Status::FileUnavailable => {
                ftp_stream.mkdir(&self.root).await?;
                // Then change to root path
                ftp_stream.cwd(&self.root).await?;
            }
            // Other errors, return.
            Err(e) => return Err(e),
            // Do nothing if success.
            Ok(_) => (),
        }

        ftp_stream.transfer_type(FileType::Binary).await?;

        Ok(ftp_stream)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.noop().await
    }

    /// Always allow reuse conn.
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

/// Backend is used to serve `Accessor` support for ftp.
#[derive(Clone)]
pub struct FtpBackend {
    endpoint: String,
    root: String,
    user: String,
    password: String,
    enable_secure: bool,
    pool: OnceCell<bb8::Pool<Manager>>,
}

impl Debug for FtpBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

#[async_trait]
impl Accessor for FtpBackend {
    type Reader = FtpReader;
    type BlockingReader = ();
    type Pager = DirStream;
    type BlockingPager = ();

    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Ftp)
            .set_root(&self.root)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            );

        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut ftp_stream = self.ftp_connect(Operation::Create).await?;

        let paths: Vec<&str> = path.split_inclusive('/').collect();

        let mut curr_path = String::new();

        for path in paths {
            curr_path.push_str(path);
            // try to create directory
            if curr_path.ends_with('/') {
                match ftp_stream.mkdir(&curr_path).await {
                    // Do nothing if status is FileUnavailable or OK(()) is return.
                    Err(FtpError::UnexpectedResponse(Response {
                        status: Status::FileUnavailable,
                        ..
                    }))
                    | Ok(()) => (),
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            } else {
                // else, create file
                ftp_stream.put_file(&curr_path, &mut "".as_bytes()).await?;
            }
        }

        return Ok(RpCreate::default());
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let mut ftp_stream = self.ftp_connect(Operation::Read).await?;

        let meta = self.ftp_stat(path).await?;

        let br = args.range();
        let (r, size): (input::Reader, _) = match (br.offset(), br.size()) {
            (Some(offset), Some(size)) => {
                ftp_stream.resume_transfer(offset as usize).await?;
                let ds = ftp_stream.retr_as_stream(path).await?.take(size);
                (Box::new(ds), min(size, meta.size() as u64 - offset))
            }
            (Some(offset), None) => {
                ftp_stream.resume_transfer(offset as usize).await?;
                let ds = ftp_stream.retr_as_stream(path).await?;
                (Box::new(ds), meta.size() as u64 - offset)
            }
            (None, Some(size)) => {
                ftp_stream
                    .resume_transfer((meta.size() as u64 - size) as usize)
                    .await?;
                let ds = ftp_stream.retr_as_stream(path).await?;
                (Box::new(ds), size)
            }
            (None, None) => {
                let ds = ftp_stream.retr_as_stream(path).await?;
                (Box::new(ds), meta.size() as u64)
            }
        };

        Ok((RpRead::new(size), FtpReader::new(r, ftp_stream)))
    }

    async fn write(&self, path: &str, _: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let mut ftp_stream = self.ftp_connect(Operation::Write).await?;

        let mut data_stream = ftp_stream.append_with_stream(path).await?;

        let bytes = copy(r, &mut data_stream).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "copy from ftp stream").set_source(err)
        })?;

        ftp_stream.finalize_put_stream(data_stream).await?;

        Ok(RpWrite::new(bytes))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // root dir, return default ObjectMetadata with Dir ObjectMode.
        if path == "/" {
            return Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)));
        }

        let file = self.ftp_stat(path).await?;

        let mode = if file.is_file() {
            ObjectMode::FILE
        } else if file.is_directory() {
            ObjectMode::DIR
        } else {
            ObjectMode::Unknown
        };
        let mut meta = ObjectMetadata::new(mode);
        meta.set_content_length(file.size() as u64);
        meta.set_last_modified(OffsetDateTime::from(file.modified()));

        Ok(RpStat::new(meta))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let mut ftp_stream = self.ftp_connect(Operation::Delete).await?;

        let result = if path.ends_with('/') {
            ftp_stream.rmdir(&path).await
        } else {
            ftp_stream.rm(&path).await
        };

        match result {
            Err(FtpError::UnexpectedResponse(Response {
                status: Status::FileUnavailable,
                ..
            }))
            | Ok(_) => (),
            Err(e) => {
                return Err(e.into());
            }
        }

        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let mut ftp_stream = self.ftp_connect(Operation::List).await?;

        let pathname = if path == "/" { None } else { Some(path) };
        let files = ftp_stream.list(pathname).await?;

        let rd = ReadDir::new(files);

        Ok((
            RpList::default(),
            DirStream::new(if path == "/" { "" } else { path }, rd, args.limit()),
        ))
    }
}

impl FtpBackend {
    async fn ftp_connect(&self, _: Operation) -> Result<PooledConnection<'static, Manager>> {
        let pool = self
            .pool
            .get_or_try_init(|| async {
                bb8::Pool::builder()
                    .max_size(64)
                    .build(Manager {
                        endpoint: self.endpoint.to_string(),
                        root: self.root.to_string(),
                        user: self.user.to_string(),
                        password: self.password.to_string(),
                        enable_secure: self.enable_secure,
                    })
                    .await
            })
            .await?;

        pool.get_owned().await.map_err(|err| match err {
            RunError::User(err) => err.into(),
            RunError::TimedOut => {
                Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary()
            }
        })
    }

    async fn ftp_stat(&self, path: &str) -> Result<File> {
        let mut ftp_stream = self.ftp_connect(Operation::Stat).await?;

        let (parent, basename) = (get_parent(path), get_basename(path));

        let pathname = if parent == "/" { None } else { Some(parent) };

        let resp = ftp_stream.list(pathname).await?;

        // Get stat of file.
        let mut files = resp
            .into_iter()
            .filter_map(|file| File::from_str(file.as_str()).ok())
            .filter(|f| f.name() == basename.trim_end_matches('/'))
            .collect::<Vec<File>>();

        if files.is_empty() {
            Err(Error::new(
                ErrorKind::ObjectNotFound,
                "file is not found during list",
            ))
        } else {
            Ok(files.remove(0))
        }
    }
}

#[cfg(test)]
mod build_test {
    use super::FtpBuilder;
    use crate::*;

    #[test]
    fn test_build() {
        // ftps scheme, should suffix with default port 21
        let mut builder = FtpBuilder::default();
        builder.endpoint("ftps://ftp_server.local");
        let b = builder.build();
        assert!(b.is_ok());

        // ftp scheme
        let mut builder = FtpBuilder::default();
        builder.endpoint("ftp://ftp_server.local:1234");
        let b = builder.build();
        assert!(b.is_ok());

        // no scheme
        let mut builder = FtpBuilder::default();
        builder.endpoint("ftp_server.local:8765");
        let b = builder.build();
        assert!(b.is_ok());

        // invalid scheme
        let mut builder = FtpBuilder::default();
        builder.endpoint("invalidscheme://ftp_server.local:8765");
        let b = builder.build();
        assert!(b.is_err());
        let e = b.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::BackendConfigInvalid);
    }
}
