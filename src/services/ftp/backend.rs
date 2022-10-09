// Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::str;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bb8::PooledConnection;
use bb8::RunError;
use futures::io::copy;
use futures::AsyncReadExt;
use http::Uri;
use log::info;
use suppaftp::async_native_tls::TlsConnector;
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
use super::err::new_ftp_error;
use super::util::FtpReader;
use crate::accessor::AccessorCapability;
use crate::error::new_other_backend_error;
use crate::error::new_other_object_error;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectStreamer;
use crate::Scheme;

/// Builder for ftp backend.
#[derive(Default)]
pub struct Builder {
    endpoint: Option<String>,
    root: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish()
    }
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "endpoint" => builder.endpoint(v),
                "user" => builder.user(v),
                "password" => builder.password(v),
                _ => continue,
            };
        }

        builder
    }

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

    /// Build a ftp backend.
    pub fn build(&mut self) -> Result<impl Accessor> {
        info!("ftp backend build started: {:?}", &self);
        let endpoint = match &self.endpoint {
            None => {
                return Err(new_other_backend_error(
                    HashMap::new(),
                    anyhow!("endpoint must be specified"),
                ))
            }
            Some(v) => v,
        };

        let endpoint_uri = match endpoint.parse::<Uri>() {
            Err(e) => {
                return Err(new_other_backend_error(
                    HashMap::new(),
                    anyhow!("endpoint must be valid uri: {:?}", e),
                ));
            }
            Ok(uri) => uri,
        };

        let host = endpoint_uri.host().unwrap_or("127.0.0.1");
        let port = endpoint_uri.port_u16().unwrap_or(21);

        let endpoint = format!("{}:{}", host, port);

        let enable_secure = match endpoint_uri.scheme_str() {
            Some("ftp") => false,
            // if the user forgot to add a scheme prefix
            // treat it as using secured scheme
            Some("ftps") | None => true,

            Some(s) => {
                return Err(new_other_backend_error(
                    HashMap::new(),
                    anyhow!("endpoint scheme unsupported or invalid: {:?}", s),
                ));
            }
        };

        let root = match &self.root {
            // set default path to '/'
            None => "/".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty());
                let mut v = v.clone();
                if !v.starts_with('/') {
                    return Err(new_other_backend_error(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("root must start with /"),
                    ));
                }
                if !v.ends_with('/') {
                    v.push('/');
                }
                v
            }
        };

        let user = match &self.user {
            None => "".to_string(),
            Some(v) => v.clone(),
        };

        let password = match &self.password {
            None => "".to_string(),
            Some(v) => v.clone(),
        };

        info!("ftp backend finished: {:?}", &self);

        Ok(Backend {
            endpoint,
            root,
            user,
            password,
            enable_secure,
            pool: OnceCell::new(),
        })
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
                .into_secure(TlsConnector::new(), &self.endpoint)
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

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        true
    }
}

/// Backend is used to serve `Accessor` support for ftp.
#[derive(Clone)]
pub struct Backend {
    endpoint: String,
    root: String,
    user: String,
    password: String,
    enable_secure: bool,
    pool: OnceCell<bb8::Pool<Manager>>,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Ftp)
            .set_root(&self.root)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            );

        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<()> {
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
                        return Err(new_other_object_error(
                            Operation::Create,
                            path,
                            anyhow!("mkdir request: {e:?}"),
                        ));
                    }
                }
            } else {
                // else, create file
                ftp_stream
                    .put_file(&curr_path, &mut "".as_bytes())
                    .await
                    .map_err(|e| {
                        new_other_object_error(
                            Operation::Create,
                            path,
                            anyhow!("put request: {e:?}"),
                        )
                    })?;
            }
        }

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_ftp_error(e, Operation::Create, path))?;

        return Ok(());
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let mut ftp_stream = self.ftp_connect(Operation::Read).await?;

        if let Some(offset) = args.offset() {
            ftp_stream
                .resume_transfer(offset as usize)
                .await
                .map_err(|e| {
                    new_other_object_error(
                        Operation::Read,
                        path,
                        anyhow!("resume transfer request: {e:?}"),
                    )
                })?;
        }

        let result = ftp_stream.retr_as_stream(path).await;
        match result {
            Err(FtpError::UnexpectedResponse(Response {
                status: Status::FileUnavailable,
                body: e,
            })) => {
                return Err(Error::new(ErrorKind::NotFound, e));
            }
            Err(e) => {
                return Err(new_other_object_error(
                    Operation::Read,
                    path,
                    anyhow!("retr request: {e:?}"),
                ));
            }
            Ok(_) => (),
        }

        // As we handle all error above, it is save to unwrap without panic.
        let data_stream = result.unwrap();

        let r: BytesReader = match args.size() {
            None => Box::new(FtpReader::new(Box::new(data_stream), ftp_stream, path)),

            Some(size) => Box::new(FtpReader::new(
                Box::new(data_stream.take(size)),
                ftp_stream,
                path,
            )),
        };
        Ok(r)
    }

    async fn write(&self, path: &str, _: OpWrite, r: BytesReader) -> Result<u64> {
        let mut ftp_stream = self.ftp_connect(Operation::Write).await?;

        let mut data_stream = ftp_stream.append_with_stream(path).await.map_err(|e| {
            new_other_object_error(Operation::Write, path, anyhow!("append request: {e:?}"))
        })?;

        let bytes = copy(r, &mut data_stream).await?;

        ftp_stream
            .finalize_put_stream(data_stream)
            .await
            .map_err(|e| {
                new_other_object_error(
                    Operation::Write,
                    path,
                    anyhow!("finalize put request: {e:?}"),
                )
            })?;

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_ftp_error(e, Operation::Write, path))?;

        Ok(bytes)
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        let mut p = path;
        let path: String;

        // root dir, return default ObjectMetadata with Dir ObjectMode.
        if p == "/" {
            return Ok(ObjectMetadata::new(ObjectMode::DIR));
        }

        let mut ftp_stream = self.ftp_connect(Operation::Stat).await?;

        // If given path points to a directory, split it into parent path and basename.
        if p.ends_with('/') {
            if let Some((basename, parent_path)) =
                p.split_inclusive('/').collect::<Vec<&str>>().split_last()
            {
                path = parent_path.join("");
                p = &basename[..basename.len() - 1];
            } else {
                path = "".to_string();
            }
        } else {
            // otherwise, directly use the path provided by arg.
            path = p.to_string();
        }

        let resp = ftp_stream.list(Some(&path)).await.map_err(|e| {
            new_other_object_error(Operation::Stat, &path, anyhow!("list request: {e:?}"))
        })?;

        // Get stat of file.
        let files = if p == path {
            resp.into_iter()
                .filter_map(|file| File::from_str(file.as_str()).ok())
                .collect::<Vec<File>>()
        // Get stat of directory.
        } else {
            resp.into_iter()
                .filter_map(|file| File::from_str(file.as_str()).ok())
                .filter(|f| f.name() == p)
                .collect::<Vec<File>>()
        };

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_ftp_error(e, Operation::Stat, &path))?;

        if files.is_empty() {
            Err(Error::new(ErrorKind::NotFound, "Not Found"))
        } else {
            let file = files.get(0).unwrap();

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

            Ok(meta)
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<()> {
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
                return Err(new_other_object_error(
                    Operation::Delete,
                    path,
                    anyhow!("remove request: {e:?}"),
                ));
            }
        }

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_ftp_error(e, Operation::Delete, path))?;

        Ok(())
    }

    async fn list(&self, path: &str, _: OpList) -> Result<ObjectStreamer> {
        let mut ftp_stream = self.ftp_connect(Operation::List).await?;

        let pathname = if path == "/" { None } else { Some(path) };
        let files = ftp_stream.list(pathname).await.map_err(|e| {
            new_other_object_error(Operation::List, path, anyhow!("list request: {e:?}"))
        })?;

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_ftp_error(e, Operation::List, path))?;

        let rd = ReadDir::new(files);

        Ok(Box::new(DirStream::new(
            Arc::new(self.clone()),
            if path == "/" { "" } else { path },
            rd,
        )))
    }
}

impl Backend {
    async fn ftp_connect(&self, op: Operation) -> Result<PooledConnection<'static, Manager>> {
        let pool = match self
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
            .await
        {
            Ok(v) => Ok(v.clone()),
            Err(err) => Err(new_ftp_error(err, op, "")),
        }?;

        pool.get_owned().await.map_err(|err| match err {
            RunError::User(err) => new_ftp_error(err, op, ""),
            RunError::TimedOut => {
                new_other_object_error(op, "", anyhow!("connection request: timeout"))
            }
        })
    }
}

#[cfg(test)]
mod build_test {
    use std::io::ErrorKind;

    use super::Builder;

    #[test]
    fn test_build() {
        // ftps scheme, should suffix with default port 21
        let mut builder = Builder::default();
        builder.endpoint("ftps://ftp_server.local");
        let b = builder.build();
        assert!(b.is_ok());

        // ftp scheme
        let mut builder = Builder::default();
        builder.endpoint("ftp://ftp_server.local:1234");
        let b = builder.build();
        assert!(b.is_ok());

        // no scheme
        let mut builder = Builder::default();
        builder.endpoint("ftp_server.local:8765");
        let b = builder.build();
        assert!(b.is_ok());

        // invalid scheme
        let mut builder = Builder::default();
        builder.endpoint("invalidscheme://ftp_server.local:8765");
        let b = builder.build();
        assert!(b.is_err());
        let e = b.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Other);
    }
}
