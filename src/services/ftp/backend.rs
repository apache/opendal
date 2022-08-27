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
use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::io::SeekFrom;
use std::str;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_compat::Compat;
use async_trait::async_trait;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use log::info;
use suppaftp::list::File;
use suppaftp::native_tls::TlsConnector;
use suppaftp::FtpStream;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::dir_stream::ReadDir;
use super::err::new_request_append_error;
use super::err::new_request_connection_error;
use super::err::new_request_list_error;
use super::err::new_request_mkdir_error;
use super::err::new_request_put_error;
use super::err::new_request_quit_error;
use super::err::new_request_remove_error;
use super::err::new_request_retr_error;
use super::err::new_request_root_eror;
use super::err::new_request_secure_error;
use super::err::new_request_sign_error;
use super::err::parse_io_error;
use crate::error::other;
use crate::error::BackendError;
use crate::io_util::unshared_reader;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BytesReader;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;

/// Builder for ftp backend.
#[derive(Default)]
pub struct Builder {
    endpoint: Option<String>,
    port: Option<String>,
    root: Option<String>,
    user: Option<String>,
    password: Option<String>,
    tls: Option<bool>,
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
    /// set endpoint for ftp backend.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// set port for ftp backend.
    pub fn port(&mut self, port: &str) -> &mut Self {
        self.port = if port.is_empty() {
            None
        } else {
            Some(port.to_string())
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

    /// set tls for ftp backend.
    pub fn tls(&mut self, tls: bool) -> &mut Self {
        self.tls = Some(tls);
        self
    }

    /// Build a ftp backend.
    pub fn build(&mut self) -> Result<Backend> {
        info!("ftp backend build started: {:?}", &self);
        let endpoint = match &self.endpoint {
            None => {
                return Err(other(BackendError::new(
                    HashMap::new(),
                    anyhow!("endpoint must be specified"),
                )))
            }
            Some(v) => v,
        };

        let port = match &self.port {
            None => "21".to_string(),
            Some(v) => v.clone(),
        };

        let root = match &self.root {
            // set default path to '/ftp'
            None => "/ftp".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty());
                let mut v = v.clone();
                if !v.starts_with('/') {
                    return Err(other(BackendError::new(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("root must start with /"),
                    )));
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

        let credential = (user, password);

        let tls = match &self.tls {
            None => false,
            Some(v) => *v,
        };

        info!("ftp backend finished: {:?}", &self);
        Ok(Backend {
            endpoint: endpoint.to_string(),
            port,
            root,
            credential,
            tls,
            //stream,
        })
    }
}

/// Backend is used to serve `Accessor` support for ftp.
#[derive(Clone)]
pub struct Backend {
    endpoint: String,
    port: String,
    root: String,
    credential: (String, String),
    tls: bool,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("credential", &self.credential)
            //.field("stream", &self.stream)
            .finish()
    }
}

impl Backend {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                _ => continue,
            };
        }
        builder.build()
    }
    /*
    pub(crate) fn get_rel_path(&self, path: &str) -> String {
        match path.strip_prefix(&self.root) {
            Some(v) => v.to_string(),
            None => unreachable!(
                "invalid path{} that does not start with backend root {}",
                &path, &self.root
            ),
        }
    }

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.to_string();
        }
        format!("{}{}", self.root, path)
    }
    */
}

#[async_trait]
impl Accessor for Backend {
    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = args.path();

        if args.mode() == ObjectMode::FILE {
            self.ftp_put(path).await?;

            return Ok(());
        }

        if args.mode() == ObjectMode::DIR {
            self.ftp_mkdir(path).await?;

            return Ok(());
        }

        unreachable!()
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = args.path();

        let reader = self.ftp_get(path).await?;

        let mut reader = Compat::new(reader);

        if let Some(offset) = args.offset() {
            reader
                .seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| parse_io_error(e, "read", args.path()))?;
        }

        let r: BytesReader = match args.size() {
            Some(size) => Box::new(reader.take(size)),
            None => Box::new(reader),
        };

        Ok(r)
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = args.path();

        let n = self.ftp_append(path, unshared_reader(r)).await?;

        Ok(n)
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = args.path();

        if path == self.root {
            let mut meta = ObjectMetadata::default();
            meta.set_mode(ObjectMode::DIR);
            return Ok(meta);
        }

        let resp = self.ftp_stat(path).await?;

        let mut meta = ObjectMetadata::default();

        if resp.is_file() {
            meta.set_mode(ObjectMode::FILE);
        } else if resp.is_directory() {
            meta.set_mode(ObjectMode::DIR);
        } else {
            meta.set_mode(ObjectMode::Unknown);
        }

        meta.set_content_length(resp.size() as u64);

        meta.set_last_modified(OffsetDateTime::from(resp.modified()));

        Ok(meta)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = args.path();
        if args.path().ends_with('/') {
            self.ftp_rmdir(path).await?;
        } else {
            self.ftp_rm(path).await?;
        }

        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = args.path();

        let rd = self.ftp_list(path).await?;

        Ok(Box::new(DirStream::new(Arc::new(self.clone()), path, rd)))
    }
}

impl Backend {
    pub(crate) fn ftp_connect(&self) -> Result<FtpStream> {
        // connecting to remote address
        let u = format! {"{}:{}", self.endpoint, self.port};

        let mut ftp_stream = FtpStream::connect(&u)
            .map_err(|e| new_request_connection_error("connection", &u, e))?;

        // switch to secure mode if ssl/tls is on.
        if self.tls {
            ftp_stream = ftp_stream
                .into_secure(TlsConnector::new().unwrap(), &u)
                .map_err(|e| new_request_secure_error("connection", &u, e))?;
        }

        // login if needed
        if !self.credential.0.is_empty() {
            ftp_stream
                .login(&self.credential.0, &self.credential.1)
                .map_err(|e| new_request_sign_error("connection", &self.endpoint, e))?;
        }

        // change to the root path
        ftp_stream
            .cwd(&self.root)
            .map_err(|e| new_request_root_eror("connection", &self.root, e))?;

        Ok(ftp_stream)
    }

    pub(crate) async fn ftp_get(&self, path: &str) -> Result<Cursor<Vec<u8>>> {
        let mut ftp_stream = self.ftp_connect()?;

        let reader = ftp_stream
            .retr_as_buffer(path)
            .map_err(|e| new_request_retr_error("get", path, e))?;

        ftp_stream
            .quit()
            .map_err(|e| new_request_quit_error("get", path, e))?;

        Ok(reader)
    }

    pub(crate) async fn ftp_put(&self, path: &str) -> Result<u64> {
        let mut ftp_stream = self.ftp_connect()?;

        let n = ftp_stream
            .put_file(&path, &mut "".as_bytes())
            .map_err(|e| new_request_put_error("put", path, e))?;
        Ok(n)
    }

    pub(crate) async fn ftp_mkdir(&self, path: &str) -> Result<()> {
        let mut ftp_stream = self.ftp_connect()?;
        ftp_stream
            .mkdir(&path)
            .map_err(|e| new_request_mkdir_error("mkdir", path, e))?;

        Ok(())
    }

    pub(crate) async fn ftp_rm(&self, path: &str) -> Result<()> {
        let mut ftp_stream = self.ftp_connect()?;

        ftp_stream
            .rm(&path)
            .map_err(|e| new_request_remove_error("rm", path, e))?;

        Ok(())
    }

    pub(crate) async fn ftp_rmdir(&self, path: &str) -> Result<()> {
        let mut ftp_stream = self.ftp_connect()?;

        ftp_stream
            .rmdir(&path)
            .map_err(|e| new_request_remove_error("rmdir", path, e))?;

        Ok(())
    }

    pub(crate) async fn ftp_append<R>(&self, path: &str, r: R) -> Result<u64>
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        let mut ftp_stream = self.ftp_connect()?;

        let mut reader = Box::pin(r);

        let mut buf = Vec::new();

        let _byte = reader.read_to_end(&mut buf).await?;

        let n = ftp_stream
            .append_file(path, &mut buf.as_slice())
            .map_err(|e| new_request_append_error("append", path, e))?;

        Ok(n)
    }

    pub(crate) async fn ftp_stat(&self, path: &str) -> Result<File> {
        let mut ftp_stream = self.ftp_connect()?;

        let mut result = ftp_stream
            .list(Some(path))
            .map_err(|e| new_request_list_error("stat", path, e))?;

        ftp_stream
            .quit()
            .map_err(|e| new_request_quit_error("stat", path, e))?;

        // As result is not empty, we can safely use swap_remove without panic
        if !result.is_empty() {
            let f = File::from_str(&result.swap_remove(0))
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

            Ok(f)
        } else {
            Err(Error::new(ErrorKind::NotFound, "file not found"))
        }
    }

    pub(crate) async fn ftp_list(&self, path: &str) -> Result<ReadDir> {
        let mut ftp_stream = self.ftp_connect()?;

        let files = ftp_stream
            .list(Some(path))
            .map_err(|e| new_request_list_error("list", path, e))?;

        let dir = ReadDir::new(files);

        Ok(dir)
    }
}
