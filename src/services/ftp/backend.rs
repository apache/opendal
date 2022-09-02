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

use futures::io::Cursor;
use futures::lock::Mutex;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::copy;
use std::io::sink;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::str;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::AsyncReadExt;
use log::info;
use suppaftp::list::File;
use suppaftp::native_tls::TlsConnector;
use suppaftp::FtpStream;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::dir_stream::ReadDir;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
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
    root: Option<String>,
    user: Option<String>,
    password: Option<String>,
    enable_secure: bool,
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
    pub fn enable_secure(&mut self) -> &mut Self {
        self.enable_secure = true;

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

        let root = match &self.root {
            // set default path to '/'
            None => "/".to_string(),
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

        let enable_secure = self.enable_secure;

        let mut ftp_stream = FtpStream::connect(&endpoint).map_err(|e| {
            other(ObjectError::new(
                "connection",
                &endpoint,
                anyhow!("connection request: {e:?}"),
            ))
        })?;

        // switch to secure mode if ssl/tls is on.
        if self.enable_secure {
            ftp_stream = ftp_stream
                .into_secure(TlsConnector::new().unwrap(), &endpoint)
                .map_err(|e| {
                    other(ObjectError::new(
                        "connection",
                        &endpoint,
                        anyhow!("switching to secure mode request: {e:?}"),
                    ))
                })?;
        }

        // login if needed
        if !credential.0.is_empty() {
            ftp_stream
                .login(&credential.0, &credential.1)
                .map_err(|e| {
                    other(ObjectError::new(
                        "connection",
                        &endpoint,
                        anyhow!("signing request: {e:?}"),
                    ))
                })?;
        }

        // change to the root path
        ftp_stream.cwd(&root).map_err(|e| {
            other(ObjectError::new(
                "connection",
                &endpoint,
                anyhow!("change root request: {e:?}"),
            ))
        })?;

        let client = ftp_stream;

        info!("ftp backend finished: {:?}", &self);
        Ok(Backend {
            endpoint: endpoint.to_string(),
            root,
            client: Arc::new(Mutex::new(client)),
            enable_secure,
        })
    }
}

/// Backend is used to serve `Accessor` support for ftp.
#[derive(Clone)]
pub struct Backend {
    endpoint: String,
    root: String,
    client: Arc<Mutex<FtpStream>>,
    enable_secure: bool,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("tls", &self.enable_secure)
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
}

#[async_trait]
impl Accessor for Backend {
    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = args.path();
        if args.mode() == ObjectMode::FILE {
            //let mut ftp_stream = self.ftp_connect()?;
            self.client
                .try_lock()
                .unwrap()
                .put_file(&path, &mut "".as_bytes())
                .map_err(|e| {
                    other(ObjectError::new(
                        "create",
                        path,
                        anyhow!("put request: {e:?}"),
                    ))
                })?;
            return Ok(());
        }

        if args.mode() == ObjectMode::DIR {
            self.client.try_lock().unwrap().mkdir(&path).map_err(|e| {
                other(ObjectError::new(
                    "create",
                    path,
                    anyhow!("mkdir request: {e:?}"),
                ))
            })?;

            return Ok(());
        }

        unreachable!()
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = args.path();
        let mut guard = self.client.lock().await;

        let mut stream = guard.retr_as_stream(path).map_err(|e| {
            other(ObjectError::new(
                "read",
                path,
                anyhow!("retrieve request: {e:?}"),
            ))
        })?;

        if let Some(offset) = args.offset() {
            copy(&mut stream.by_ref().take(offset), &mut sink())?;
        }

        let mut buf = Vec::new();
        let r = match args.size() {
            None => {
                stream.by_ref().read_to_end(&mut buf)?;
                Box::new(Cursor::new(buf))
            }
            Some(size) => {
                stream.by_ref().take(size).read_to_end(&mut buf)?;
                Box::new(Cursor::new(buf))
            }
        };

        guard.finalize_retr_stream(stream).map_err(|e| {
            other(ObjectError::new(
                "read",
                path,
                anyhow!("finalizing stream request: {e:?}"),
            ))
        })?;

        drop(guard);

        Ok(r)
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = args.path();

        let mut guard = self.client.lock().await;

        let mut reader = Box::pin(r);

        let mut buf = Vec::new();

        let _byte = reader.read_to_end(&mut buf).await?;

        let n = guard.append_file(path, &mut buf.as_slice()).map_err(|e| {
            other(ObjectError::new(
                "write",
                path,
                anyhow!("append request: {e:?}"),
            ))
        })?;

        drop(guard);

        Ok(n)
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = args.path();

        if path.is_empty() {
            let mut meta = ObjectMetadata::default();
            meta.set_mode(ObjectMode::DIR);
            return Ok(meta);
        }

        let mut guard = self.client.lock().await;

        let mut resp = guard.list(Some(path)).map_err(|e| {
            other(ObjectError::new(
                "stat",
                path,
                anyhow!("list request: {e:?}"),
            ))
        })?;

        drop(guard);
        // As result is not empty, we can safely use swap_remove without panic
        if !resp.is_empty() {
            let mut meta = ObjectMetadata::default();

            let f = File::from_str(&resp.swap_remove(0))
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            if f.is_file() {
                meta.set_mode(ObjectMode::FILE);
            } else if f.is_directory() {
                meta.set_mode(ObjectMode::DIR);
            } else {
                meta.set_mode(ObjectMode::Unknown);
            }

            meta.set_content_length(f.size() as u64);

            meta.set_last_modified(OffsetDateTime::from(f.modified()));

            Ok(meta)
        } else {
            Err(Error::new(ErrorKind::NotFound, "file not found"))
        }
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = args.path();

        let mut guard = self.client.lock().await;

        if args.path().ends_with('/') {
            guard.rmdir(&path).map_err(|e| {
                other(ObjectError::new(
                    "delete",
                    path,
                    anyhow!("remove directory request: {e:?}"),
                ))
            })?;
        } else {
            guard.rm(&path).map_err(|e| {
                other(ObjectError::new(
                    "delete",
                    path,
                    anyhow!("remove file request: {e:?}"),
                ))
            })?;
        }

        drop(guard);

        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = args.path();

        let mut guard = self.client.lock().await;

        let files = guard.list(Some(path)).map_err(|e| {
            other(ObjectError::new(
                "list",
                path,
                anyhow!("list request: {e:?}"),
            ))
        })?;

        drop(guard);

        let rd = ReadDir::new(files);

        Ok(Box::new(DirStream::new(Arc::new(self.clone()), path, rd)))
    }
}
