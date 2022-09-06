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
use futures::io::copy;
use futures::AsyncReadExt;
use log::info;
use suppaftp::async_native_tls::TlsConnector;
use suppaftp::list::File;
use suppaftp::FtpStream;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::dir_stream::ReadDir;
use super::err::new_request_connection_err;
use super::err::new_request_quit_err;
use crate::accessor::AccessorCapability;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
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
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Scheme;

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

        info!("ftp backend finished: {:?}", &self);
        Ok(Backend {
            endpoint: endpoint.to_string(),
            root,
            credential,
            enable_secure,
        })
    }
}

/// Backend is used to serve `Accessor` support for ftp.
#[derive(Clone)]
pub struct Backend {
    endpoint: String,
    root: String,
    credential: (String, String),
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
                "endpoint" => builder.endpoint(v),
                "user" => builder.user(v),
                "password" => builder.password(v),
                _ => continue,
            };
        }
        builder.build()
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

    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = args.path();

        let mut ftp_stream = self.ftp_connect(Operation::Create).await?;

        if args.mode() == ObjectMode::FILE {
            ftp_stream
                .put_file(&path, &mut "".as_bytes())
                .await
                .map_err(|e| {
                    other(ObjectError::new(
                        Operation::Create,
                        path,
                        anyhow!("put request: {e:?}"),
                    ))
                })?;

            return Ok(());
        }

        if args.mode() == ObjectMode::DIR {
            ftp_stream.mkdir(&path).await.map_err(|e| {
                other(ObjectError::new(
                    Operation::Create,
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

        let mut ftp_stream = self.ftp_connect(Operation::Read).await?;

        if let Some(offset) = args.offset() {
            ftp_stream
                .resume_transfer(offset as usize)
                .await
                .map_err(|e| {
                    other(ObjectError::new(
                        Operation::Read,
                        path,
                        anyhow!("resume transfer request: {e:?}"),
                    ))
                })?;
        }

        let data_stream = ftp_stream.retr_as_stream(path).await.map_err(|e| {
            other(ObjectError::new(
                Operation::Read,
                path,
                anyhow!("retrieve request: {e:?}"),
            ))
        })?;

        let r: BytesReader = match args.size() {
            None => Box::new(data_stream),
            Some(size) => Box::new(data_stream.take(size)),
        };

        Ok(r)
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = args.path();

        let mut ftp_stream = self.ftp_connect(Operation::Write).await?;

        let mut data_stream = ftp_stream.append_with_stream(path).await.map_err(|e| {
            other(ObjectError::new(
                Operation::Write,
                path,
                anyhow!("append request: {e:?}"),
            ))
        })?;

        let bytes = copy(r, &mut data_stream).await?;

        ftp_stream
            .finalize_put_stream(data_stream)
            .await
            .map_err(|e| {
                other(ObjectError::new(
                    Operation::Write,
                    path,
                    anyhow!("finalize put request: {e:?}"),
                ))
            })?;

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_request_quit_err(e, Operation::Write, path))?;

        Ok(bytes)
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = args.path();

        let mut ftp_stream = self.ftp_connect(Operation::Stat).await?;

        if path.is_empty() {
            let mut meta = ObjectMetadata::default();
            meta.set_mode(ObjectMode::DIR);
            return Ok(meta);
        }

        let mut resp = ftp_stream.list(Some(path)).await.map_err(|e| {
            other(ObjectError::new(
                Operation::Stat,
                path,
                anyhow!("list request: {e:?}"),
            ))
        })?;

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_request_quit_err(e, Operation::Write, path))?;

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

        let mut ftp_stream = self.ftp_connect(Operation::Delete).await?;

        if args.path().ends_with('/') {
            ftp_stream.rmdir(&path).await.map_err(|e| {
                other(ObjectError::new(
                    Operation::Delete,
                    path,
                    anyhow!("remove directory request: {e:?}"),
                ))
            })?;
        } else {
            ftp_stream.rm(&path).await.map_err(|e| {
                other(ObjectError::new(
                    Operation::Delete,
                    path,
                    anyhow!("remove file request: {e:?}"),
                ))
            })?;
        }

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_request_quit_err(e, Operation::Write, path))?;

        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = args.path();

        let mut ftp_stream = self.ftp_connect(Operation::List).await?;

        let files = ftp_stream.list(Some(path)).await.map_err(|e| {
            other(ObjectError::new(
                Operation::List,
                path,
                anyhow!("list request: {e:?}"),
            ))
        })?;

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_request_quit_err(e, Operation::Write, path))?;

        let rd = ReadDir::new(files);

        Ok(Box::new(DirStream::new(Arc::new(self.clone()), path, rd)))
    }
}

impl Backend {
    pub(crate) async fn ftp_connect(&self, op: Operation) -> Result<FtpStream> {
        let stream = FtpStream::connect(&self.endpoint)
            .await
            .map_err(|e| new_request_connection_err(e, Operation::Delete, &self.endpoint))?;

        // switch to secure mode if ssl/tls is on.
        let mut ftp_stream = if self.enable_secure {
            stream
                .into_secure(TlsConnector::new(), &self.endpoint)
                .await
                .map_err(|e| {
                    other(ObjectError::new(
                        op,
                        &self.endpoint,
                        anyhow!("enable secure request: {e:?}"),
                    ))
                })?
        } else {
            stream
        };

        // login if needed
        if !self.credential.0.is_empty() {
            ftp_stream
                .login(&self.credential.0, &self.credential.1)
                .await
                .map_err(|e| {
                    other(ObjectError::new(
                        op,
                        &self.endpoint,
                        anyhow!("login request: {e:?}"),
                    ))
                })?;
        }

        // change to the root path
        ftp_stream.cwd(&self.root).await.map_err(|e| {
            other(ObjectError::new(
                op,
                &self.endpoint,
                anyhow!("change root request: {e:?}"),
            ))
        })?;

        ftp_stream
            .quit()
            .await
            .map_err(|e| new_request_quit_err(e, op, &self.endpoint))?;

        Ok(ftp_stream)
    }
}
