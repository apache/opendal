// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use http::Response;
use http::StatusCode;
use log::debug;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;

use super::core::File;
use super::core::KoofrCore;
use super::core::KoofrSigner;
use super::delete::KoofrDeleter;
use super::error::parse_error;
use super::lister::KoofrLister;
use super::writer::KoofrWriter;
use super::writer::KoofrWriters;
use crate::raw::*;
use crate::services::KoofrConfig;
use crate::*;

impl Configurator for KoofrConfig {
    type Builder = KoofrBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        KoofrBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [Koofr](https://app.koofr.net/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct KoofrBuilder {
    config: KoofrConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for KoofrBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("KoofrBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl KoofrBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// endpoint.
    ///
    /// It is required. e.g. `https://api.koofr.net/`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = endpoint.to_string();

        self
    }

    /// email.
    ///
    /// It is required. e.g. `test@example.com`
    pub fn email(mut self, email: &str) -> Self {
        self.config.email = email.to_string();

        self
    }

    /// Koofr application password.
    ///
    /// Go to <https://app.koofr.net/app/admin/preferences/password>.
    /// Click "Generate Password" button to generate a new application password.
    ///
    /// # Notes
    ///
    /// This is not user's Koofr account password.
    /// Please use the application password instead.
    /// Please also remind users of this.
    pub fn password(mut self, password: &str) -> Self {
        self.config.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for KoofrBuilder {
    const SCHEME: Scheme = Scheme::Koofr;
    type Config = KoofrConfig;

    /// Builds the backend and returns the result of KoofrBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        if self.config.endpoint.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Koofr));
        }

        debug!("backend use endpoint {}", &self.config.endpoint);

        if self.config.email.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "email is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Koofr));
        }

        debug!("backend use email {}", &self.config.email);

        let password = match &self.config.password {
            Some(password) => Ok(password.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "password is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Koofr)),
        }?;

        let signer = Arc::new(Mutex::new(KoofrSigner::default()));

        Ok(KoofrBackend {
            core: Arc::new(KoofrCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Koofr)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_content_type: true,
                            stat_has_last_modified: true,

                            create_dir: true,

                            read: true,

                            write: true,
                            write_can_empty: true,

                            delete: true,

                            rename: true,

                            copy: true,

                            list: true,
                            list_has_content_length: true,
                            list_has_content_type: true,
                            list_has_last_modified: true,

                            shared: true,

                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = self.http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
                },
                root,
                endpoint: self.config.endpoint.clone(),
                email: self.config.email.clone(),
                password,
                mount_id: OnceCell::new(),
                signer,
            }),
        })
    }
}

/// Backend for Koofr services.
#[derive(Debug, Clone)]
pub struct KoofrBackend {
    core: Arc<KoofrCore>,
}

impl Access for KoofrBackend {
    type Reader = HttpBody;
    type Writer = KoofrWriters;
    type Lister = oio::PageLister<KoofrLister>;
    type Deleter = oio::OneShotDeleter<KoofrDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.ensure_dir_exists(path).await?;
        self.core
            .create_dir(&build_abs_path(&self.core.root, path))
            .await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let path = build_rooted_abs_path(&self.core.root, path);
        let resp = self.core.info(&path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let file: File =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                let mode = if file.ty == "dir" {
                    EntryMode::DIR
                } else {
                    EntryMode::FILE
                };

                let mut md = Metadata::new(mode);

                md.set_content_length(file.size)
                    .set_content_type(&file.content_type)
                    .set_last_modified(parse_datetime_from_from_timestamp_millis(file.modified)?);

                Ok(RpStat::new(md))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.get(path, args.range()).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = KoofrWriter::new(self.core.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(KoofrDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = KoofrLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        self.core.ensure_dir_exists(to).await?;

        if from == to {
            return Ok(RpCopy::default());
        }

        let resp = self.core.remove(to).await?;

        let status = resp.status();

        if status != StatusCode::OK && status != StatusCode::NOT_FOUND {
            return Err(parse_error(resp));
        }

        let resp = self.core.copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.core.ensure_dir_exists(to).await?;

        if from == to {
            return Ok(RpRename::default());
        }

        let resp = self.core.remove(to).await?;

        let status = resp.status();

        if status != StatusCode::OK && status != StatusCode::NOT_FOUND {
            return Err(parse_error(resp));
        }

        let resp = self.core.move_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => Err(parse_error(resp)),
        }
    }
}
