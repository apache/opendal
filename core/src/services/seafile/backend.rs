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

use http::Response;
use http::StatusCode;
use log::debug;
use tokio::sync::RwLock;

use super::core::parse_dir_detail;
use super::core::parse_file_detail;
use super::core::SeafileCore;
use super::core::SeafileSigner;
use super::delete::SeafileDeleter;
use super::error::parse_error;
use super::lister::SeafileLister;
use super::writer::SeafileWriter;
use super::writer::SeafileWriters;
use crate::raw::*;
use crate::services::SeafileConfig;
use crate::*;

impl Configurator for SeafileConfig {
    type Builder = SeafileBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        SeafileBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [seafile](https://www.seafile.com) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SeafileBuilder {
    config: SeafileConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for SeafileBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SeafileBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl SeafileBuilder {
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

    /// endpoint of this backend.
    ///
    /// It is required. e.g. `http://127.0.0.1:80`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// username of this backend.
    ///
    /// It is required. e.g. `me@example.com`
    pub fn username(mut self, username: &str) -> Self {
        self.config.username = if username.is_empty() {
            None
        } else {
            Some(username.to_string())
        };

        self
    }

    /// password of this backend.
    ///
    /// It is required. e.g. `asecret`
    pub fn password(mut self, password: &str) -> Self {
        self.config.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };

        self
    }

    /// Set repo name of this backend.
    ///
    /// It is required. e.g. `myrepo`
    pub fn repo_name(mut self, repo_name: &str) -> Self {
        self.config.repo_name = repo_name.to_string();

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

impl Builder for SeafileBuilder {
    const SCHEME: Scheme = Scheme::Seafile;
    type Config = SeafileConfig;

    /// Builds the backend and returns the result of SeafileBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket.
        if self.config.repo_name.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "repo_name is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Seafile));
        }

        debug!("backend use repo_name {}", &self.config.repo_name);

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Seafile)),
        }?;

        let username = match &self.config.username {
            Some(username) => Ok(username.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "username is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Seafile)),
        }?;

        let password = match &self.config.password {
            Some(password) => Ok(password.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "password is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Seafile)),
        }?;

        Ok(SeafileBackend {
            core: Arc::new(SeafileCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Seafile)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_last_modified: true,

                            read: true,

                            write: true,
                            write_can_empty: true,

                            delete: true,

                            list: true,
                            list_has_content_length: true,
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
                endpoint,
                username,
                password,
                repo_name: self.config.repo_name.clone(),
                signer: Arc::new(RwLock::new(SeafileSigner::default())),
            }),
        })
    }
}

/// Backend for seafile services.
#[derive(Debug, Clone)]
pub struct SeafileBackend {
    core: Arc<SeafileCore>,
}

impl Access for SeafileBackend {
    type Reader = HttpBody;
    type Writer = SeafileWriters;
    type Lister = oio::PageLister<SeafileLister>;
    type Deleter = oio::OneShotDeleter<SeafileDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let metadata = if path.ends_with('/') {
            let dir_detail = self.core.dir_detail(path).await?;
            parse_dir_detail(dir_detail)
        } else {
            let file_detail = self.core.file_detail(path).await?;

            parse_file_detail(file_detail)
        };

        metadata.map(RpStat::new)
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.download_file(path, args.range()).await?;

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

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let w = SeafileWriter::new(self.core.clone(), args, path.to_string());
        let w = oio::OneShotWriter::new(w);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(SeafileDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = SeafileLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
