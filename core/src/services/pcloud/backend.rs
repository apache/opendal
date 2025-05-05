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

use super::core::*;
use super::delete::PcloudDeleter;
use super::error::parse_error;
use super::error::PcloudError;
use super::lister::PcloudLister;
use super::writer::PcloudWriter;
use super::writer::PcloudWriters;
use crate::raw::*;
use crate::services::PcloudConfig;
use crate::*;

impl Configurator for PcloudConfig {
    type Builder = PcloudBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        PcloudBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [pCloud](https://www.pcloud.com/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct PcloudBuilder {
    config: PcloudConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for PcloudBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("PcloudBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl PcloudBuilder {
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

    /// Pcloud endpoint.
    /// <https://api.pcloud.com> for United States and <https://eapi.pcloud.com> for Europe
    /// ref to [doc.pcloud.com](https://docs.pcloud.com/)
    ///
    /// It is required. e.g. `https://api.pcloud.com`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = endpoint.to_string();

        self
    }

    /// Pcloud username.
    ///
    /// It is required. your pCloud login email, e.g. `example@gmail.com`
    pub fn username(mut self, username: &str) -> Self {
        self.config.username = if username.is_empty() {
            None
        } else {
            Some(username.to_string())
        };

        self
    }

    /// Pcloud password.
    ///
    /// It is required. your pCloud login password, e.g. `password`
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

impl Builder for PcloudBuilder {
    const SCHEME: Scheme = Scheme::Pcloud;
    type Config = PcloudConfig;

    /// Builds the backend and returns the result of PcloudBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle endpoint.
        if self.config.endpoint.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Pcloud));
        }

        debug!("backend use endpoint {}", &self.config.endpoint);

        let username = match &self.config.username {
            Some(username) => Ok(username.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "username is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Pcloud)),
        }?;

        let password = match &self.config.password {
            Some(password) => Ok(password.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "password is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Pcloud)),
        }?;

        Ok(PcloudBackend {
            core: Arc::new(PcloudCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Pcloud)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_last_modified: true,

                            create_dir: true,

                            read: true,

                            write: true,

                            delete: true,
                            rename: true,
                            copy: true,

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
                endpoint: self.config.endpoint.clone(),
                username,
                password,
            }),
        })
    }
}

/// Backend for Pcloud services.
#[derive(Debug, Clone)]
pub struct PcloudBackend {
    core: Arc<PcloudCore>,
}

impl Access for PcloudBackend {
    type Reader = HttpBody;
    type Writer = PcloudWriters;
    type Lister = oio::PageLister<PcloudLister>;
    type Deleter = oio::OneShotDeleter<PcloudDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.ensure_dir_exists(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.stat(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: StatResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                let result = resp.result;
                if result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                }

                if let Some(md) = resp.metadata {
                    let md = parse_stat_metadata(md);
                    return md.map(RpStat::new);
                }

                Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let link = self.core.get_file_link(path).await?;

        let resp = self.core.download(&link, args.range()).await?;

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
        let writer = PcloudWriter::new(self.core.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(PcloudDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = PcloudLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        self.core.ensure_dir_exists(to).await?;

        let resp = if from.ends_with('/') {
            self.core.copy_folder(from, to).await?
        } else {
            self.core.copy_file(from, to).await?
        };

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: PcloudError =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                let result = resp.result;
                if result == 2009 || result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                }

                Ok(RpCopy::default())
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.core.ensure_dir_exists(to).await?;

        let resp = if from.ends_with('/') {
            self.core.rename_folder(from, to).await?
        } else {
            self.core.rename_file(from, to).await?
        };

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: PcloudError =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                let result = resp.result;
                if result == 2009 || result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                }

                Ok(RpRename::default())
            }
            _ => Err(parse_error(resp)),
        }
    }
}
