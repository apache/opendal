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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use log::debug;
use serde::Deserialize;
use tokio::sync::RwLock;

use super::core::parse_dir_detail;
use super::core::parse_file_detail;
use super::core::SeafileCore;
use super::error::parse_error;
use super::lister::SeafileLister;
use super::writer::SeafileWriter;
use super::writer::SeafileWriters;
use crate::raw::*;
use crate::services::seafile::core::SeafileSigner;
use crate::*;

/// Config for backblaze seafile services support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct SeafileConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// endpoint address of this backend.
    pub endpoint: Option<String>,
    /// username of this backend.
    pub username: Option<String>,
    /// password of this backend.
    pub password: Option<String>,
    /// repo_name of this backend.
    ///
    /// required.
    pub repo_name: String,
}

impl Debug for SeafileConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SeafileConfig");

        d.field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .field("repo_name", &self.repo_name);

        d.finish_non_exhaustive()
    }
}

/// [seafile](https://www.seafile.com) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SeafileBuilder {
    config: SeafileConfig,

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
    pub fn root(&mut self, root: &str) -> &mut Self {
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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
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
    pub fn username(&mut self, username: &str) -> &mut Self {
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
    pub fn password(&mut self, password: &str) -> &mut Self {
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
    pub fn repo_name(&mut self, repo_name: &str) -> &mut Self {
        self.config.repo_name = repo_name.to_string();

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for SeafileBuilder {
    const SCHEME: Scheme = Scheme::Seafile;
    type Accessor = SeafileBackend;

    /// Converts a HashMap into an SeafileBuilder instance.
    ///
    /// # Arguments
    ///
    /// * `map` - A HashMap containing the configuration values.
    ///
    /// # Returns
    ///
    /// Returns an instance of SeafileBuilder.
    fn from_map(map: HashMap<String, String>) -> Self {
        // Deserialize the configuration from the HashMap.
        let config = SeafileConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        // Create an SeafileBuilder instance with the deserialized config.
        SeafileBuilder {
            config,
            http_client: None,
        }
    }

    /// Builds the backend and returns the result of SeafileBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
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

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Seafile)
            })?
        };

        Ok(SeafileBackend {
            core: Arc::new(SeafileCore {
                root,
                endpoint,
                username,
                password,
                repo_name: self.config.repo_name.clone(),
                signer: Arc::new(RwLock::new(SeafileSigner::default())),
                client,
            }),
        })
    }
}

/// Backend for seafile services.
#[derive(Debug, Clone)]
pub struct SeafileBackend {
    core: Arc<SeafileCore>,
}

#[async_trait]
impl Accessor for SeafileBackend {
    type Reader = oio::Buffer;
    type Writer = SeafileWriters;
    type Lister = oio::PageLister<SeafileLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Seafile)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_can_next: true,

                write: true,
                write_can_empty: true,

                delete: true,

                list: true,

                ..Default::default()
            });

        am
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

    async fn read(&self, path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.download_file(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let size = parse_content_length(resp.headers())?;
                let range = parse_content_range(resp.headers())?;
                Ok((
                    RpRead::new().with_size(size).with_range(range),
                    resp.into_body(),
                ))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let w = SeafileWriter::new(self.core.clone(), args, path.to_string());
        let w = oio::OneShotWriter::new(w);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self, path: &str, _args: OpDelete) -> Result<RpDelete> {
        let _ = self.core.delete(path).await?;

        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = SeafileLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
