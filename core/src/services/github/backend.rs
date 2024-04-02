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

use super::core::GithubCore;
use super::error::parse_error;
use super::lister::GithubLister;
use super::writer::GithubWriter;
use super::writer::GithubWriters;
use crate::raw::*;
use crate::services::github::reader::GithubReader;
use crate::*;

/// Config for backblaze Github services support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct GithubConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// Github access_token.
    ///
    /// optional.
    /// If not provided, the backend will only support read operations for public repositories.
    /// And rate limit will be limited to 60 requests per hour.
    pub token: Option<String>,
    /// Github repo owner.
    ///
    /// required.
    pub owner: String,
    /// Github repo name.
    ///
    /// required.
    pub repo: String,
}

impl Debug for GithubConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("GithubConfig");

        d.field("root", &self.root)
            .field("owner", &self.owner)
            .field("repo", &self.repo);

        d.finish_non_exhaustive()
    }
}

/// [github contents](https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#create-or-update-file-contents) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GithubBuilder {
    config: GithubConfig,

    http_client: Option<HttpClient>,
}

impl Debug for GithubBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("GithubBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl GithubBuilder {
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

    /// Github access_token.
    ///
    /// required.
    pub fn token(&mut self, token: &str) -> &mut Self {
        self.config.token = Some(token.to_string());

        self
    }

    /// Set Github repo owner.
    pub fn owner(&mut self, owner: &str) -> &mut Self {
        self.config.owner = owner.to_string();

        self
    }

    /// Set Github repo name.
    pub fn repo(&mut self, repo: &str) -> &mut Self {
        self.config.repo = repo.to_string();

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

impl Builder for GithubBuilder {
    const SCHEME: Scheme = Scheme::Github;
    type Accessor = GithubBackend;

    /// Converts a HashMap into an GithubBuilder instance.
    ///
    /// # Arguments
    ///
    /// * `map` - A HashMap containing the configuration values.
    ///
    /// # Returns
    ///
    /// Returns an instance of GithubBuilder.
    fn from_map(map: HashMap<String, String>) -> Self {
        // Deserialize the configuration from the HashMap.
        let config = GithubConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        // Create an GithubBuilder instance with the deserialized config.
        GithubBuilder {
            config,
            http_client: None,
        }
    }

    /// Builds the backend and returns the result of GithubBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle owner.
        if self.config.owner.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "owner is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Github));
        }

        debug!("backend use owner {}", &self.config.owner);

        // Handle repo.
        if self.config.repo.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "repo is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Github));
        }

        debug!("backend use repo {}", &self.config.repo);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Github)
            })?
        };

        Ok(GithubBackend {
            core: Arc::new(GithubCore {
                root,
                token: self.config.token.clone(),
                owner: self.config.owner.clone(),
                repo: self.config.repo.clone(),
                client,
            }),
        })
    }
}

/// Backend for Github services.
#[derive(Debug, Clone)]
pub struct GithubBackend {
    core: Arc<GithubCore>,
}

#[async_trait]
impl Accessor for GithubBackend {
    type Reader = GithubReader;

    type Writer = GithubWriters;

    type Lister = oio::PageLister<GithubLister>;

    type BlockingReader = ();

    type BlockingWriter = ();

    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Github)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                create_dir: true,

                write: true,
                write_can_empty: true,

                delete: true,

                list: true,

                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let empty_bytes = bytes::Bytes::new();

        let resp = self
            .core
            .upload(&format!("{}.gitkeep", path), empty_bytes)
            .await?;

        match parts.status {
            StatusCode::OK | StatusCode::CREATED => Ok(RpCreateDir::default()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.stat(path).await?;

        match parts.status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            GithubReader::new(self.core.clone(), path, args),
        ))
    }

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = GithubWriter::new(self.core.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        match self.core.delete(path).await {
            Ok(_) => Ok(RpDelete::default()),
            Err(err) => Err(err),
        }
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = GithubLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
