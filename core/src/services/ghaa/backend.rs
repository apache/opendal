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
use chrono::DateTime;
use chrono::Utc;
use http::StatusCode;
use log::debug;
use serde::Deserialize;

use super::error::parse_error;
use crate::raw::*;
use crate::services::ghaa::core::GhaaCore;
use crate::services::ghaa::core::GhaaStatResponse;
use crate::services::ghaa::lister::GhaaLister;
use crate::*;

/// GitHub Action Artifacts services config.
#[derive(Default, Deserialize, Clone)]
pub struct GhaaConfig {
    /// repo's owner of this backend.
    pub owner: String,

    /// repo name of this backend
    pub repo: String,

    /// token of this backend
    ///
    /// - If the repository is private you must use an access token with the repo scope.
    /// - You must authenticate using an access token with the repo scope to delete, read artifacts.
    pub token: Option<String>,
}

impl Debug for GhaaConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("GhaaConfig");

        ds.field("owner", &self.owner);
        ds.field("repo", &self.repo);
        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

/// GitHub Action Artifacts services support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct GhaaBuilder {
    config: GhaaConfig,
    http_client: Option<HttpClient>,
}

impl Debug for GhaaBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("config", &self.config);

        ds.finish()
    }
}

impl GhaaBuilder {
    /// Set repo's owner of this backend.
    ///
    /// The account owner of the repository. The name is not case sensitive.
    pub fn owner(&mut self, owner: &str) -> &mut Self {
        if !owner.is_empty() {
            self.config.owner = owner.to_string();
        }

        self
    }

    /// Set repo name of this backend
    ///
    /// The name of the repository without the .git extension. The name is not case sensitive.
    pub fn repo(&mut self, repo: &str) -> &mut Self {
        if !repo.is_empty() {
            self.config.repo = repo.to_string();
        }
        self
    }

    /// Set token of this backend
    ///
    /// - If the repository is private you must use an access token with the repo scope.
    /// - You must authenticate using an access token with the repo scope to delete, read artifacts.
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }

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

impl Builder for GhaaBuilder {
    const SCHEME: Scheme = Scheme::Ghaa;
    type Accessor = GhaaBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let config = GhaaConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialization must succeed");
        GhaaBuilder {
            config,
            http_client: None,
        }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", self);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Ghaa)
            })?
        };

        let backend = GhaaBackend {
            core: Arc::new(GhaaCore {
                client,
                config: self.config.clone(),
            }),
        };

        Ok(backend)
    }
}

/// Backend for GitHub Action Artifacts services.
#[derive(Debug, Clone)]
pub struct GhaaBackend {
    core: Arc<GhaaCore>,
}

#[async_trait]
impl Accessor for GhaaBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Lister = oio::PageLister<GhaaLister>;
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Ghaa)
            .set_native_capability(Capability {
                stat: true,
                read: true,
                delete: true,
                list: true,
                list_with_limit: true,
                ..Default::default()
            });
        am
    }

    async fn read(&self, path: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.download_artifact(path).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok((RpRead::new(), resp.into_body())),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let resp = self.core.stat_artifact(path).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let slc = resp.into_body().bytes().await?;
                let parsed_body: GhaaStatResponse =
                    serde_json::from_slice(&slc).map_err(new_json_deserialize_error)?;

                let mut meta = Metadata::new(EntryMode::FILE);
                meta.set_content_length(parsed_body.size_in_bytes);
                meta.set_last_modified(
                    DateTime::parse_from_rfc3339(&parsed_body.updated_at)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap(),
                );
                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.delete_artifact(path).await?;
        let status = resp.status();
        match status {
            StatusCode::NO_CONTENT => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let lister = GhaaLister::new(self.core.clone(), path.to_string(), args.limit());
        Ok((RpList::default(), oio::PageLister::new(lister)))
    }
}
