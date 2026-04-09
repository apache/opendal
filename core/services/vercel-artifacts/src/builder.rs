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
use std::sync::Arc;

use super::VERCEL_ARTIFACTS_SCHEME;
use super::backend::VercelArtifactsBackend;
use super::config::VercelArtifactsConfig;
use super::core::VercelArtifactsCore;
use opendal_core::raw::*;
use opendal_core::*;

/// [Vercel Cache](https://vercel.com/docs/concepts/monorepos/remote-caching) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct VercelArtifactsBuilder {
    pub(super) config: VercelArtifactsConfig,
}

impl Debug for VercelArtifactsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelArtifactsBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl VercelArtifactsBuilder {
    /// set the bearer access token for Vercel
    ///
    /// default: no access token, which leads to failure
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = Some(access_token.to_string());
        self
    }

    /// Set the endpoint for the Vercel artifacts API.
    ///
    /// Default: `https://api.vercel.com`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }
        self
    }

    /// Set the Vercel team ID.
    ///
    /// When set, the `teamId` query parameter is appended to all requests.
    pub fn team_id(mut self, team_id: &str) -> Self {
        if !team_id.is_empty() {
            self.config.team_id = Some(team_id.to_string());
        }
        self
    }

    /// Set the Vercel team slug.
    ///
    /// When set, the `slug` query parameter is appended to all requests.
    pub fn team_slug(mut self, team_slug: &str) -> Self {
        if !team_slug.is_empty() {
            self.config.team_slug = Some(team_slug.to_string());
        }
        self
    }
}

impl Builder for VercelArtifactsBuilder {
    type Config = VercelArtifactsConfig;

    fn build(self) -> Result<impl Access> {
        let info = AccessorInfo::default();
        info.set_scheme(VERCEL_ARTIFACTS_SCHEME)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,

                shared: true,

                ..Default::default()
            });

        let access_token = self
            .config
            .access_token
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "access_token not set"))?;

        let endpoint = self
            .config
            .endpoint
            .unwrap_or_else(|| "https://api.vercel.com".to_string());

        let mut query_params = Vec::new();
        if let Some(team_id) = &self.config.team_id {
            query_params.push(format!("teamId={team_id}"));
        }
        if let Some(slug) = &self.config.team_slug {
            query_params.push(format!("slug={slug}"));
        }
        let query_string = if query_params.is_empty() {
            String::new()
        } else {
            format!("?{}", query_params.join("&"))
        };

        Ok(VercelArtifactsBackend {
            core: Arc::new(VercelArtifactsCore {
                info: Arc::new(info),
                access_token,
                endpoint,
                query_string,
            }),
        })
    }
}
