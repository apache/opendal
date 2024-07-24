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

use serde::Deserialize;
use serde::Serialize;

use super::backend::VercelArtifactsBackend;
use crate::raw::Access;
use crate::raw::HttpClient;
use crate::Scheme;
use crate::*;

/// Config for Vercel Cache support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct VercelArtifactsConfig {
    /// The access token for Vercel.
    pub access_token: Option<String>,
}

impl Debug for VercelArtifactsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelArtifactsConfig")
            .field("access_token", &"<redacted>")
            .finish()
    }
}

impl Configurator for VercelArtifactsConfig {
    fn into_builder(self) -> impl Builder {
        VercelArtifactsBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [Vercel Cache](https://vercel.com/docs/concepts/monorepos/remote-caching) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct VercelArtifactsBuilder {
    config: VercelArtifactsConfig,
    http_client: Option<HttpClient>,
}

impl Debug for VercelArtifactsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("VercelArtifactsBuilder");
        d.field("config", &self.config);
        d.finish_non_exhaustive()
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

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(mut self, http_client: HttpClient) -> Self {
        self.http_client = Some(http_client);
        self
    }
}

impl Builder for VercelArtifactsBuilder {
    const SCHEME: Scheme = Scheme::VercelArtifacts;
    type Config = VercelArtifactsConfig;

    fn build(self) -> Result<impl Access> {
        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::VercelArtifacts)
            })?
        };

        match self.config.access_token.clone() {
            Some(access_token) => Ok(VercelArtifactsBackend {
                access_token,
                client,
            }),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "access_token not set")),
        }
    }
}
