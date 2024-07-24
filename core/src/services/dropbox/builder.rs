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

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use super::backend::DropboxBackend;
use super::core::DropboxCore;
use super::core::DropboxSigner;
use crate::raw::*;
use crate::*;

/// Config for [Dropbox](https://www.dropbox.com/) backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct DropboxConfig {
    /// root path for dropbox.
    pub root: Option<String>,
    /// access token for dropbox.
    pub access_token: Option<String>,
    /// refresh_token for dropbox.
    pub refresh_token: Option<String>,
    /// client_id for dropbox.
    pub client_id: Option<String>,
    /// client_secret for dropbox.
    pub client_secret: Option<String>,
}

impl Debug for DropboxConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DropBoxConfig")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl Configurator for DropboxConfig {
    fn into_builder(self) -> impl Builder {
        DropboxBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [Dropbox](https://www.dropbox.com/) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct DropboxBuilder {
    config: DropboxConfig,

    http_client: Option<HttpClient>,
}

impl Debug for DropboxBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("root", &self.config.root)
            .finish()
    }
}

impl DropboxBuilder {
    /// Set the root directory for dropbox.
    ///
    /// Default to `/` if not set.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = Some(root.to_string());
        self
    }

    /// Access token is used for temporary access to the Dropbox API.
    ///
    /// You can get the access token from [Dropbox App Console](https://www.dropbox.com/developers/apps)
    ///
    /// NOTE: this token will be expired in 4 hours.
    /// If you are trying to use the Dropbox service in a long time, please set a refresh_token instead.
    pub fn access_token(&mut self, access_token: &str) -> &mut Self {
        self.config.access_token = Some(access_token.to_string());
        self
    }

    /// Refresh token is used for long term access to the Dropbox API.
    ///
    /// You can get the refresh token via OAuth 2.0 Flow of Dropbox.
    ///
    /// OpenDAL will use this refresh token to get a new access token when the old one is expired.
    pub fn refresh_token(&mut self, refresh_token: &str) -> &mut Self {
        self.config.refresh_token = Some(refresh_token.to_string());
        self
    }

    /// Set the client id for Dropbox.
    ///
    /// This is required for OAuth 2.0 Flow to refresh the access token.
    pub fn client_id(&mut self, client_id: &str) -> &mut Self {
        self.config.client_id = Some(client_id.to_string());
        self
    }

    /// Set the client secret for Dropbox.
    ///
    /// This is required for OAuth 2.0 Flow with refresh the access token.
    pub fn client_secret(&mut self, client_secret: &str) -> &mut Self {
        self.config.client_secret = Some(client_secret.to_string());
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, http_client: HttpClient) -> &mut Self {
        self.http_client = Some(http_client);
        self
    }
}

impl Builder for DropboxBuilder {
    const SCHEME: Scheme = Scheme::Dropbox;
    type Config = DropboxConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Dropbox)
            })?
        };

        let signer = match (self.config.access_token, self.config.refresh_token) {
            (Some(access_token), None) => DropboxSigner {
                access_token,
                // We will never expire user specified token.
                expires_in: DateTime::<Utc>::MAX_UTC,
                ..Default::default()
            },
            (None, Some(refresh_token)) => {
                let client_id = self.config.client_id.ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "client_id must be set when refresh_token is set",
                    )
                    .with_context("service", Scheme::Dropbox)
                })?;
                let client_secret = self.config.client_secret.ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "client_secret must be set when refresh_token is set",
                    )
                    .with_context("service", Scheme::Dropbox)
                })?;

                DropboxSigner {
                    refresh_token,
                    client_id,
                    client_secret,
                    ..Default::default()
                }
            }
            (Some(_), Some(_)) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token and refresh_token can not be set at the same time",
                )
                .with_context("service", Scheme::Dropbox))
            }
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token or refresh_token must be set",
                )
                .with_context("service", Scheme::Dropbox))
            }
        };

        Ok(DropboxBackend {
            core: Arc::new(DropboxCore {
                root,
                signer: Arc::new(Mutex::new(signer)),
                client,
            }),
        })
    }
}
