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
use log::debug;
use tokio::sync::Mutex;

use super::backend::GdriveBackend;
use super::core::GdriveCore;
use super::core::GdrivePathQuery;
use super::core::GdriveSigner;
use crate::raw::Access;
use crate::raw::HttpClient;
use crate::raw::PathCacher;
use crate::raw::{normalize_root, AccessorInfo};
use crate::services::GdriveConfig;
use crate::Scheme;
use crate::*;

impl Configurator for GdriveConfig {
    type Builder = GdriveBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        GdriveBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [GoogleDrive](https://drive.google.com/) backend support.
#[derive(Default)]
#[doc = include_str!("docs.md")]
pub struct GdriveBuilder {
    config: GdriveConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for GdriveBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("config", &self.config)
            .finish()
    }
}

impl GdriveBuilder {
    /// Set root path of GoogleDrive folder.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Access token is used for temporary access to the GoogleDrive API.
    ///
    /// You can get the access token from [GoogleDrive App Console](https://console.cloud.google.com/apis/credentials)
    /// or [GoogleDrive OAuth2 Playground](https://developers.google.com/oauthplayground/)
    ///
    /// # Note
    ///
    /// - An access token is valid for 1 hour.
    /// - If you want to use the access token for a long time,
    ///   you can use the refresh token to get a new access token.
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = Some(access_token.to_string());
        self
    }

    /// Refresh token is used for long term access to the GoogleDrive API.
    ///
    /// You can get the refresh token via OAuth 2.0 Flow of GoogleDrive API.
    ///
    /// OpenDAL will use this refresh token to get a new access token when the old one is expired.
    pub fn refresh_token(mut self, refresh_token: &str) -> Self {
        self.config.refresh_token = Some(refresh_token.to_string());
        self
    }

    /// Set the client id for GoogleDrive.
    ///
    /// This is required for OAuth 2.0 Flow to refresh the access token.
    pub fn client_id(mut self, client_id: &str) -> Self {
        self.config.client_id = Some(client_id.to_string());
        self
    }

    /// Set the client secret for GoogleDrive.
    ///
    /// This is required for OAuth 2.0 Flow with refresh the access token.
    pub fn client_secret(mut self, client_secret: &str) -> Self {
        self.config.client_secret = Some(client_secret.to_string());
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
    pub fn http_client(mut self, http_client: HttpClient) -> Self {
        self.http_client = Some(http_client);
        self
    }
}

impl Builder for GdriveBuilder {
    const SCHEME: Scheme = Scheme::Gdrive;
    type Config = GdriveConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Gdrive)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,
                stat_has_content_length: true,
                stat_has_content_type: true,
                stat_has_last_modified: true,

                read: true,

                list: true,
                list_has_content_type: true,
                list_has_content_length: true,
                list_has_etag: true,

                write: true,

                create_dir: true,
                delete: true,
                rename: true,
                copy: true,

                shared: true,

                ..Default::default()
            });

        // allow deprecated api here for compatibility
        #[allow(deprecated)]
        if let Some(client) = self.http_client {
            info.update_http_client(|_| client);
        }

        let accessor_info = Arc::new(info);
        let mut signer = GdriveSigner::new(accessor_info.clone());
        match (self.config.access_token, self.config.refresh_token) {
            (Some(access_token), None) => {
                signer.access_token = access_token;
                // We will never expire user specified access token.
                signer.expires_in = DateTime::<Utc>::MAX_UTC;
            }
            (None, Some(refresh_token)) => {
                let client_id = self.config.client_id.ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "client_id must be set when refresh_token is set",
                    )
                    .with_context("service", Scheme::Gdrive)
                })?;
                let client_secret = self.config.client_secret.ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "client_secret must be set when refresh_token is set",
                    )
                    .with_context("service", Scheme::Gdrive)
                })?;

                signer.refresh_token = refresh_token;
                signer.client_id = client_id;
                signer.client_secret = client_secret;
            }
            (Some(_), Some(_)) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token and refresh_token cannot be set at the same time",
                )
                .with_context("service", Scheme::Gdrive))
            }
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token or refresh_token must be set",
                )
                .with_context("service", Scheme::Gdrive))
            }
        };

        let signer = Arc::new(Mutex::new(signer));

        Ok(GdriveBackend {
            core: Arc::new(GdriveCore {
                info: accessor_info.clone(),
                root,
                signer: signer.clone(),
                path_cache: PathCacher::new(GdrivePathQuery::new(accessor_info, signer))
                    .with_lock(),
            }),
        })
    }
}
