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

use chrono::DateTime;
use chrono::Utc;
use log::debug;
use tokio::sync::Mutex;

use super::backend::GdriveBackend;
use crate::raw::HttpClient;
use crate::raw::{normalize_root, PathCacher};
use crate::services::gdrive::core::GdriveSigner;
use crate::services::gdrive::core::{GdriveCore, GdrivePathQuery};
use crate::Scheme;
use crate::*;

/// [GoogleDrive](https://drive.google.com/) backend support.
#[derive(Default)]
#[doc = include_str!("docs.md")]
pub struct GdriveBuilder {
    root: Option<String>,

    access_token: Option<String>,

    refresh_token: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,

    http_client: Option<HttpClient>,
}

impl Debug for GdriveBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend").field("root", &self.root).finish()
    }
}

impl GdriveBuilder {
    /// Set root path of GoogleDrive folder.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());
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
    /// you can use the refresh token to get a new access token.
    pub fn access_token(&mut self, access_token: &str) -> &mut Self {
        self.access_token = Some(access_token.to_string());
        self
    }

    /// Refresh token is used for long term access to the GoogleDrive API.
    ///
    /// You can get the refresh token via OAuth 2.0 Flow of GoogleDrive API.
    ///
    /// OpenDAL will use this refresh token to get a new access token when the old one is expired.
    pub fn refresh_token(&mut self, refresh_token: &str) -> &mut Self {
        self.refresh_token = Some(refresh_token.to_string());
        self
    }

    /// Set the client id for GoogleDrive.
    ///
    /// This is required for OAuth 2.0 Flow to refresh the access token.
    pub fn client_id(&mut self, client_id: &str) -> &mut Self {
        self.client_id = Some(client_id.to_string());
        self
    }

    /// Set the client secret for GoogleDrive.
    ///
    /// This is required for OAuth 2.0 Flow with refresh the access token.
    pub fn client_secret(&mut self, client_secret: &str) -> &mut Self {
        self.client_secret = Some(client_secret.to_string());
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

impl Builder for GdriveBuilder {
    const SCHEME: Scheme = Scheme::Gdrive;

    type Accessor = GdriveBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = Self::default();

        map.get("root").map(|v| builder.root(v));
        map.get("access_token").map(|v| builder.access_token(v));
        map.get("refresh_token").map(|v| builder.refresh_token(v));
        map.get("client_id").map(|v| builder.client_id(v));
        map.get("client_secret").map(|v| builder.client_secret(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Gdrive)
            })?
        };

        let mut signer = GdriveSigner::new(client.clone());
        match (self.access_token.take(), self.refresh_token.take()) {
            (Some(access_token), None) => {
                signer.access_token = access_token;
                // We will never expire user specified access token.
                signer.expires_in = DateTime::<Utc>::MAX_UTC;
            }
            (None, Some(refresh_token)) => {
                let client_id = self.client_id.take().ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "client_id must be set when refresh_token is set",
                    )
                    .with_context("service", Scheme::Gdrive)
                })?;
                let client_secret = self.client_secret.take().ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "client_secret must be set when refresh_token is set",
                    )
                    .with_context("service", Scheme::Gdrive)
                })?;

                signer.refresh_token = refresh_token;
                signer.client = client.clone();
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
                root,
                signer: signer.clone(),
                client: client.clone(),
                path_cache: PathCacher::new(GdrivePathQuery::new(client, signer)).with_lock(),
            }),
        })
    }
}
