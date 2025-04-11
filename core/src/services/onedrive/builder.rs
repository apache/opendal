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
use services::onedrive::core::OneDriveCore;
use services::onedrive::core::OneDriveSigner;

use tokio::sync::Mutex;

use super::backend::OnedriveBackend;
use crate::raw::normalize_root;
use crate::raw::Access;
use crate::raw::AccessorInfo;
use crate::raw::HttpClient;
use crate::services::OnedriveConfig;
use crate::Scheme;
use crate::*;

impl Configurator for OnedriveConfig {
    type Builder = OnedriveBuilder;
    fn into_builder(self) -> Self::Builder {
        OnedriveBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// Microsoft [OneDrive](https://onedrive.com) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct OnedriveBuilder {
    config: OnedriveConfig,
    http_client: Option<HttpClient>,
}

impl Debug for OnedriveBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("config", &self.config)
            .finish()
    }
}

impl OnedriveBuilder {
    /// Set root path of OneDrive folder.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
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
    pub fn http_client(mut self, http_client: HttpClient) -> Self {
        self.http_client = Some(http_client);
        self
    }

    /// Set the access token for a time limited access to Microsoft Graph API (also OneDrive).
    ///
    /// Microsoft Graph API uses a typical OAuth 2.0 flow for authentication and authorization.
    /// You can get a access token from [Microsoft Graph Explore](https://developer.microsoft.com/en-us/graph/graph-explorer).
    ///
    /// # Note
    ///
    /// - An access token is short-lived.
    /// - Use a refresh_token if you want to use OneDrive API for an extended period of time.
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = Some(access_token.to_string());
        self
    }

    /// Set the refresh token for long term access to Microsoft Graph API.
    ///
    /// OpenDAL will use a refresh token to maintain a fresh access token automatically.
    ///
    /// # Note
    ///
    /// - A refresh token is available through a OAuth 2.0 flow, with an additional scope `offline_access`.
    pub fn refresh_token(mut self, refresh_token: &str) -> Self {
        self.config.refresh_token = Some(refresh_token.to_string());
        self
    }

    /// Set the client_id for a Microsoft Graph API application (available though Azure's registration portal)
    ///
    /// Required when using the refresh token.
    pub fn client_id(mut self, client_id: &str) -> Self {
        self.config.client_id = Some(client_id.to_string());
        self
    }

    /// Set the client_secret for a Microsoft Graph API application
    ///
    /// Required for Web app when using the refresh token.
    /// Don't use a client secret when use in a native app since the native app can't store the secret reliably.
    pub fn client_secret(mut self, client_secret: &str) -> Self {
        self.config.client_secret = Some(client_secret.to_string());
        self
    }

    /// Enable versioning support for OneDrive
    pub fn enable_versioning(mut self, enabled: bool) -> Self {
        self.config.enable_versioning = enabled;
        self
    }
}

impl Builder for OnedriveBuilder {
    const SCHEME: Scheme = Scheme::Onedrive;
    type Config = OnedriveConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Onedrive)
            .set_root(&root)
            .set_native_capability(Capability {
                read: true,
                read_with_if_none_match: true,

                write: true,
                write_with_if_match: true,
                // OneDrive supports the file size up to 250GB
                // Read more at https://support.microsoft.com/en-us/office/restrictions-and-limitations-in-onedrive-and-sharepoint-64883a5d-228e-48f5-b3d2-eb39e07630fa#individualfilesize
                // However, we can't enable this, otherwise OpenDAL behavior tests will try to test creating huge
                // file up to this size.
                // write_total_max_size: Some(250 * 1024 * 1024 * 1024),
                copy: true,
                rename: true,

                stat: true,
                stat_with_if_none_match: true,
                stat_has_content_length: true,
                stat_has_etag: true,
                stat_has_last_modified: true,
                stat_with_version: self.config.enable_versioning,

                delete: true,
                create_dir: true,

                list: true,
                list_with_limit: true,
                list_with_versions: self.config.enable_versioning,
                list_has_content_length: true,
                list_has_etag: true,
                list_has_last_modified: true,
                list_has_version: self.config.enable_versioning, // same as `list_with_versions`?

                shared: true,

                ..Default::default()
            });

        // allow deprecated api here for compatibility
        #[allow(deprecated)]
        if let Some(client) = self.http_client {
            info.update_http_client(|_| client);
        }

        let accessor_info = Arc::new(info);
        let mut signer = OneDriveSigner::new(accessor_info.clone());

        // Requires OAuth 2.0 tokens:
        // - `access_token` (the short-lived token)
        // - `refresh_token` flow (the long term token)
        // to be mutually exclusive for setting up for implementation simplicity
        match (self.config.access_token, self.config.refresh_token) {
            (Some(access_token), None) => {
                signer.access_token = access_token;
                signer.expires_in = DateTime::<Utc>::MAX_UTC;
            }
            (None, Some(refresh_token)) => {
                let client_id = self.config.client_id.ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "client_id must be set when refresh_token is set",
                    )
                    .with_context("service", Scheme::Onedrive)
                })?;

                signer.refresh_token = refresh_token;
                signer.client_id = client_id;
                if let Some(client_secret) = self.config.client_secret {
                    signer.client_secret = client_secret;
                }
            }
            (Some(_), Some(_)) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token and refresh_token cannot be set at the same time",
                )
                .with_context("service", Scheme::Onedrive))
            }
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token or refresh_token must be set",
                )
                .with_context("service", Scheme::Onedrive))
            }
        };

        let core = Arc::new(OneDriveCore {
            info: accessor_info,
            root,
            signer: Arc::new(Mutex::new(signer)),
        });

        Ok(OnedriveBackend { core })
    }
}
