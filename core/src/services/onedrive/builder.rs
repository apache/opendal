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

use log::debug;
use services::onedrive::core::OneDriveCore;

use super::backend::OneDriveBackend;
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
    /// set the bearer access token for OneDrive
    ///
    /// default: no access token, which leads to failure
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = Some(access_token.to_string());
        self
    }

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
    pub fn http_client(mut self, http_client: HttpClient) -> Self {
        self.http_client = Some(http_client);
        self
    }
}

impl Builder for OnedriveBuilder {
    const SCHEME: Scheme = Scheme::Onedrive;
    type Config = OnedriveConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let access_token = self
            .config
            .access_token
            .ok_or(Error::new(ErrorKind::ConfigInvalid, "access_token not set"))?;

        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Onedrive)
            .set_root(&root)
            .set_native_capability(Capability {
                read: true,
                write: true,

                stat: true,
                stat_has_content_length: true,
                stat_has_etag: true,
                stat_has_last_modified: true,

                delete: true,
                create_dir: true,

                list: true,
                list_has_content_length: true,
                list_has_etag: true,
                list_has_last_modified: true,

                shared: true,

                ..Default::default()
            });

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Onedrive)
            })?
        };
        let core = Arc::new(OneDriveCore {
            info: Arc::new(info),
            root,
            access_token,
            client,
        });

        Ok(OneDriveBackend { core })
    }
}
