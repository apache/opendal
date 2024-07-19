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

use log::debug;
use serde::{Deserialize, Serialize};

use super::backend::OnedriveBackend;
use crate::raw::normalize_root;
use crate::raw::ConfigDeserializer;
use crate::raw::HttpClient;
use crate::Scheme;
use crate::*;

/// Config for [OneDrive](https://onedrive.com) backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct OnedriveConfig {
    /// bearer access token for OneDrive
    pub access_token: Option<String>,
    /// root path of OneDrive folder.
    pub root: Option<String>,
}

impl Debug for OnedriveConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnedriveConfig")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// [OneDrive](https://onedrive.com) backend support.
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
    pub fn access_token(&mut self, access_token: &str) -> &mut Self {
        self.config.access_token = Some(access_token.to_string());
        self
    }

    /// Set root path of OneDrive folder.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = Some(root.to_string());
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

impl Builder for OnedriveBuilder {
    const SCHEME: Scheme = Scheme::Onedrive;

    type Accessor = OnedriveBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        // Deserialize the configuration from the HashMap.
        let config = OnedriveConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        // Create an OnedriveBuilder instance with the deserialized config.
        OnedriveBuilder {
            config,
            http_client: None,
        }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let root = normalize_root(&self.config.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Onedrive)
            })?
        };

        match self.config.access_token.clone() {
            Some(access_token) => Ok(OnedriveBackend::new(root, access_token, client)),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "access_token not set")),
        }
    }
}
