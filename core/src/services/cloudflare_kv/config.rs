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
use std::time::Duration;

use super::backend::CloudflareKvBuilder;
use serde::Deserialize;
use serde::Serialize;

/// Cloudflare KV Service Support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CloudflareKvConfig {
    /// The token used to authenticate with CloudFlare.
    pub api_token: Option<String>,
    /// The account ID used to authenticate with CloudFlare. Used as URI path parameter.
    pub account_id: Option<String>,
    /// The namespace ID. Used as URI path parameter.
    pub namespace_id: Option<String>,
    /// The default ttl for write operations.
    pub default_ttl: Option<Duration>,

    /// Root within this backend.
    pub root: Option<String>,
}

impl Debug for CloudflareKvConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("CloudflareKvConfig");

        ds.field("root", &self.root);
        ds.field("account_id", &self.account_id);
        ds.field("namespace_id", &self.namespace_id);

        if self.api_token.is_some() {
            ds.field("api_token", &"<redacted>");
        }

        ds.finish()
    }
}

impl crate::Configurator for CloudflareKvConfig {
    type Builder = CloudflareKvBuilder;
    fn into_builder(self) -> Self::Builder {
        CloudflareKvBuilder {
            config: self,
            http_client: None,
        }
    }
}
