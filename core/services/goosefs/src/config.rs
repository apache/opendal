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

use serde::Deserialize;
use serde::Serialize;

use super::backend::GooseFsBuilder;

/// Config for GooseFS service support.
///
/// GooseFS is a distributed caching file system,
/// accessed via native gRPC protocol (not REST Proxy).
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GooseFsConfig {
    /// Root path of this backend.
    ///
    /// All operations will happen under this root.
    /// Default to `/` if not set.
    pub root: Option<String>,

    /// Master address(es) in `host:port` format.
    ///
    /// For single master: `"10.0.0.1:9200"`
    /// For HA (comma-separated): `"10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200"`
    ///
    /// When multiple addresses are provided, the client uses
    /// `PollingMasterInquireClient` to discover the Primary Master automatically.
    ///
    /// Resolution precedence at `build()` time (highest → lowest), following
    /// `goosefs-sdk` `docs/CLIENT_CONFIGURATION.md` §1:
    ///   1. This field (when set on the builder / OpenDAL config map)
    ///   2. `GOOSEFS_MASTER_ADDR` environment variable
    ///   3. `goosefs.master.rpc.addresses` / `goosefs.master.hostname` in
    ///      `goosefs-site.properties`
    ///
    /// `build()` fails with `ConfigInvalid` only when **none** of the above
    /// supplies a master address.
    pub master_addr: Option<String>,

    /// Block size in bytes for new files (default: 64 MiB).
    pub block_size: Option<u64>,

    /// Chunk size in bytes for streaming RPCs (default: 1 MiB).
    pub chunk_size: Option<u64>,

    /// Default write type for new files.
    ///
    /// Supported values: `"must_cache"`, `"cache_through"`, `"through"`, `"async_through"`.
    /// Default: `"must_cache"`.
    pub write_type: Option<String>,

    /// Authentication type.
    ///
    /// Supported values: `"nosasl"`, `"simple"`.
    /// Default: `"simple"` — PLAIN SASL with username.
    /// `"nosasl"` — skip authentication entirely.
    pub auth_type: Option<String>,

    /// Authentication username.
    ///
    /// Used in SIMPLE mode as the login identity.
    /// Default: current OS user (`$USER` / `$USERNAME`).
    pub auth_username: Option<String>,
}

impl Debug for GooseFsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GooseFsConfig")
            .field("root", &self.root)
            .field("master_addr", &self.master_addr)
            .field("block_size", &self.block_size)
            .field("chunk_size", &self.chunk_size)
            .field("write_type", &self.write_type)
            .field("auth_type", &self.auth_type)
            .field("auth_username", &self.auth_username)
            .finish_non_exhaustive()
    }
}

impl opendal_core::Configurator for GooseFsConfig {
    type Builder = GooseFsBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();
        if let Some(authority) = uri.authority() {
            // goosefs://host:port/path → master_addr = "host:port"
            map.insert("master_addr".to_string(), authority.to_string());
        }
        if let Some(root) = uri.root() {
            if !root.is_empty() {
                map.insert("root".to_string(), root.to_string());
            }
        }
        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        GooseFsBuilder { config: self }
    }
}
