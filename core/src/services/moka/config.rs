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

use serde::Deserialize;
use serde::Serialize;

/// Config for Moka services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MokaConfig {
    /// Name for this cache instance.
    pub name: Option<String>,
    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.max_capacity)
    pub max_capacity: Option<u64>,
    /// Sets the time to live of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_live)
    pub time_to_live: Option<Duration>,
    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    pub time_to_idle: Option<Duration>,
    /// Sets the segments number of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::segments`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.segments)
    pub num_segments: Option<usize>,

    /// root path of this backend
    pub root: Option<String>,
}

impl Debug for MokaConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaConfig")
            .field("name", &self.name)
            .field("max_capacity", &self.max_capacity)
            .field("time_to_live", &self.time_to_live)
            .field("time_to_idle", &self.time_to_idle)
            .field("num_segments", &self.num_segments)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}
