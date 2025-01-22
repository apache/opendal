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

/// Config for WebHDFS support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct WebhdfsConfig {
    /// Root for webhdfs.
    pub root: Option<String>,
    /// Endpoint for webhdfs.
    pub endpoint: Option<String>,
    /// Name of the user for webhdfs.
    pub user_name: Option<String>,
    /// Delegation token for webhdfs.
    pub delegation: Option<String>,
    /// Disable batch listing
    pub disable_list_batch: bool,
    /// atomic_write_dir of this backend
    pub atomic_write_dir: Option<String>,
}

impl Debug for WebhdfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhdfsConfig")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("user_name", &self.user_name)
            .field("atomic_write_dir", &self.atomic_write_dir)
            .finish_non_exhaustive()
    }
}
