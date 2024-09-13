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

/// Config for Aliyun Drive services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct AliyunDriveConfig {
    /// The Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Default to `/` if not set.
    pub root: Option<String>,
    /// The access_token of this backend.
    ///
    /// Solution for client-only purpose. #4733
    ///
    /// Required if no client_id, client_secret and refresh_token are provided.
    pub access_token: Option<String>,
    /// The client_id of this backend.
    ///
    /// Required if no access_token is provided.
    pub client_id: Option<String>,
    /// The client_secret of this backend.
    ///
    /// Required if no access_token is provided.
    pub client_secret: Option<String>,
    /// The refresh_token of this backend.
    ///
    /// Required if no access_token is provided.
    pub refresh_token: Option<String>,
    /// The drive_type of this backend.
    ///
    /// All operations will happen under this type of drive.
    ///
    /// Available values are `default`, `backup` and `resource`.
    ///
    /// Fallback to default if not set or no other drives can be found.
    pub drive_type: String,
}

impl Debug for AliyunDriveConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("AliyunDriveConfig");

        d.field("root", &self.root)
            .field("drive_type", &self.drive_type);

        d.finish_non_exhaustive()
    }
}
