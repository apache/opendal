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

/// Tencent-Cloud COS services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
pub struct CosConfig {
    /// Root of this backend.
    pub root: Option<String>,
    /// Endpoint of this backend.
    pub endpoint: Option<String>,
    /// Secret ID of this backend.
    pub secret_id: Option<String>,
    /// Secret key of this backend.
    pub secret_key: Option<String>,
    /// Bucket of this backend.
    pub bucket: Option<String>,
    /// is bucket versioning enabled for this bucket
    pub enable_versioning: bool,
    /// Disable config load so that opendal will not load config from
    pub disable_config_load: bool,
}

impl Debug for CosConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CosConfig")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("secret_id", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}
