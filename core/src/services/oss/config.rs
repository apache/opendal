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

use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

/// Config for Aliyun Object Storage Service (OSS) support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct OssConfig {
    /// Root for oss.
    pub root: Option<String>,

    /// Endpoint for oss.
    pub endpoint: Option<String>,
    /// Presign endpoint for oss.
    pub presign_endpoint: Option<String>,
    /// Bucket for oss.
    pub bucket: String,

    // OSS features
    /// Server side encryption for oss.
    pub server_side_encryption: Option<String>,
    /// Server side encryption key id for oss.
    pub server_side_encryption_key_id: Option<String>,
    /// Allow anonymous for oss.
    pub allow_anonymous: bool,

    // authenticate options
    /// Access key id for oss.
    pub access_key_id: Option<String>,
    /// Access key secret for oss.
    pub access_key_secret: Option<String>,
    /// batch_max_operations
    pub batch_max_operations: Option<usize>,
}

impl Debug for OssConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");
        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("allow_anonymous", &self.allow_anonymous);

        d.finish_non_exhaustive()
    }
}
