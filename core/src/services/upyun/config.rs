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

/// Config for upyun services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct UpyunConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// bucket address of this backend.
    pub bucket: String,
    /// username of this backend.
    pub operator: Option<String>,
    /// password of this backend.
    pub password: Option<String>,
}

impl Debug for UpyunConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);
        ds.field("bucket", &self.bucket);
        ds.field("operator", &self.operator);

        ds.finish()
    }
}
