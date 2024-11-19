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

/// Config for icloud services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct IcloudConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// default to `/` if not set.
    pub root: Option<String>,
    /// apple_id of this backend.
    ///
    /// apple_id must be full, mostly like `example@gmail.com`.
    pub apple_id: Option<String>,
    /// password of this backend.
    ///
    /// password must be full.
    pub password: Option<String>,

    /// Session
    ///
    /// token must be valid.
    pub trust_token: Option<String>,
    /// ds_web_auth_token must be set in Session
    pub ds_web_auth_token: Option<String>,
    /// enable the china origin
    /// China region `origin` Header needs to be set to "https://www.icloud.com.cn".
    ///
    /// otherwise Apple server will return 302.
    pub is_china_mainland: bool,
}

impl Debug for IcloudConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("IcloudBuilder");
        d.field("root", &self.root);
        d.field("is_china_mainland", &self.is_china_mainland);
        d.finish_non_exhaustive()
    }
}
