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

use crate::backend::BaiduNetdiskBuilder;
use opendal_core::*;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration for the Baidu Netdisk Open Platform.
#[derive(Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct BaiduNetdiskConfig {
    /// Required absolute root inside `/apps/<application name>/`.
    pub root: Option<String>,
    /// Initial access token; refresh credentials allow renewal when it expires.
    pub access_token: Option<String>,
    /// AppKey, required together with client_secret and refresh_token.
    pub client_id: Option<String>,
    /// SecretKey, required together with client_id and refresh_token.
    pub client_secret: Option<String>,
    /// Latest single-use refresh token returned by Baidu OAuth.
    pub refresh_token: Option<String>,
}

impl fmt::Debug for BaiduNetdiskConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BaiduNetdiskConfig")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl Configurator for BaiduNetdiskConfig {
    type Builder = BaiduNetdiskBuilder;
    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut map = uri.options().clone();
        if let Some(root) = uri.root() {
            map.insert("root".into(), root.into());
        }
        Self::from_iter(map)
    }
    fn into_builder(self) -> Self::Builder {
        BaiduNetdiskBuilder {
            config: self,
            on_refresh: None,
        }
    }
}
