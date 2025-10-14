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
use super::FTP_SCHEME;
use super::backend::FtpBuilder;

/// Config for Ftp services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FtpConfig {
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// root of this backend
    pub root: Option<String>,
    /// user of this backend
    pub user: Option<String>,
    /// password of this backend
    pub password: Option<String>,
}

impl Debug for FtpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpConfig")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl crate::Configurator for FtpConfig {
    type Builder = FtpBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let authority = uri.authority().ok_or_else(|| {
            crate::Error::new(crate::ErrorKind::ConfigInvalid, "uri authority is required")
                .with_context("service", crate::Scheme::Ftp)
        })?;

        let mut map = uri.options().clone();
        map.insert(
            "endpoint".to_string(),
            format!("{FTP_SCHEME}://{authority}"),
        );

        if let Some(root) = uri.root() {
            map.insert("root".to_string(), root.to_string());
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        FtpBuilder { config: self }
    }
}
