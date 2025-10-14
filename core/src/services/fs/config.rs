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
use super::backend::FsBuilder;

/// config for file system
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FsConfig {
    /// root dir for backend
    pub root: Option<String>,

    /// tmp dir for atomic write
    pub atomic_write_dir: Option<String>,
}

impl crate::Configurator for FsConfig {
    type Builder = FsBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(value) = match (uri.name(), uri.root()) {
            (Some(name), Some(rest)) if !rest.is_empty() => Some(format!("/{}/{}", name, rest)),
            (Some(name), _) => Some(format!("/{}", name)),
            (None, Some(rest)) if !rest.is_empty() => Some(format!("/{}", rest)),
            (None, Some(rest)) => Some(rest.to_string()),
            _ => None,
        } {
            map.insert("root".to_string(), value);
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        FsBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;
    use http::Uri;

    #[test]
    fn from_uri_extracts_root() {
        let uri = OperatorUri::new(
            Uri::from_static("fs://tmp/data"),
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let cfg = FsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.root.as_deref(), Some("/tmp/data"));
    }
}
