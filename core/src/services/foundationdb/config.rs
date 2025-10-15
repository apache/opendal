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

use super::backend::FoundationdbBuilder;
use serde::Deserialize;
use serde::Serialize;

/// [foundationdb](https://www.foundationdb.org/) service support.
///Config for FoundationDB.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FoundationdbConfig {
    ///root of the backend.
    pub root: Option<String>,
    ///config_path for the backend.
    pub config_path: Option<String>,
}

impl Debug for FoundationdbConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("FoundationConfig");

        ds.field("root", &self.root);
        ds.field("config_path", &self.config_path);

        ds.finish()
    }
}

impl crate::Configurator for FoundationdbConfig {
    type Builder = FoundationdbBuilder;
    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(path) = uri.root() {
            if !path.is_empty() {
                if let Some((config, root)) = path.rsplit_once('/') {
                    if !config.is_empty() {
                        map.entry("config_path".to_string())
                            .or_insert_with(|| format!("/{config}"));
                    }
                    if !root.is_empty() {
                        map.insert("root".to_string(), root.to_string());
                    }
                } else {
                    map.entry("config_path".to_string())
                        .or_insert_with(|| format!("/{path}"));
                }
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        FoundationdbBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_config_path_and_root() {
        let uri = OperatorUri::new(
            "foundationdb:/etc/foundationdb/fdb.cluster/data"
                .parse()
                .unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = FoundationdbConfig::from_uri(&uri).unwrap();
        assert_eq!(
            cfg.config_path.as_deref(),
            Some("/etc/foundationdb/fdb.cluster")
        );
        assert_eq!(cfg.root.as_deref(), Some("data"));
    }
}
