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

use super::backend::SledBuilder;
use serde::Deserialize;
use serde::Serialize;

/// Config for Sled services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SledConfig {
    /// That path to the sled data directory.
    pub datadir: Option<String>,
    /// The root for sled.
    pub root: Option<String>,
    /// The tree for sled.
    pub tree: Option<String>,
}

impl Debug for SledConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SledConfig")
            .field("datadir", &self.datadir)
            .field("root", &self.root)
            .field("tree", &self.tree)
            .finish()
    }
}

impl crate::Configurator for SledConfig {
    type Builder = SledBuilder;
    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(path) = uri.root() {
            if !path.is_empty() {
                if let Some((datadir_part, rest)) = path.split_once("//") {
                    if !datadir_part.is_empty() {
                        map.entry("datadir".to_string())
                            .or_insert_with(|| format!("/{datadir_part}"));
                    }

                    let mut segments = rest.splitn(2, '/');
                    if let Some(tree) = segments.next() {
                        if !tree.is_empty() {
                            map.entry("tree".to_string())
                                .or_insert_with(|| tree.to_string());
                        }
                    }

                    if let Some(root) = segments.next() {
                        if !root.is_empty() {
                            map.insert("root".to_string(), root.to_string());
                        }
                    }
                } else {
                    map.entry("datadir".to_string())
                        .or_insert_with(|| format!("/{path}"));
                }
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        SledBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_datadir_tree_and_root() {
        let uri = OperatorUri::new(
            "sled:/var/data/sled//cache/items".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SledConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.datadir.as_deref(), Some("/var/data/sled"));
        assert_eq!(cfg.tree.as_deref(), Some("cache"));
        assert_eq!(cfg.root.as_deref(), Some("items"));
    }
}
