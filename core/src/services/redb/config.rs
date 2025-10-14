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

use super::backend::RedbBuilder;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde::Serialize;

/// Config for redb service support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct RedbConfig {
    /// path to the redb data directory.
    pub datadir: Option<String>,
    /// The root for redb.
    pub root: Option<String>,
    /// The table name for redb.
    pub table: Option<String>,
}

impl crate::Configurator for RedbConfig {
    type Builder = RedbBuilder;
    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(authority) = uri.authority() {
            let decoded = percent_decode_str(authority).decode_utf8_lossy();
            if !decoded.is_empty() {
                map.entry("datadir".to_string())
                    .or_insert_with(|| decoded.to_string());
            }
        }

        if let Some(path) = uri.root() {
            if !path.is_empty() {
                let (table, rest) = match path.split_once('/') {
                    Some((table, remainder)) => (table, Some(remainder)),
                    None => (path, None),
                };

                if !table.is_empty() {
                    map.entry("table".to_string())
                        .or_insert_with(|| table.to_string());
                }

                if let Some(root) = rest {
                    if !root.is_empty() {
                        map.insert("root".to_string(), root.to_string());
                    }
                }
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        RedbBuilder {
            config: self,
            database: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_datadir_table_and_root() {
        let uri = OperatorUri::new(
            "redb://%2Ftmp%2Fredb/op_table/cache".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = RedbConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.datadir.as_deref(), Some("/tmp/redb"));
        assert_eq!(cfg.table.as_deref(), Some("op_table"));
        assert_eq!(cfg.root.as_deref(), Some("cache"));
    }
}
