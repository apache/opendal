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

use super::backend::HdfsBuilder;
use serde::Deserialize;
use serde::Serialize;

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
///
/// Config for Hdfs services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HdfsConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// name node of this backend
    pub name_node: Option<String>,
    /// kerberos_ticket_cache_path of this backend
    pub kerberos_ticket_cache_path: Option<String>,
    /// user of this backend
    pub user: Option<String>,
    /// enable the append capacity
    pub enable_append: bool,
    /// atomic_write_dir of this backend
    pub atomic_write_dir: Option<String>,
}

impl Debug for HdfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsConfig")
            .field("root", &self.root)
            .field("name_node", &self.name_node)
            .field(
                "kerberos_ticket_cache_path",
                &self.kerberos_ticket_cache_path,
            )
            .field("user", &self.user)
            .field("enable_append", &self.enable_append)
            .field("atomic_write_dir", &self.atomic_write_dir)
            .finish_non_exhaustive()
    }
}

impl crate::Configurator for HdfsConfig {
    type Builder = HdfsBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let authority = uri.authority().ok_or_else(|| {
            crate::Error::new(crate::ErrorKind::ConfigInvalid, "uri authority is required")
                .with_context("service", crate::Scheme::Hdfs)
        })?;

        let mut map = uri.options().clone();
        map.insert("name_node".to_string(), format!("hdfs://{authority}"));

        if let Some(root) = uri.root() {
            if !root.is_empty() {
                map.insert("root".to_string(), root.to_string());
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        HdfsBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_name_node_and_root() {
        let uri = OperatorUri::new(
            "hdfs://cluster.local:8020/user/data".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = HdfsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.name_node.as_deref(), Some("hdfs://cluster.local:8020"));
        assert_eq!(cfg.root.as_deref(), Some("user/data"));
    }
}
