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

use super::backend::HdfsNativeBuilder;
use serde::Deserialize;
use serde::Serialize;

/// Config for HdfsNative services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HdfsNativeConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// name_node of this backend
    pub name_node: Option<String>,
    /// enable the append capacity
    pub enable_append: bool,
}

impl Debug for HdfsNativeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsNativeConfig")
            .field("root", &self.root)
            .field("name_node", &self.name_node)
            .field("enable_append", &self.enable_append)
            .finish_non_exhaustive()
    }
}

impl crate::Configurator for HdfsNativeConfig {
    type Builder = HdfsNativeBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let authority = uri.authority().ok_or_else(|| {
            crate::Error::new(crate::ErrorKind::ConfigInvalid, "uri authority is required")
                .with_context("service", crate::Scheme::HdfsNative)
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
        HdfsNativeBuilder { config: self }
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
            "hdfs_native://namenode:9000/user/project".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = HdfsNativeConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.name_node.as_deref(), Some("hdfs://namenode:9000"));
        assert_eq!(cfg.root.as_deref(), Some("user/project"));
    }
}
