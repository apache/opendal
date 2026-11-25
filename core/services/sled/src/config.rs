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

use super::backend::SledBuilder;

/// Config for Sled services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SledConfig {
    /// That path to the sled data directory.
    pub datadir: Option<String>,
    /// The tree for sled.
    pub tree: Option<String>,
    /// The root for sled.
    pub root: Option<String>,
}

impl Debug for SledConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SledConfig")
            .field("tree", &self.tree)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl opendal_core::Configurator for SledConfig {
    type Builder = SledBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(path) = uri.root()
            && !path.is_empty()
        {
            map.entry("datadir".to_string())
                .or_insert_with(|| format!("/{path}"));
        }

        // A local path has no host. Ignoring one would silently move the
        // datadir: `sled://tmp/data` would become `/data`. An explicit
        // `datadir` option takes precedence, as for fs.
        if !uri.options().contains_key("datadir")
            && let Some(host) = uri.name()
            && !host.eq_ignore_ascii_case("localhost")
        {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "sled uri must not contain a host, use sled:///path",
            )
            .with_context("host", host));
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
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_sets_datadir_tree_and_root() {
        let uri = OperatorUri::new(
            "sled:///var/data/sled?tree=cache&root=items",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SledConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.datadir.as_deref(), Some("/var/data/sled"));
        assert_eq!(cfg.tree.as_deref(), Some("cache"));
        assert_eq!(cfg.root.as_deref(), Some("items"));
    }

    #[test]
    fn from_uri_rejects_host() {
        let no_opts = Vec::<(String, String)>::new;
        let uri = OperatorUri::new("sled://tmp/data", no_opts()).unwrap();
        let err = SledConfig::from_uri(&uri).unwrap_err();
        assert_eq!(err.kind(), opendal_core::ErrorKind::ConfigInvalid);

        let uri = OperatorUri::new("sled://localhost/data", no_opts()).unwrap();
        assert_eq!(
            SledConfig::from_uri(&uri).unwrap().datadir.as_deref(),
            Some("/data")
        );

        // An explicit `datadir` option takes precedence over the host.
        let uri = OperatorUri::new(
            "sled://tmp/data",
            [("datadir".to_string(), "/explicit".to_string())],
        )
        .unwrap();
        assert_eq!(
            SledConfig::from_uri(&uri).unwrap().datadir.as_deref(),
            Some("/explicit")
        );
    }
}
