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

use opendal_core::*;
use serde::Deserialize;
use serde::Serialize;

use super::backend::RocksdbBuilder;

/// Config for Rocksdb Service.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct RocksdbConfig {
    /// The path to the rocksdb data directory.
    pub datadir: Option<String>,
    /// the working directory of the service. Can be "/path/to/dir"
    ///
    /// default is "/"
    pub root: Option<String>,
}

impl Configurator for RocksdbConfig {
    type Builder = RocksdbBuilder;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut map = uri.options().clone();

        if let Some(path) = uri.root()
            && !path.is_empty()
            && !map.contains_key("datadir")
        {
            if let Some(host) = uri.name()
                && !host.eq_ignore_ascii_case("localhost")
            {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "rocksdb uri must not contain a host, use rocksdb:///path",
                )
                .with_context("host", host));
            }

            map.insert("datadir".to_string(), format!("/{path}"));
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        RocksdbBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_uri_sets_datadir_and_root() -> Result<()> {
        for base in [
            "rocksdb:///var/db?root=namespace",
            "rocksdb://LOCALHOST/var/db?root=namespace",
        ] {
            let uri = OperatorUri::new(base, [])?;
            let cfg = RocksdbConfig::from_uri(&uri)?;
            assert_eq!(cfg.datadir.as_deref(), Some("/var/db"));
            assert_eq!(cfg.root.as_deref(), Some("namespace"));
        }
        Ok(())
    }

    #[test]
    fn from_uri_rejects_host_when_using_uri_path() -> Result<()> {
        let uri = OperatorUri::new("rocksdb://remote.example/var/db?root=namespace", [])?;
        let err = RocksdbConfig::from_uri(&uri).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        Ok(())
    }

    #[test]
    fn from_uri_preserves_explicit_datadir() -> Result<()> {
        for (base, expected) in [
            (
                "rocksdb://remote.example/var/db?datadir=/explicit",
                Some("/explicit"),
            ),
            ("rocksdb://remote.example/var/db?datadir=", None),
        ] {
            let uri = OperatorUri::new(base, [])?;
            let cfg = RocksdbConfig::from_uri(&uri)?;
            assert_eq!(cfg.datadir.as_deref(), expected);
        }
        Ok(())
    }
}
