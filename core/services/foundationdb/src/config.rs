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

use serde::Deserialize;
use serde::Serialize;

use super::backend::FoundationdbBuilder;

/// [foundationdb](https://www.foundationdb.org/) service support.
/// Config for FoundationDB.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FoundationdbConfig {
    /// root of the backend.
    pub root: Option<String>,
    /// config_path for the backend.
    pub config_path: Option<String>,
}

impl opendal_core::Configurator for FoundationdbConfig {
    type Builder = FoundationdbBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        // With no URI path, the builder would open the default cluster.
        if !uri.options().contains_key("config_path")
            && let Some(host) = uri.name()
            && !host.eq_ignore_ascii_case("localhost")
        {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "foundationdb uri must not contain a host, use foundationdb:///path",
            )
            .with_context("host", host));
        }

        let mut map = uri.options().clone();

        if let Some(path) = uri.root()
            && !path.is_empty()
        {
            map.entry("config_path".to_string())
                .or_insert_with(|| format!("/{path}"));
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
    use opendal_core::Configurator;
    use opendal_core::ErrorKind;
    use opendal_core::OperatorUri;
    use opendal_core::Result;

    #[test]
    fn from_uri_sets_config_path_and_root() -> Result<()> {
        for base in [
            "foundationdb:///etc/foundationdb/fdb.cluster?root=data",
            "foundationdb://LOCALHOST/etc/foundationdb/fdb.cluster?root=data",
        ] {
            let uri = OperatorUri::new(base, [])?;
            let cfg = FoundationdbConfig::from_uri(&uri)?;
            assert_eq!(
                cfg.config_path.as_deref(),
                Some("/etc/foundationdb/fdb.cluster")
            );
            assert_eq!(cfg.root.as_deref(), Some("data"));
        }
        Ok(())
    }

    #[test]
    fn from_uri_rejects_host_without_config_path_option() -> Result<()> {
        for base in [
            "foundationdb://remote.example/etc/foundationdb/fdb.cluster?root=data",
            "foundationdb://remote.example?root=data",
        ] {
            let uri = OperatorUri::new(base, [])?;
            let err = FoundationdbConfig::from_uri(&uri).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "{base}");
        }
        Ok(())
    }

    #[test]
    fn from_uri_preserves_explicit_config_path() -> Result<()> {
        for (base, expected) in [
            (
                "foundationdb://remote.example/uri.cluster?config_path=/explicit.cluster",
                Some("/explicit.cluster"),
            ),
            (
                "foundationdb://remote.example/uri.cluster?config_path=",
                None,
            ),
        ] {
            let uri = OperatorUri::new(base, [])?;
            let cfg = FoundationdbConfig::from_uri(&uri)?;
            assert_eq!(cfg.config_path.as_deref(), expected);
        }
        Ok(())
    }
}
