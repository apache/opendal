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

use super::builder::IpmfsBuilder;

/// Config for IPFS MFS support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct IpmfsConfig {
    /// Root for ipfs.
    pub root: Option<String>,
    /// Endpoint for ipfs.
    pub endpoint: Option<String>,
}

impl crate::Configurator for IpmfsConfig {
    type Builder = IpmfsBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let authority = uri.authority().ok_or_else(|| {
            crate::Error::new(crate::ErrorKind::ConfigInvalid, "uri authority is required")
                .with_context("service", crate::Scheme::Ipmfs)
        })?;

        let mut map = uri.options().clone();
        map.insert("endpoint".to_string(), format!("http://{authority}"));

        if let Some(root) = uri.root() {
            if !root.is_empty() {
                map.insert("root".to_string(), root.to_string());
            }
        }

        Self::from_iter(map)
    }

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        IpmfsBuilder {
            config: self,
            http_client: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_endpoint_and_root() {
        let uri = OperatorUri::new(
            "ipmfs://localhost:5001/mfs/path",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = IpmfsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("http://localhost:5001"));
        assert_eq!(cfg.root.as_deref(), Some("mfs/path"));
    }
}
