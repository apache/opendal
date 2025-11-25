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

use super::builder::VercelArtifactsBuilder;

/// Config for Vercel Cache support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct VercelArtifactsConfig {
    /// The access token for Vercel.
    pub access_token: Option<String>,
}

impl Debug for VercelArtifactsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelArtifactsConfig")
            .finish_non_exhaustive()
    }
}

impl crate::Configurator for VercelArtifactsConfig {
    type Builder = VercelArtifactsBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        Self::from_iter(uri.options().clone())
    }

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        VercelArtifactsBuilder {
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
    fn from_uri_loads_access_token() {
        let uri = OperatorUri::new("vercel-artifacts://cache", vec![(
            "access_token".to_string(),
            "token123".to_string(),
        )])
        .unwrap();

        let cfg = VercelArtifactsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.access_token.as_deref(), Some("token123"));
    }
}
