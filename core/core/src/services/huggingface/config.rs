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

use super::HUGGINGFACE_SCHEME;
use super::backend::HuggingfaceBuilder;

/// Configuration for Huggingface service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HuggingfaceConfig {
    /// Repo type of this backend. Default is model.
    ///
    /// Available values:
    /// - model
    /// - dataset
    /// - datasets (alias for dataset)
    pub repo_type: Option<String>,
    /// Repo id of this backend.
    ///
    /// This is required.
    pub repo_id: Option<String>,
    /// Revision of this backend.
    ///
    /// Default is main.
    pub revision: Option<String>,
    /// Root of this backend. Can be "/path/to/dir".
    ///
    /// Default is "/".
    pub root: Option<String>,
    /// Token of this backend.
    ///
    /// This is optional.
    pub token: Option<String>,
    /// Endpoint of the Huggingface Hub.
    ///
    /// Default is "https://huggingface.co".
    pub endpoint: Option<String>,
}

impl Debug for HuggingfaceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HuggingfaceConfig")
            .field("repo_type", &self.repo_type)
            .field("repo_id", &self.repo_id)
            .field("revision", &self.revision)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl crate::Configurator for HuggingfaceConfig {
    type Builder = HuggingfaceBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();
        map.retain(|_, v| !v.is_empty());

        if let Some(repo_type) = uri.name() {
            if !repo_type.is_empty() {
                map.insert("repo_type".to_string(), repo_type.to_string());
            }
        }

        if let Some(raw_path) = uri.root() {
            let parts: Vec<_> = raw_path.split('/').filter(|s| !s.is_empty()).collect();

            if parts.len() >= 2 {
                map.insert("repo_id".to_string(), format!("{}/{}", parts[0], parts[1]));

                if parts.len() >= 3 {
                    if map.contains_key("revision") {
                        let root_value = parts[2..].join("/");
                        if !root_value.is_empty() {
                            map.insert("root".to_string(), root_value);
                        }
                    } else {
                        map.insert("revision".to_string(), parts[2].to_string());
                        if parts.len() > 3 {
                            let root_value = parts[3..].join("/");
                            if !root_value.is_empty() {
                                map.insert("root".to_string(), root_value);
                            }
                        }
                    }
                }
            } else if parts.is_empty() {
                // no owner/repo provided, fall back to options-only
            } else {
                return Err(crate::Error::new(
                    crate::ErrorKind::ConfigInvalid,
                    "repository owner and name are required in uri path",
                )
                .with_context("service", HUGGINGFACE_SCHEME));
            }
        }

        if !map.contains_key("repo_id") {
            return Err(crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "repo_id is required via uri path or option",
            )
            .with_context("service", HUGGINGFACE_SCHEME));
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        HuggingfaceBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_repo_type_id_and_revision() {
        let uri = OperatorUri::new(
            "huggingface://model/opendal/sample/main/dataset",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = HuggingfaceConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.repo_type.as_deref(), Some("model"));
        assert_eq!(cfg.repo_id.as_deref(), Some("opendal/sample"));
        assert_eq!(cfg.revision.as_deref(), Some("main"));
        assert_eq!(cfg.root.as_deref(), Some("dataset"));
    }

    #[test]
    fn from_uri_uses_existing_revision_and_sets_root() {
        let uri = OperatorUri::new(
            "huggingface://dataset/opendal/sample/data/train",
            vec![("revision".to_string(), "dev".to_string())],
        )
        .unwrap();

        let cfg = HuggingfaceConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.repo_type.as_deref(), Some("dataset"));
        assert_eq!(cfg.repo_id.as_deref(), Some("opendal/sample"));
        assert_eq!(cfg.revision.as_deref(), Some("dev"));
        assert_eq!(cfg.root.as_deref(), Some("data/train"));
    }

    #[test]
    fn from_uri_allows_options_only() {
        let uri = OperatorUri::new(
            "huggingface",
            vec![
                ("repo_type".to_string(), "model".to_string()),
                ("repo_id".to_string(), "opendal/sample".to_string()),
                ("revision".to_string(), "main".to_string()),
                ("root".to_string(), "".to_string()),
            ],
        )
        .unwrap();

        let cfg = HuggingfaceConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.repo_type.as_deref(), Some("model"));
        assert_eq!(cfg.repo_id.as_deref(), Some("opendal/sample"));
        assert_eq!(cfg.revision.as_deref(), Some("main"));
        assert!(cfg.root.is_none());
    }

    #[test]
    fn from_uri_requires_owner_and_repo() {
        let uri = OperatorUri::new(
            "huggingface://model/opendal",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert!(HuggingfaceConfig::from_uri(&uri).is_err());
    }
}
