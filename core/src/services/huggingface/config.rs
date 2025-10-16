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

use super::backend::HuggingfaceBuilder;
use serde::Deserialize;
use serde::Serialize;

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
}

impl Debug for HuggingfaceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("HuggingfaceConfig");

        if let Some(repo_type) = &self.repo_type {
            ds.field("repo_type", &repo_type);
        }
        if let Some(repo_id) = &self.repo_id {
            ds.field("repo_id", &repo_id);
        }
        if let Some(revision) = &self.revision {
            ds.field("revision", &revision);
        }
        if let Some(root) = &self.root {
            ds.field("root", &root);
        }
        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

impl crate::Configurator for HuggingfaceConfig {
    type Builder = HuggingfaceBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(repo_type) = uri.name() {
            if !repo_type.is_empty() {
                map.insert("repo_type".to_string(), repo_type.to_string());
            }
        }

        let raw_path = uri.root().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "uri path must include owner and repo",
            )
            .with_context("service", crate::Scheme::Huggingface)
        })?;

        let mut segments = raw_path.splitn(4, '/');
        let owner = segments.next().filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "repository owner is required in uri path",
            )
            .with_context("service", crate::Scheme::Huggingface)
        })?;
        let repo = segments.next().filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "repository name is required in uri path",
            )
            .with_context("service", crate::Scheme::Huggingface)
        })?;

        map.insert("repo_id".to_string(), format!("{owner}/{repo}"));

        if let Some(segment) = segments.next() {
            if map.contains_key("revision") {
                let mut root_value = segment.to_string();
                if let Some(rest) = segments.next() {
                    if !rest.is_empty() {
                        if !root_value.is_empty() {
                            root_value.push('/');
                            root_value.push_str(rest);
                        } else {
                            root_value = rest.to_string();
                        }
                    }
                }
                if !root_value.is_empty() {
                    map.insert("root".to_string(), root_value);
                }
            } else {
                if !segment.is_empty() {
                    map.insert("revision".to_string(), segment.to_string());
                }
                if let Some(rest) = segments.next() {
                    if !rest.is_empty() {
                        map.insert("root".to_string(), rest.to_string());
                    }
                }
            }
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
    fn from_uri_requires_owner_and_repo() {
        let uri = OperatorUri::new(
            "huggingface://model/opendal",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert!(HuggingfaceConfig::from_uri(&uri).is_err());
    }
}
