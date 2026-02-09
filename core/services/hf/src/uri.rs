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

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;
use serde::Serialize;

use super::HUGGINGFACE_SCHEME;
use opendal_core::raw::*;

/// Repository type of Huggingface. Supports `model`, `dataset`, and `space`.
/// [Reference](https://huggingface.co/docs/hub/repositories)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RepoType {
    #[default]
    Model,
    Dataset,
    Space,
}

impl RepoType {
    pub fn parse(s: &str) -> opendal_core::Result<Self> {
        match s.to_lowercase().replace(' ', "").as_str() {
            "model" | "models" => Ok(Self::Model),
            "dataset" | "datasets" => Ok(Self::Dataset),
            "space" | "spaces" => Ok(Self::Space),
            other => Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                format!("unknown repo type: {other}"),
            )
            .with_context("service", HUGGINGFACE_SCHEME)),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Dataset => "dataset",
            Self::Space => "space",
        }
    }

    pub fn as_plural_str(&self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HfRepo {
    pub repo_type: RepoType,
    pub repo_id: String,
    pub revision: Option<String>,
}

impl HfRepo {
    pub fn new(repo_type: RepoType, repo_id: String, revision: Option<String>) -> Self {
        Self {
            repo_type,
            repo_id,
            revision,
        }
    }

    /// Return the revision, defaulting to "main" if unset.
    pub fn revision(&self) -> &str {
        self.revision.as_deref().unwrap_or("main")
    }

    /// Create an `HfUri` for the given root and path within this repo.
    pub fn uri(&self, root: &str, path: &str) -> HfUri {
        HfUri {
            repo: self.clone(),
            path: build_abs_path(root, path)
                .trim_start_matches('/')
                .trim_end_matches('/')
                .to_string(),
        }
    }

    /// Build the paths-info API URL for this repository.
    pub fn paths_info_url(&self, endpoint: &str) -> String {
        format!(
            "{}/api/{}/{}/paths-info/{}",
            endpoint,
            self.repo_type.as_plural_str(),
            &self.repo_id,
            percent_encode_revision(self.revision()),
        )
    }

    /// Build the XET token API URL for this repository.
    #[cfg(feature = "xet")]
    pub fn xet_token_url(&self, endpoint: &str, token_type: &str) -> String {
        format!(
            "{}/api/{}/{}/xet-{}-token/{}",
            endpoint,
            self.repo_type.as_plural_str(),
            &self.repo_id,
            token_type,
            self.revision(),
        )
    }
}

/// Parsed Hugging Face URI following the official format:
/// `hf://[<repo_type_prefix>/]<repo_id>[@<revision>][/<path_in_repo>]`
///
/// Use this directly when you need access to `path_in_repo` separately
/// from the config (e.g. to resolve a specific file within the repo).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HfUri {
    pub repo: HfRepo,
    pub path: String,
}

impl HfUri {
    /// Parse a Hugging Face path into its components.
    /// Path format: `[<repo_type>/]<repo_id>[@<revision>][/<path_in_repo>]`
    pub fn parse(path: &str) -> opendal_core::Result<Self> {
        if path.is_empty() {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "repo_id is required in uri path",
            )
            .with_context("service", HUGGINGFACE_SCHEME));
        }

        let mut path = path.to_string();

        // Strip repo_type prefix if present (e.g. "datasets/user/repo" → "user/repo")
        let repo_type = if let Some((first, rest)) = path.split_once('/') {
            if let Ok(rt) = RepoType::parse(first) {
                path = rest.to_string();
                rt
            } else {
                RepoType::Model
            }
        } else if RepoType::parse(&path).is_ok() {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "repository name is required in uri path",
            )
            .with_context("service", HUGGINGFACE_SCHEME));
        } else {
            RepoType::Model
        };

        // Parse repo_id, revision, and path_in_repo.
        // Path is now: <repo_id>[@<revision>][/<path_in_repo>]
        let (repo_id, revision, path_in_repo) = if path.contains('/') {
            // Check if @ appears in the first two segments (the repo_id portion).
            // This distinguishes "user/repo@rev/file" from "user/repo/path/to/@file".
            let first_two: String = path.splitn(3, '/').take(2).collect::<Vec<_>>().join("/");

            if first_two.contains('@') {
                let (repo_id, rev_and_path) = path.split_once('@').unwrap();
                let rev_and_path = rev_and_path.replace("%2F", "/");
                let (revision, path_in_repo) = Self::parse_revision(&rev_and_path);
                (repo_id.to_string(), Some(revision), path_in_repo)
            } else {
                let segments: Vec<_> = path.splitn(3, '/').collect();
                let repo_id = format!("{}/{}", segments[0], segments[1]);
                let path_in_repo = segments.get(2).copied().unwrap_or("").to_string();
                (repo_id, None, path_in_repo)
            }
        } else if let Some((repo_id, rev)) = path.split_once('@') {
            let rev = rev.replace("%2F", "/");
            (
                repo_id.to_string(),
                if rev.is_empty() { None } else { Some(rev) },
                String::new(),
            )
        } else {
            (path, None, String::new())
        };

        Ok(Self {
            repo: HfRepo::new(repo_type, repo_id, revision),
            path: path_in_repo,
        })
    }

    /// Split a string after `@` into (revision, path_in_repo).
    /// Handles special refs like `refs/convert/parquet` and `refs/pr/10`.
    fn parse_revision(rev_and_path: &str) -> (String, String) {
        if !rev_and_path.contains('/') {
            return (rev_and_path.to_string(), String::new());
        }

        // Match special refs: refs/(convert|pr)/<segment>
        if let Some(rest) = rev_and_path.strip_prefix("refs/convert/") {
            return if let Some(slash) = rest.find('/') {
                (
                    rev_and_path[..14 + slash].to_string(),
                    rest[slash + 1..].to_string(),
                )
            } else {
                (rev_and_path.to_string(), String::new())
            };
        }
        if let Some(rest) = rev_and_path.strip_prefix("refs/pr/") {
            return if let Some(slash) = rest.find('/') {
                let revision = format!("refs/pr/{}", &rest[..slash]);
                (revision, rest[slash + 1..].to_string())
            } else {
                (rev_and_path.to_string(), String::new())
            };
        }

        // Regular revision: split on first /
        let (rev, path) = rev_and_path.split_once('/').unwrap();
        (rev.to_string(), path.to_string())
    }

    /// Return the revision, defaulting to "main" if unset.
    pub fn revision(&self) -> &str {
        self.repo.revision()
    }

    /// Build the resolve URL for this URI.
    pub fn resolve_url(&self, endpoint: &str) -> String {
        let revision = percent_encode_revision(self.revision());
        let path = percent_encode_path(&self.path);
        match self.repo.repo_type {
            RepoType::Model => {
                format!(
                    "{}/{}/resolve/{}/{}",
                    endpoint, &self.repo.repo_id, revision, path
                )
            }
            RepoType::Dataset => {
                format!(
                    "{}/datasets/{}/resolve/{}/{}",
                    endpoint, &self.repo.repo_id, revision, path
                )
            }
            RepoType::Space => {
                format!(
                    "{}/spaces/{}/resolve/{}/{}",
                    endpoint, &self.repo.repo_id, revision, path
                )
            }
        }
    }

    /// Build the paths-info API URL for this URI.
    pub fn paths_info_url(&self, endpoint: &str) -> String {
        self.repo.paths_info_url(endpoint)
    }

    /// Build the file tree API URL for this URI.
    pub fn file_tree_url(&self, endpoint: &str, recursive: bool, cursor: Option<&str>) -> String {
        let mut url = format!(
            "{}/api/{}/{}/tree/{}/{}?expand=True",
            endpoint,
            self.repo.repo_type.as_plural_str(),
            &self.repo.repo_id,
            percent_encode_revision(self.revision()),
            percent_encode_path(&self.path),
        );

        if recursive {
            url.push_str("&recursive=True");
        }

        if let Some(cursor_val) = cursor {
            url.push_str(&format!("&cursor={}", cursor_val));
        }

        url
    }

    /// Build the preupload API URL for this URI.
    pub fn preupload_url(&self, endpoint: &str) -> String {
        // Split repo_id into namespace and repo (e.g., "user/repo" -> "user", "repo")
        let parts: Vec<&str> = self.repo.repo_id.splitn(2, '/').collect();
        let (namespace, repo) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("", self.repo.repo_id.as_str())
        };

        format!(
            "{}/api/{}/{}/{}/preupload/{}",
            endpoint,
            self.repo.repo_type.as_plural_str(),
            namespace,
            repo,
            percent_encode_revision(self.revision()),
        )
    }

    /// Build the commit API URL for this URI.
    pub fn commit_url(&self, endpoint: &str) -> String {
        // Split repo_id into namespace and repo (e.g., "user/repo" -> "user", "repo")
        let parts: Vec<&str> = self.repo.repo_id.splitn(2, '/').collect();
        let (namespace, repo) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            // Handle case where repo_id doesn't contain a slash (shouldn't happen normally)
            ("", self.repo.repo_id.as_str())
        };

        format!(
            "{}/api/{}/{}/{}/commit/{}",
            endpoint,
            self.repo.repo_type.as_plural_str(),
            namespace,
            repo,
            percent_encode_revision(self.revision()),
        )
    }
}

pub(super) fn percent_encode_revision(revision: &str) -> String {
    utf8_percent_encode(revision, NON_ALPHANUMERIC).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(path: &str) -> HfUri {
        HfUri::parse(path).unwrap()
    }

    #[test]
    fn test_repo_type_parse() {
        assert_eq!(RepoType::parse("models").unwrap(), RepoType::Model);
        assert_eq!(RepoType::parse("Models").unwrap(), RepoType::Model);
        assert_eq!(RepoType::parse("MODELS").unwrap(), RepoType::Model);
        assert_eq!(RepoType::parse("datasets").unwrap(), RepoType::Dataset);
        assert_eq!(RepoType::parse("Datasets").unwrap(), RepoType::Dataset);
        assert_eq!(RepoType::parse("spaces").unwrap(), RepoType::Space);
        assert_eq!(RepoType::parse("Spaces").unwrap(), RepoType::Space);
        assert_eq!(RepoType::parse("model").unwrap(), RepoType::Model);
        assert_eq!(RepoType::parse("dataset").unwrap(), RepoType::Dataset);
        assert_eq!(RepoType::parse("space").unwrap(), RepoType::Space);
        assert_eq!(RepoType::parse("data sets").unwrap(), RepoType::Dataset);
        assert_eq!(RepoType::parse("Data Sets").unwrap(), RepoType::Dataset);
        assert!(RepoType::parse("unknown").is_err());
        assert!(RepoType::parse("foobar").is_err());
    }

    #[test]
    fn resolve_with_namespace() {
        let p = resolve("username/my_model");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "username/my_model");
        assert!(p.repo.revision.is_none());
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_with_revision() {
        let p = resolve("username/my_model@dev");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "username/my_model");
        assert_eq!(p.repo.revision.as_deref(), Some("dev"));
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_datasets_prefix() {
        let p = resolve("datasets/username/my_dataset");
        assert_eq!(p.repo.repo_type, RepoType::Dataset);
        assert_eq!(p.repo.repo_id, "username/my_dataset");
        assert!(p.repo.revision.is_none());
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_datasets_prefix_and_revision() {
        let p = resolve("datasets/username/my_dataset@dev");
        assert_eq!(p.repo.repo_type, RepoType::Dataset);
        assert_eq!(p.repo.repo_id, "username/my_dataset");
        assert_eq!(p.repo.revision.as_deref(), Some("dev"));
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_with_path_in_repo() {
        let p = resolve("username/my_model/config.json");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "username/my_model");
        assert!(p.repo.revision.is_none());
        assert_eq!(p.path, "config.json");
    }

    #[test]
    fn resolve_with_revision_and_path() {
        let p = resolve("username/my_model@dev/path/to/file.txt");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "username/my_model");
        assert_eq!(p.repo.revision.as_deref(), Some("dev"));
        assert_eq!(p.path, "path/to/file.txt");
    }

    #[test]
    fn resolve_datasets_revision_and_path() {
        let p = resolve("datasets/username/my_dataset@dev/train/data.csv");
        assert_eq!(p.repo.repo_type, RepoType::Dataset);
        assert_eq!(p.repo.repo_id, "username/my_dataset");
        assert_eq!(p.repo.revision.as_deref(), Some("dev"));
        assert_eq!(p.path, "train/data.csv");
    }

    #[test]
    fn resolve_refs_convert_revision() {
        let p = resolve("datasets/squad@refs/convert/parquet");
        assert_eq!(p.repo.repo_type, RepoType::Dataset);
        assert_eq!(p.repo.repo_id, "squad");
        assert_eq!(p.repo.revision.as_deref(), Some("refs/convert/parquet"));
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_refs_pr_revision() {
        let p = resolve("username/my_model@refs/pr/10");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "username/my_model");
        assert_eq!(p.repo.revision.as_deref(), Some("refs/pr/10"));
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_encoded_revision() {
        let p = resolve("username/my_model@refs%2Fpr%2F10");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "username/my_model");
        assert_eq!(p.repo.revision.as_deref(), Some("refs/pr/10"));
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_at_in_path_not_revision() {
        let p = resolve("username/my_model/path/to/@not-a-revision.txt");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "username/my_model");
        assert!(p.repo.revision.is_none());
        assert_eq!(p.path, "path/to/@not-a-revision.txt");
    }

    #[test]
    fn resolve_bare_repo_type_fails() {
        assert!(HfUri::parse("datasets").is_err());
        assert!(HfUri::parse("").is_err());
    }

    #[test]
    fn resolve_bare_repo_no_namespace() {
        let p = resolve("gpt2");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "gpt2");
        assert!(p.repo.revision.is_none());
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_bare_repo_with_revision() {
        let p = resolve("gpt2@dev");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "gpt2");
        assert_eq!(p.repo.revision.as_deref(), Some("dev"));
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_bare_dataset_no_namespace() {
        let p = resolve("datasets/squad");
        assert_eq!(p.repo.repo_type, RepoType::Dataset);
        assert_eq!(p.repo.repo_id, "squad");
        assert!(p.repo.revision.is_none());
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_bare_dataset_with_revision() {
        let p = resolve("datasets/squad@dev");
        assert_eq!(p.repo.repo_type, RepoType::Dataset);
        assert_eq!(p.repo.repo_id, "squad");
        assert_eq!(p.repo.revision.as_deref(), Some("dev"));
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_models_prefix() {
        let p = resolve("models/username/my_model");
        assert_eq!(p.repo.repo_type, RepoType::Model);
        assert_eq!(p.repo.repo_id, "username/my_model");
        assert!(p.repo.revision.is_none());
        assert_eq!(p.path, "");
    }

    #[test]
    fn resolve_spaces_prefix() {
        let p = resolve("spaces/username/my_space");
        assert_eq!(p.repo.repo_type, RepoType::Space);
        assert_eq!(p.repo.repo_id, "username/my_space");
        assert!(p.repo.revision.is_none());
        assert_eq!(p.path, "");
    }
}
