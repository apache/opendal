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

use std::sync::Arc;

use log::debug;

use super::GIT_SCHEME;
use super::config::GitConfig;
use super::core::GitCore;
use super::lister::GitLister;
use super::reader::GitReader;
use opendal_core::raw::*;
use opendal_core::*;

/// Git service support with transparent LFS handling.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct GitBuilder {
    pub(super) config: GitConfig,
}

impl GitBuilder {
    /// Set the repository URL (required).
    ///
    /// Examples:
    /// - https://github.com/apache/opendal.git
    /// - https://gitlab.com/user/repo.git
    /// - https://huggingface.co/meta-llama/Llama-2-7b
    pub fn repository(mut self, repository: &str) -> Self {
        if !repository.is_empty() {
            self.config.repository = Some(repository.to_string());
        }
        self
    }

    /// Set the Git reference (branch, tag, or commit hash).
    ///
    /// If not set, will use whatever HEAD points to in the remote repository
    /// (typically "main", "master", "dev", etc.).
    pub fn reference(mut self, reference: &str) -> Self {
        if !reference.is_empty() {
            self.config.reference = Some(reference.to_string());
        }
        self
    }

    /// Set root path within the repository.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }

    /// Set username for authentication.
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_string());
        }
        self
    }

    /// Set password or personal access token for authentication.
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_string());
        }
        self
    }

    /// Set whether to resolve Git LFS pointer files.
    ///
    /// When enabled (default), the service will automatically detect
    /// Git LFS pointer files and stream the actual LFS content.
    ///
    /// Default is true.
    pub fn resolve_lfs(mut self, resolve_lfs: bool) -> Self {
        self.config.resolve_lfs = Some(resolve_lfs);
        self
    }
}

impl Builder for GitBuilder {
    type Config = GitConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let repository = match self.config.repository.clone() {
            Some(v) => v,
            None => {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "repository is required")
                        .with_context("service", GIT_SCHEME),
                );
            }
        };

        // Use "HEAD" as a sentinel value if no reference is provided
        // This will be resolved to the actual default branch when connecting
        let reference = self
            .config
            .reference
            .clone()
            .unwrap_or_else(|| "HEAD".to_string());
        let root = self.config.root.clone().unwrap_or_else(|| "/".to_string());
        let resolve_lfs = self.config.resolve_lfs.unwrap_or(true);

        debug!(
            "backend build finished: repository={}, reference={}, root={}, resolve_lfs={}",
            repository, reference, root, resolve_lfs
        );

        Ok(GitBackend {
            core: Arc::new(GitCore::new(
                repository,
                reference,
                root,
                self.config.username.clone(),
                self.config.password.clone(),
                resolve_lfs,
            )),
        })
    }
}

/// Backend for Git service.
#[derive(Debug, Clone)]
pub struct GitBackend {
    core: Arc<GitCore>,
}

impl Access for GitBackend {
    type Reader = GitReader;
    type Writer = ();
    type Lister = GitLister;
    type Deleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_scheme(GIT_SCHEME)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,
                read: true,
                list: true,
                ..Default::default()
            });

        info.into()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let core = self.core.clone();
        let path = path.to_string();

        let (size, is_dir, last_modified) =
            tokio::task::spawn_blocking(move || core.stat_file_blocking(&path))
                .await
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to join task").set_source(e)
                })??;

        let mut metadata = if is_dir {
            Metadata::new(EntryMode::DIR)
        } else {
            Metadata::new(EntryMode::FILE).with_content_length(size)
        };

        // Set last_modified to commit time
        if let Ok(timestamp) = Timestamp::from_second(last_modified) {
            metadata.set_last_modified(timestamp);
        }

        Ok(RpStat::new(metadata))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let core = self.core.clone();
        let path_str = path.to_string();
        let range = args.range();

        // Read blob from git object database
        let content = tokio::task::spawn_blocking(move || core.read_blob_blocking(&path_str))
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to join task").set_source(e)
            })??;

        let is_lfs = GitCore::is_lfs_pointer(&content);

        let final_content = if self.core.resolve_lfs && is_lfs {
            if let Ok(pointer) = GitCore::parse_lfs_pointer(&content) {
                self.core.fetch_lfs_http(&pointer, range).await?
            } else {
                content
            }
        } else if !range.is_full() {
            let offset = range.offset() as usize;
            if offset >= content.len() {
                return Err(Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "range start offset exceeds content length",
                ));
            }

            let size = range.size().map(|s| s as usize);
            let end = size.map_or(content.len(), |s| (offset + s).min(content.len()));
            content.slice(offset..end)
        } else {
            content
        };

        Ok((RpRead::new(), GitReader::new(final_content)))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let core = self.core.clone();
        let path_str = path.to_string();

        let entries = tokio::task::spawn_blocking(move || core.list_dir_blocking(&path_str))
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to join task").set_source(e)
            })??;

        Ok((RpList::default(), GitLister::new(path.to_string(), entries)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_repository_required() {
        let builder = GitBuilder::default();
        let result = builder.build();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("repository is required")
        );
    }

    #[test]
    fn test_builder_with_repository() {
        let builder = GitBuilder::default().repository("https://github.com/apache/opendal.git");
        let result = builder.build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_builder_defaults() {
        let builder = GitBuilder::default().repository("https://github.com/apache/opendal.git");
        let _backend = builder.build().unwrap();
    }

    #[test]
    fn test_builder_with_all_options() {
        let builder = GitBuilder::default()
            .repository("https://github.com/apache/opendal.git")
            .reference("main")
            .root("/core/src")
            .username("testuser")
            .password("testtoken")
            .resolve_lfs(false);

        let result = builder.build();
        assert!(result.is_ok());
    }
}
