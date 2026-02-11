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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use base64::Engine;

use opendal_core::raw::*;
use opendal_core::*;

// Constants for Git references and LFS
const GIT_HEAD_REF: &str = "HEAD";
const LFS_VERSION_PREFIX: &str = "version https://git-lfs.github.com/spec/v1";
const LFS_CONTENT_TYPE: &str = "application/vnd.git-lfs+json";
const LFS_OID_PREFIX: &str = "oid sha256:";
const LFS_SIZE_PREFIX: &str = "size ";

/// Git LFS pointer information
#[derive(Debug, Clone)]
pub struct LfsPointer {
    pub oid: String,
    pub size: u64,
}

/// Wrapper for gix repository with auto-cleanup temp dir
#[derive(Clone)]
struct RepoHolder {
    repo: gix::Repository,
    _tempdir: Arc<tempfile::TempDir>,
}

/// Cached LFS download URL from batch API
#[derive(Clone, Debug)]
struct LfsDownloadUrl {
    url: String,
}

/// Core functionality for Git operations.
pub struct GitCore {
    pub repository: String,
    pub reference: String,
    pub root: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub resolve_lfs: bool,
    repo_cell: Arc<Mutex<Option<RepoHolder>>>,
    commit_oid_cache: Arc<Mutex<Option<gix::hash::ObjectId>>>,
    commit_info_cache: Arc<Mutex<Option<(gix::hash::ObjectId, i64)>>>,
    lfs_url_cache: Arc<Mutex<HashMap<String, LfsDownloadUrl>>>,
}

impl Debug for GitCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitCore")
            .field("repository", &self.repository)
            .field("reference", &self.reference)
            .field("root", &self.root)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("resolve_lfs", &self.resolve_lfs)
            .finish_non_exhaustive()
    }
}

impl GitCore {
    /// Create a new GitCore instance
    pub fn new(
        repository: String,
        reference: String,
        root: String,
        username: Option<String>,
        password: Option<String>,
        resolve_lfs: bool,
    ) -> Self {
        Self {
            repository,
            reference,
            root,
            username,
            password,
            resolve_lfs,
            repo_cell: Arc::new(Mutex::new(None)),
            commit_oid_cache: Arc::new(Mutex::new(None)),
            commit_info_cache: Arc::new(Mutex::new(None)),
            lfs_url_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_or_create_repo(&self) -> Result<gix::Repository> {
        let mut cell = self
            .repo_cell
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "repository lock poisoned"))?;

        if cell.is_none() {
            let temp_dir = tempfile::tempdir().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to create temp dir").set_source(e)
            })?;

            // Build URL with credentials if provided
            let clone_url =
                if let (Some(username), Some(password)) = (&self.username, &self.password) {
                    // Parse the URL and inject credentials
                    let url = self.repository.as_str();
                    if let Some(scheme_end) = url.find("://") {
                        let (scheme, rest) = url.split_at(scheme_end + 3);
                        format!("{}{}:{}@{}", scheme, username, password, rest)
                    } else {
                        self.repository.clone()
                    }
                } else {
                    self.repository.clone()
                };

            let mut prepare =
                gix::prepare_clone(clone_url.as_str(), temp_dir.path()).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to prepare clone").set_source(e)
                })?;

            // Use shallow clone for HEAD, full clone for specific refs
            if self.reference == GIT_HEAD_REF {
                use std::num::NonZeroU32;
                let depth = NonZeroU32::new(1).expect("1 is non-zero");
                prepare = prepare.with_shallow(gix::remote::fetch::Shallow::DepthAtRemote(depth));
            }

            let (repo, _) = prepare
                .fetch_only(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to fetch repository").set_source(e)
                })?;

            *cell = Some(RepoHolder {
                repo,
                _tempdir: Arc::new(temp_dir),
            });
        }

        Ok(cell
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "repository not initialized"))?
            .repo
            .clone())
    }

    /// Get commit tree and time from OID
    fn get_commit_tree<'a>(
        &self,
        repo: &'a gix::Repository,
        commit_oid: gix::hash::ObjectId,
    ) -> Result<(gix::Tree<'a>, i64)> {
        if let Some((tree_oid, commit_time)) = *self
            .commit_info_cache
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "commit info cache lock poisoned"))?
        {
            let tree = repo
                .find_object(tree_oid)
                .map_err(|e| Error::new(ErrorKind::NotFound, "tree not found").set_source(e))?
                .into_tree();
            return Ok((tree, commit_time));
        }

        let commit = repo
            .find_object(commit_oid)
            .map_err(|e| Error::new(ErrorKind::NotFound, "commit not found").set_source(e))?
            .peel_to_commit()
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to peel to commit").set_source(e)
            })?;

        let commit_time = commit
            .time()
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to get commit time").set_source(e)
            })?
            .seconds;

        let tree = commit
            .tree()
            .map_err(|e| Error::new(ErrorKind::Unexpected, "failed to get tree").set_source(e))?;

        let tree_id = tree.id().detach();
        *self
            .commit_info_cache
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "commit info cache lock poisoned"))? =
            Some((tree_id, commit_time));

        Ok((tree, commit_time))
    }

    /// Resolve reference to commit OID (blocking operation)
    fn resolve_reference(&self, repo: &gix::Repository) -> Result<gix::hash::ObjectId> {
        // Return cached OID if available
        if let Some(oid) = *self
            .commit_oid_cache
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "commit cache lock poisoned"))?
        {
            return Ok(oid);
        }

        let commit_oid = if self.reference == GIT_HEAD_REF {
            let mut head = repo.head().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to get HEAD").set_source(e)
            })?;
            head.peel_to_commit()
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to peel HEAD to commit").set_source(e)
                })?
                .id()
                .into()
        } else {
            // Try as SHA first, then as ref name
            if let Ok(oid) = gix::hash::ObjectId::from_hex(self.reference.as_bytes()) {
                oid
            } else {
                // Try as ref name (branch/tag)
                let reference = repo.find_reference(&self.reference).map_err(|e| {
                    Error::new(
                        ErrorKind::NotFound,
                        format!("reference not found: {}", self.reference),
                    )
                    .set_source(e)
                })?;
                let object = reference.into_fully_peeled_id().map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to peel reference").set_source(e)
                })?;
                object.detach()
            }
        };

        // Cache result for subsequent operations
        *self
            .commit_oid_cache
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "commit cache lock poisoned"))? =
            Some(commit_oid);

        Ok(commit_oid)
    }

    /// Get file metadata (blocking operation) - returns (size, is_dir, last_modified)
    pub fn stat_file_blocking(&self, path: &str) -> Result<(u64, bool, i64)> {
        let repo = self.get_or_create_repo()?;
        let commit_oid = self.resolve_reference(&repo)?;
        let (tree, commit_time) = self.get_commit_tree(&repo, commit_oid)?;

        let path = path.trim_start_matches('/');
        let entry = tree
            .lookup_entry_by_path(path)
            .map_err(|e| Error::new(ErrorKind::Unexpected, "failed to lookup path").set_source(e))?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, format!("path not found: {}", path)))?;

        let is_dir = entry.mode().is_tree();

        if is_dir {
            return Ok((0, true, commit_time));
        }

        let obj = entry
            .object()
            .map_err(|e| Error::new(ErrorKind::Unexpected, "failed to get object").set_source(e))?;
        let blob = obj.into_blob();
        let mut size = blob.data.len() as u64;

        if self.resolve_lfs && Self::is_lfs_pointer(&blob.data) {
            if let Ok(pointer) = Self::parse_lfs_pointer(&blob.data) {
                size = pointer.size;
            }
        }

        Ok((size, false, commit_time))
    }

    /// List directory contents (blocking operation) - returns Vec<(name, is_dir, size, last_modified)>
    pub fn list_dir_blocking(&self, path: &str) -> Result<Vec<(String, bool, u64, i64)>> {
        let repo = self.get_or_create_repo()?;
        let commit_oid = self.resolve_reference(&repo)?;
        let (mut tree, commit_time) = self.get_commit_tree(&repo, commit_oid)?;

        let path = path.trim_start_matches('/').trim_end_matches('/');
        if !path.is_empty() {
            let entry = tree
                .lookup_entry_by_path(path)
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to lookup path").set_source(e)
                })?
                .ok_or_else(|| {
                    Error::new(ErrorKind::NotFound, format!("path not found: {}", path))
                })?;

            if !entry.mode().is_tree() {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    "path is not a directory",
                ));
            }

            let obj = entry.object().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to get object").set_source(e)
            })?;
            tree = obj.into_tree();
        }

        let mut entries = Vec::new();
        for entry in tree.iter() {
            let entry = entry.map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to read entry").set_source(e)
            })?;
            let name = entry.filename().to_string();
            let is_dir = entry.mode().is_tree();

            let size = if is_dir {
                0
            } else {
                let obj = entry.object().map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to get object").set_source(e)
                })?;
                let blob = obj.into_blob();
                let mut size = blob.data.len() as u64;

                if self.resolve_lfs && Self::is_lfs_pointer(&blob.data) {
                    if let Ok(pointer) = Self::parse_lfs_pointer(&blob.data) {
                        size = pointer.size;
                    }
                }

                size
            };

            entries.push((name, is_dir, size, commit_time));
        }

        Ok(entries)
    }

    /// Get blob content (blocking operation) - reads from git objects directly
    pub fn read_blob_blocking(&self, path: &str) -> Result<bytes::Bytes> {
        let repo = self.get_or_create_repo()?;
        let commit_oid = self.resolve_reference(&repo)?;
        let (tree, _commit_time) = self.get_commit_tree(&repo, commit_oid)?;

        let path = path.trim_start_matches('/');
        let entry = tree
            .lookup_entry_by_path(path)
            .map_err(|e| Error::new(ErrorKind::Unexpected, "failed to lookup path").set_source(e))?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, format!("path not found: {}", path)))?;

        if entry.mode().is_tree() {
            return Err(Error::new(ErrorKind::IsADirectory, "path is a directory"));
        }

        let obj = entry
            .object()
            .map_err(|e| Error::new(ErrorKind::Unexpected, "failed to get object").set_source(e))?;
        let blob = obj.into_blob();

        Ok(bytes::Bytes::copy_from_slice(&blob.data))
    }

    /// Check if file content is a Git LFS pointer
    pub fn is_lfs_pointer(content: &[u8]) -> bool {
        if let Ok(text) = std::str::from_utf8(&content[..256.min(content.len())]) {
            text.starts_with(LFS_VERSION_PREFIX)
        } else {
            false
        }
    }

    /// Parse LFS pointer to extract OID and size
    pub fn parse_lfs_pointer(content: &[u8]) -> Result<LfsPointer> {
        let text = std::str::from_utf8(content)
            .map_err(|e| Error::new(ErrorKind::Unexpected, "invalid LFS pointer").set_source(e))?;

        let mut oid = None;
        let mut size = None;

        for line in text.lines() {
            if let Some(oid_str) = line.strip_prefix(LFS_OID_PREFIX) {
                oid = Some(oid_str.trim().to_string());
            } else if let Some(size_str) = line.strip_prefix(LFS_SIZE_PREFIX) {
                size = size_str.trim().parse().ok();
            }
        }

        match (oid, size) {
            (Some(oid), Some(size)) => Ok(LfsPointer { oid, size }),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                "invalid LFS pointer format",
            )),
        }
    }

    /// Get LFS download URL from cache or batch API
    async fn get_lfs_download_url(&self, pointer: &LfsPointer) -> Result<String> {
        // Check cache first
        {
            let cache = self
                .lfs_url_cache
                .lock()
                .map_err(|_| Error::new(ErrorKind::Unexpected, "LFS cache lock poisoned"))?;
            if let Some(cached) = cache.get(&pointer.oid) {
                return Ok(cached.url.clone());
            }
        }

        // Not in cache, call batch API
        let http_client = HttpClient::new()?;
        let base_url = self
            .repository
            .strip_suffix(".git")
            .unwrap_or(&self.repository);
        let batch_url = format!("{}.git/info/lfs/objects/batch", base_url);

        let batch_body = format!(
            r#"{{"operation":"download","transfers":["basic"],"objects":[{{"oid":"{}","size":{}}}]}}"#,
            pointer.oid, pointer.size
        );

        let mut batch_req = http::Request::builder()
            .method(http::Method::POST)
            .uri(&batch_url)
            .header("Content-Type", LFS_CONTENT_TYPE)
            .header("Accept", LFS_CONTENT_TYPE);

        // Add credentials if provided
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            let auth = format!("{}:{}", username, password);
            let encoded = base64::prelude::BASE64_STANDARD.encode(auth.as_bytes());
            batch_req = batch_req.header("Authorization", format!("Basic {}", encoded));
        }

        let batch_req = batch_req.body(Buffer::from(batch_body)).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to build batch request").set_source(e)
        })?;

        let batch_resp = http_client.send(batch_req).await?;

        if !batch_resp.status().is_success() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("LFS batch API failed with status: {}", batch_resp.status()),
            ));
        }

        // Parse batch response to get download URL
        let batch_data = batch_resp.into_body().to_bytes();
        let batch_json: serde_json::Value = serde_json::from_slice(&batch_data).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to parse batch response").set_source(e)
        })?;

        let download_url = batch_json["objects"][0]["actions"]["download"]["href"]
            .as_str()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "no download URL in batch response"))?
            .to_string();

        // Cache the URL for reuse
        {
            let mut cache = self
                .lfs_url_cache
                .lock()
                .map_err(|_| Error::new(ErrorKind::Unexpected, "LFS cache lock poisoned"))?;
            cache.insert(
                pointer.oid.clone(),
                LfsDownloadUrl {
                    url: download_url.clone(),
                },
            );
        }

        Ok(download_url)
    }

    /// Fetch LFS file content via HTTP with range support
    pub async fn fetch_lfs_http(
        &self,
        pointer: &LfsPointer,
        range: BytesRange,
    ) -> Result<bytes::Bytes> {
        let http_client = HttpClient::new()?;

        // Get download URL (from cache or batch API)
        let download_url = self.get_lfs_download_url(pointer).await?;

        // Download from the URL with range support
        let mut download_req = http::Request::builder()
            .method(http::Method::GET)
            .uri(&download_url);

        // Add Range header if specified
        if !range.is_full() {
            download_req = download_req.header("Range", range.to_header());
        }

        // Add credentials if provided
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            let auth = format!("{}:{}", username, password);
            let encoded = base64::prelude::BASE64_STANDARD.encode(auth.as_bytes());
            download_req = download_req.header("Authorization", format!("Basic {}", encoded));
        }

        let download_req = download_req.body(Buffer::new()).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to build download request").set_source(e)
        })?;

        let download_resp = http_client.send(download_req).await?;

        if !download_resp.status().is_success() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "LFS download failed with status: {}",
                    download_resp.status()
                ),
            ));
        }

        Ok(download_resp.into_body().to_bytes())
    }
}
