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
use std::collections::VecDeque;
use std::sync::Arc;

use http::StatusCode;

use super::core::GdriveCore;
use super::core::GdriveFile;
use super::core::GdriveFileList;
use super::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub struct GdriveLister {
    path: String,
    core: Arc<GdriveCore>,
}

impl GdriveLister {
    pub fn new(path: String, core: Arc<GdriveCore>) -> Self {
        Self { path, core }
    }
}

impl oio::PageList for GdriveLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let file_id = self.core.path_cache.get(&self.path).await?;

        let file_id = match file_id {
            Some(file_id) => file_id,
            None => {
                ctx.done = true;
                return Ok(());
            }
        };

        let is_dir_path = self.path.is_empty() || self.path.ends_with('/');

        // Prefix listing: only return `path` itself if it exists.
        //
        // This matches OpenDAL's list semantics:
        // - `list("file")` returns `file`
        // - `list("dir")` returns `dir/` (but does NOT list its children)
        // - list children requires a trailing slash, like `list("dir/")`
        if !is_dir_path {
            let resp = self.core.gdrive_stat_by_id(&file_id).await?;
            if resp.status() != StatusCode::OK {
                return Err(parse_error(resp));
            }

            let bytes = resp.into_body().to_bytes();
            let file =
                serde_json::from_slice::<GdriveFile>(&bytes).map_err(new_json_deserialize_error)?;
            let is_dir = file.mime_type.as_str() == "application/vnd.google-apps.folder";

            let abs_path = if is_dir {
                format!("{}/", self.path)
            } else {
                self.path.clone()
            };
            let path = build_rel_path(&self.core.root, &abs_path);

            let mut meta = Metadata::new(if is_dir {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            })
            .with_content_type(file.mime_type);
            if let Some(size) = file.size {
                meta = meta.with_content_length(size.parse::<u64>().map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "parse content length").set_source(e)
                })?);
            }
            if let Some(modified_time) = file.modified_time {
                meta =
                    meta.with_last_modified(modified_time.parse::<Timestamp>().map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
                    })?);
            }

            ctx.entries.push_back(oio::Entry::new(&path, meta));
            ctx.done = true;
            return Ok(());
        }

        let resp = self
            .core
            .gdrive_list(file_id.as_str(), 1000, &ctx.token)
            .await?;

        let bytes = match resp.status() {
            StatusCode::OK => resp.into_body().to_bytes(),
            _ => return Err(parse_error(resp)),
        };

        // Google Drive returns an empty response when attempting to list a non-existent directory.
        if bytes.is_empty() {
            ctx.done = true;
            return Ok(());
        }

        // Include the current directory itself when handling the first page of the listing.
        if ctx.token.is_empty() && !ctx.done {
            let path = build_rel_path(&self.core.root, &self.path);
            let e = oio::Entry::new(&path, Metadata::new(EntryMode::DIR));
            ctx.entries.push_back(e);
        }

        let decoded_response =
            serde_json::from_slice::<GdriveFileList>(&bytes).map_err(new_json_deserialize_error)?;

        if let Some(next_page_token) = decoded_response.next_page_token {
            ctx.token = next_page_token;
        } else {
            ctx.done = true;
        }

        for mut file in decoded_response.files {
            let file_type = if file.mime_type.as_str() == "application/vnd.google-apps.folder" {
                if !file.name.ends_with('/') {
                    file.name += "/";
                }
                EntryMode::DIR
            } else {
                EntryMode::FILE
            };

            let root = &self.core.root;
            let path = format!("{}{}", &self.path, file.name);
            let normalized_path = build_rel_path(root, &path);

            let mut metadata = Metadata::new(file_type).with_content_type(file.mime_type.clone());
            if let Some(size) = file.size {
                metadata = metadata.with_content_length(size.parse::<u64>().map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "parse content length").set_source(e)
                })?);
            }
            if let Some(modified_time) = file.modified_time {
                metadata = metadata.with_last_modified(
                    modified_time.parse::<Timestamp>().map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
                    })?,
                );
            }

            let entry = oio::Entry::new(&normalized_path, metadata);
            ctx.entries.push_back(entry);
        }

        Ok(())
    }
}

// ============================================================================
// GdriveFlatLister - Efficient recursive listing implementation
// ============================================================================

// GdriveFlatLister implements efficient recursive listing for Google Drive.
//
// Unlike the generic FlatLister which makes one API call per directory,
// this implementation uses batch queries with OR conditions to list
// multiple directories in a single API call (up to 50 parent IDs per query).
//
// This optimization reduces API calls from O(n) to O(n/50) where n is the
// number of directories, matching rclone's performance.

/// Maximum number of parent IDs to include in a single batch query.
/// Google Drive API has limits on query length, 50 is a safe value.
const BATCH_SIZE: usize = 50;

/// Page size for API requests. Google Drive allows up to 1000.
const PAGE_SIZE: i32 = 1000;

pub struct GdriveFlatLister {
    core: Arc<GdriveCore>,
    root_path: String,
    prefix: Option<String>,

    /// Queue of directory IDs that still need to be listed
    pending_dirs: VecDeque<PendingDir>,

    /// Buffer of entries ready to be returned
    entry_buffer: VecDeque<oio::Entry>,

    /// Current batch of directory IDs being listed
    current_batch: Vec<String>,

    /// Map from directory ID to its absolute path (for reconstructing file paths)
    dir_id_to_path: HashMap<String, String>,

    /// Pagination token for current batch query
    page_token: String,

    /// Whether we've started processing
    started: bool,

    /// Whether listing is complete
    done: bool,
}

/// Represents a directory waiting to be listed
struct PendingDir {
    id: String,
    path: String,
}

impl GdriveFlatLister {
    pub fn new(root_path: String, core: Arc<GdriveCore>) -> Self {
        Self {
            core,
            root_path,
            prefix: None,
            pending_dirs: VecDeque::new(),
            entry_buffer: VecDeque::new(),
            current_batch: Vec::new(),
            dir_id_to_path: HashMap::new(),
            page_token: String::new(),
            started: false,
            done: false,
        }
    }

    /// Initialize the lister by resolving the root directory ID
    async fn initialize(&mut self) -> Result<()> {
        log::debug!(
            "GdriveFlatLister: initializing with root path: {:?}",
            &self.root_path
        );

        let root_id = self.core.path_cache.get(&self.root_path).await?;

        let root_id = match root_id {
            Some(id) => {
                log::debug!("GdriveFlatLister: root path resolved to ID: {:?}", &id);
                id
            }
            None => {
                log::debug!(
                    "GdriveFlatLister: root path not found: {:?}",
                    &self.root_path
                );
                // Directory listing on a non-existent directory should return empty.
                if self.root_path.ends_with('/') {
                    self.done = true;
                    return Ok(());
                }

                // Prefix listing can still return matches even if the exact path doesn't exist.
                let prefix = self.root_path.clone();
                self.prefix = Some(prefix.clone());

                let mut parent_path = get_parent(&prefix).to_string();
                if parent_path == "/" {
                    parent_path.clear();
                }

                let parent_id = self.core.path_cache.get(&parent_path).await?;
                let parent_id = match parent_id {
                    Some(id) => id,
                    None => {
                        self.done = true;
                        return Ok(());
                    }
                };

                self.root_path = parent_path.clone();
                self.pending_dirs.push_back(PendingDir {
                    id: parent_id,
                    path: parent_path,
                });

                self.started = true;
                return Ok(());
            }
        };

        // Resolve the entry type for root path so we can handle:
        //
        // - `list("dir", recursive=true)` where `dir` is a folder but without trailing slash.
        // - `list("prefix", recursive=true)` where `prefix` points to a file or a file prefix.
        let resp = self.core.gdrive_stat_by_id(&root_id).await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bytes = resp.into_body().to_bytes();
        let root_file =
            serde_json::from_slice::<GdriveFile>(&bytes).map_err(new_json_deserialize_error)?;
        let root_is_dir = root_file.mime_type.as_str() == "application/vnd.google-apps.folder";

        if root_is_dir {
            // Directory paths must end with `/` (except the root which is represented by empty path).
            if !self.root_path.is_empty() && !self.root_path.ends_with('/') {
                self.root_path.push('/');
                self.core.path_cache.insert(&self.root_path, &root_id).await;
            }

            // Add the root directory entry first.
            let mut rel_path = build_rel_path(&self.core.root, &self.root_path);
            if !rel_path.is_empty() && !rel_path.ends_with('/') {
                rel_path.push('/');
            }
            let entry = oio::Entry::new(&rel_path, Metadata::new(EntryMode::DIR));
            self.entry_buffer.push_back(entry);

            // Queue the root directory for listing.
            self.pending_dirs.push_back(PendingDir {
                id: root_id.clone(),
                path: self.root_path.clone(),
            });
            self.dir_id_to_path.insert(root_id, self.root_path.clone());
        } else {
            // For file/prefix listing, start from its parent directory and filter results by prefix.
            let prefix = self.root_path.clone();
            self.prefix = Some(prefix.clone());

            let mut parent_path = get_parent(&prefix).to_string();
            if parent_path == "/" {
                parent_path.clear();
            }

            let parent_id = self.core.path_cache.get(&parent_path).await?;
            let parent_id = match parent_id {
                Some(id) => id,
                None => {
                    self.done = true;
                    return Ok(());
                }
            };

            self.root_path = parent_path.clone();
            self.pending_dirs.push_back(PendingDir {
                id: parent_id,
                path: parent_path,
            });
        }

        self.started = true;
        Ok(())
    }

    /// Fill the current batch with pending directory IDs
    fn fill_batch(&mut self) {
        self.current_batch.clear();
        while self.current_batch.len() < BATCH_SIZE {
            if let Some(dir) = self.pending_dirs.pop_front() {
                self.current_batch.push(dir.id.clone());
                self.dir_id_to_path.insert(dir.id, dir.path);
            } else {
                break;
            }
        }
    }

    /// Process a batch of directories using OR query
    async fn process_batch(&mut self) -> Result<()> {
        if self.current_batch.is_empty() {
            self.done = true;
            return Ok(());
        }

        log::debug!(
            "GdriveFlatLister: processing batch of {} directories: {:?}",
            self.current_batch.len(),
            &self.current_batch
        );

        let resp = self
            .core
            .gdrive_list_batch(&self.current_batch, PAGE_SIZE, &self.page_token)
            .await?;

        let bytes = match resp.status() {
            StatusCode::OK => resp.into_body().to_bytes(),
            _ => return Err(parse_error(resp)),
        };

        log::debug!("GdriveFlatLister: response size: {} bytes", bytes.len());

        if bytes.is_empty() {
            self.page_token.clear();
            self.fill_batch();
            return Ok(());
        }

        let decoded_response =
            serde_json::from_slice::<GdriveFileList>(&bytes).map_err(new_json_deserialize_error)?;

        log::debug!(
            "GdriveFlatLister: got {} files, next_page_token: {:?}",
            decoded_response.files.len(),
            decoded_response.next_page_token.is_some()
        );

        // Process files from the response FIRST (this may add new dirs to pending_dirs)
        for file in decoded_response.files {
            self.process_file(file).await?;
        }

        // Update pagination state AFTER processing files
        if let Some(next_page_token) = decoded_response.next_page_token {
            self.page_token = next_page_token;
        } else {
            self.page_token.clear();
            // Current batch is done, prepare next batch with newly discovered directories
            self.fill_batch();
        }

        Ok(())
    }

    /// Process a single file from the API response
    async fn process_file(&mut self, mut file: GdriveFile) -> Result<()> {
        let is_dir = file.mime_type.as_str() == "application/vnd.google-apps.folder";

        if is_dir && !file.name.ends_with('/') {
            file.name.push('/');
        }

        // Find the parent directory path.
        //
        // Google Drive files can have multiple parents. The batch query guarantees that at least
        // one parent is among the directories we are currently listing, but it may not be the
        // first one in the `parents` array. Always pick a parent that we can resolve.
        let parent_path = file
            .parents
            .iter()
            .find_map(|parent_id| self.dir_id_to_path.get(parent_id).cloned())
            .unwrap_or_else(|| self.root_path.clone());

        let abs_path = format!("{}{}", parent_path, file.name);
        let rel_path = build_rel_path(&self.core.root, &abs_path);

        // Build metadata
        let entry_mode = if is_dir {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        };

        let mut metadata = Metadata::new(entry_mode).with_content_type(file.mime_type.clone());

        if let Some(size) = file.size {
            if let Ok(size) = size.parse::<u64>() {
                metadata = metadata.with_content_length(size);
            }
        }

        if let Some(modified_time) = file.modified_time {
            if let Ok(ts) = modified_time.parse::<Timestamp>() {
                metadata = metadata.with_last_modified(ts);
            }
        }

        let matches_prefix = self
            .prefix
            .as_ref()
            .map(|prefix| abs_path.starts_with(prefix))
            .unwrap_or(true);

        if matches_prefix {
            let entry = oio::Entry::new(&rel_path, metadata);
            self.entry_buffer.push_back(entry);
        }

        // If it's a directory, queue it for recursive listing if it can contain matching entries.
        let should_traverse = is_dir
            && self
                .prefix
                .as_ref()
                .map(|prefix| abs_path.starts_with(prefix) || prefix.starts_with(&abs_path))
                .unwrap_or(true);

        if should_traverse {
            self.pending_dirs.push_back(PendingDir {
                id: file.id.clone(),
                path: abs_path.clone(),
            });
            self.dir_id_to_path.insert(file.id, abs_path);
        }

        Ok(())
    }
}

impl oio::List for GdriveFlatLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        // Initialize on first call
        if !self.started {
            self.initialize().await?;
            self.fill_batch();
        }

        loop {
            // Return buffered entries first
            if let Some(entry) = self.entry_buffer.pop_front() {
                return Ok(Some(entry));
            }

            // If we're done, return None
            if self.done {
                return Ok(None);
            }

            // Process more directories
            self.process_batch().await?;

            // Check if we got any entries or if we're done
            if self.entry_buffer.is_empty()
                && self.current_batch.is_empty()
                && self.pending_dirs.is_empty()
            {
                self.done = true;
            }
        }
    }
}
