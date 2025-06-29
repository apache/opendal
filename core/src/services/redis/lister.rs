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

use std::collections::HashSet;
use std::sync::Arc;

use super::core::RedisCore;
use crate::raw::oio;
use crate::raw::*;
use crate::*;

/// Redis lister that uses SCAN cursor for efficient key discovery
///
/// This lister leverages Redis's SCAN command to iterate through keys efficiently
/// without blocking the server. It supports:
/// - Cursor-based pagination for large key sets
/// - Hierarchical directory simulation using "/" delimiter
/// - Configurable scan batch size via OpList::limit()
/// - Both recursive and non-recursive listing modes
pub struct RedisLister {
    core: Arc<RedisCore>,
    root: String,
    path: String,
    recursive: bool,
    /// Limit for SCAN command batch size, taken from OpList::limit()
    limit: Option<usize>,
}

impl RedisLister {
    /// Create a new Redis lister
    ///
    /// # Arguments
    /// * `core` - Redis core instance for executing commands
    /// * `root` - Root path for the accessor
    /// * `path` - Target path to list
    /// * `recursive` - Whether to list recursively
    /// * `limit` - Batch size for SCAN operations (from OpList::limit())
    pub fn new(
        core: Arc<RedisCore>,
        root: String,
        path: String,
        recursive: bool,
        limit: Option<usize>,
    ) -> Self {
        Self {
            core,
            root,
            path,
            recursive,
            limit,
        }
    }

    fn build_scan_pattern(&self) -> String {
        let abs_path = build_abs_path(&self.root, &self.path);
        if abs_path.is_empty() {
            "*".to_string()
        } else {
            // Normalize path for pattern matching and ensure trailing slash is handled consistently
            let normalized_path = abs_path.trim_end_matches('/');
            format!("{}/*", normalized_path)
        }
    }

    fn extract_relative_path(&self, key: &str) -> Option<String> {
        let abs_path = build_abs_path(&self.root, &self.path);

        // For empty path (root listing), all keys are valid
        if abs_path.is_empty() {
            return Some(key.to_string());
        }

        // Normalize the path for consistent prefix matching
        let prefix = abs_path.trim_end_matches('/');
        let prefix_with_slash = format!("{}/", prefix);

        // Check if key starts with our path prefix
        if key.starts_with(&prefix_with_slash) {
            Some(key[prefix_with_slash.len()..].to_string())
        } else {
            None
        }
    }

    fn should_include_key(&self, relative_path: &str) -> bool {
        // For recursive listing, include all keys
        if self.recursive {
            return true;
        }

        // For non-recursive, only include direct children (no slashes)
        !relative_path.contains('/')
    }

    fn extract_directory_from_key(&self, relative_path: &str) -> Option<String> {
        // For recursive listing, we never need to extract directories
        if self.recursive {
            return None;
        }

        // Extract the directory part if there's a slash
        relative_path.find('/').map(|slash_pos| {
            let dir_name = &relative_path[..slash_pos];
            format!("{}/", dir_name)
        })
    }
}

impl oio::PageList for RedisLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        // Parse cursor from token
        let current_cursor = if ctx.token.is_empty() {
            0
        } else {
            ctx.token.parse::<u64>().unwrap_or(0)
        };

        // Reuse pattern for all keys in this batch
        let pattern = self.build_scan_pattern();
        let (next_cursor, keys) = self.core.scan(&pattern, current_cursor, self.limit).await?;

        // Only allocate HashSet if needed (for non-recursive listing)
        let mut directories_in_this_batch = if !self.recursive {
            Some(HashSet::with_capacity(keys.len() / 2)) // Estimate directory count
        } else {
            None
        };

        for key in keys {
            if let Some(relative_path) = self.extract_relative_path(&key) {
                // Handle directory extraction for non-recursive listing
                if let (Some(dir_path), Some(dirs)) = (
                    self.extract_directory_from_key(&relative_path),
                    &mut directories_in_this_batch,
                ) {
                    dirs.insert(dir_path);
                }

                // Handle file entries
                if self.should_include_key(&relative_path) {
                    ctx.entries.push_back(oio::Entry::new(
                        &relative_path,
                        Metadata::new(EntryMode::FILE),
                    ));
                }
            }
        }

        // Add directories to entries (only for non-recursive mode)
        if let Some(dirs) = directories_in_this_batch {
            for dir_path in dirs {
                ctx.entries
                    .push_back(oio::Entry::new(&dir_path, Metadata::new(EntryMode::DIR)));
            }
        }

        // Set next token or mark as done
        if next_cursor == 0 {
            ctx.done = true;
        } else {
            ctx.token = next_cursor.to_string();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::OnceCell;

    fn create_test_core() -> Arc<RedisCore> {
        Arc::new(RedisCore {
            addr: "redis://127.0.0.1:6379".to_string(),
            client: None,
            cluster_client: None,
            conn: OnceCell::new(),
            default_ttl: Some(Duration::from_secs(60)),
        })
    }

    #[test]
    fn test_build_scan_pattern() {
        let core = create_test_core();

        // Test root path
        let lister = RedisLister::new(core.clone(), "/".to_string(), "".to_string(), false, None);
        assert_eq!(lister.build_scan_pattern(), "*");

        // Test subdirectory path
        let lister = RedisLister::new(
            core.clone(),
            "/".to_string(),
            "test/dir".to_string(),
            false,
            None,
        );
        assert_eq!(lister.build_scan_pattern(), "test/dir/*");

        // Test with custom root
        let lister = RedisLister::new(
            core,
            "/myroot/".to_string(),
            "subdir".to_string(),
            true,
            None,
        );
        assert_eq!(lister.build_scan_pattern(), "myroot/subdir/*");
    }

    #[test]
    fn test_extract_relative_path() {
        let core = create_test_core();

        // Test with root path
        let lister = RedisLister::new(core.clone(), "/".to_string(), "".to_string(), false, None);
        assert_eq!(
            lister.extract_relative_path("file.txt"),
            Some("file.txt".to_string())
        );
        assert_eq!(
            lister.extract_relative_path("dir/file.txt"),
            Some("dir/file.txt".to_string())
        );

        // Test with subdirectory
        let lister = RedisLister::new(core, "/".to_string(), "test".to_string(), false, None);
        assert_eq!(
            lister.extract_relative_path("test/file.txt"),
            Some("file.txt".to_string())
        );
        assert_eq!(
            lister.extract_relative_path("test/dir/file.txt"),
            Some("dir/file.txt".to_string())
        );
        assert_eq!(lister.extract_relative_path("other/file.txt"), None);
    }

    #[test]
    fn test_should_include_key() {
        let core = create_test_core();

        // Test recursive mode
        let lister = RedisLister::new(core.clone(), "/".to_string(), "".to_string(), true, None);
        assert!(lister.should_include_key("file.txt"));
        assert!(lister.should_include_key("dir/file.txt"));
        assert!(lister.should_include_key("deep/nested/file.txt"));

        // Test non-recursive mode
        let lister = RedisLister::new(core, "/".to_string(), "".to_string(), false, None);
        assert!(lister.should_include_key("file.txt"));
        assert!(!lister.should_include_key("dir/file.txt"));
        assert!(!lister.should_include_key("deep/nested/file.txt"));
    }

    #[test]
    fn test_extract_directory_from_key() {
        let core = create_test_core();

        // Test recursive mode - should not extract directories
        let lister = RedisLister::new(core.clone(), "/".to_string(), "".to_string(), true, None);
        assert_eq!(lister.extract_directory_from_key("dir/file.txt"), None);

        // Test non-recursive mode - should extract directories
        let lister = RedisLister::new(core, "/".to_string(), "".to_string(), false, None);
        assert_eq!(
            lister.extract_directory_from_key("dir/file.txt"),
            Some("dir/".to_string())
        );
        assert_eq!(lister.extract_directory_from_key("file.txt"), None);
        assert_eq!(
            lister.extract_directory_from_key("deep/nested/file.txt"),
            Some("deep/".to_string())
        );
    }

    #[test]
    fn test_redis_lister_creation() {
        let core = create_test_core();
        let lister = RedisLister::new(
            core,
            "/test/".to_string(),
            "subdir".to_string(),
            false,
            Some(100),
        );

        assert_eq!(lister.root, "/test/");
        assert_eq!(lister.path, "subdir");
        assert!(!lister.recursive);
        assert_eq!(lister.limit, Some(100));
    }
}
