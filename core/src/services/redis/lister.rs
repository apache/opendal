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
            let prefix = abs_path.trim_end_matches('/');
            format!("{}/*", prefix)
        }
    }

    fn extract_relative_path(&self, key: &str) -> Option<String> {
        let abs_path = build_abs_path(&self.root, &self.path);
        let prefix = if abs_path.is_empty() {
            ""
        } else {
            abs_path.trim_end_matches('/')
        };

        if prefix.is_empty() {
            Some(key.to_string())
        } else if key.starts_with(&format!("{}/", prefix)) {
            Some(key[prefix.len() + 1..].to_string())
        } else {
            None
        }
    }

    fn should_include_key(&self, relative_path: &str) -> bool {
        if self.recursive {
            true
        } else {
            // For non-recursive, only include direct children
            !relative_path.contains('/')
        }
    }

    fn extract_directory_from_key(&self, relative_path: &str) -> Option<String> {
        if self.recursive {
            return None; // Don't emit directories in recursive mode
        }

        if let Some(slash_pos) = relative_path.find('/') {
            let dir_name = &relative_path[..slash_pos];
            Some(format!("{}/", dir_name))
        } else {
            None
        }
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

        let pattern = self.build_scan_pattern();
        let (next_cursor, keys) = self.core.scan(&pattern, current_cursor, self.limit).await?;

        let mut directories_in_this_batch = HashSet::new();

        for key in keys {
            if let Some(relative_path) = self.extract_relative_path(&key) {
                // Handle directory extraction for non-recursive listing
                if let Some(dir_path) = self.extract_directory_from_key(&relative_path) {
                    directories_in_this_batch.insert(dir_path);
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

        // Add directories to entries
        for dir_path in directories_in_this_batch {
            ctx.entries
                .push_back(oio::Entry::new(&dir_path, Metadata::new(EntryMode::DIR)));
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
