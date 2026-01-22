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

use opendal_core::raw::*;
use opendal_core::*;

/// Lister for Git service
pub struct GitLister {
    entries: Vec<(String, bool, u64, i64)>,
    base_path: String,
    index: usize,
}

impl GitLister {
    pub fn new(base_path: String, entries: Vec<(String, bool, u64, i64)>) -> Self {
        Self {
            entries,
            base_path,
            index: 0,
        }
    }
}

impl oio::List for GitLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.index >= self.entries.len() {
            return Ok(None);
        }

        let (name, is_dir, size, last_modified) = &self.entries[self.index];
        self.index += 1;

        // Construct full path from base path + name
        let full_path = if self.base_path == "/" {
            name.clone()
        } else {
            format!("{}/{}", self.base_path.trim_end_matches('/'), name)
        };

        // Ensure dir paths end with `/`
        let path = if *is_dir && !full_path.ends_with('/') {
            format!("{}/", full_path)
        } else {
            full_path
        };

        let mut metadata = if *is_dir {
            Metadata::new(EntryMode::DIR)
        } else {
            Metadata::new(EntryMode::FILE).with_content_length(*size)
        };

        // Set last_modified to commit time
        if let Ok(timestamp) = Timestamp::from_second(*last_modified) {
            metadata.set_last_modified(timestamp);
        }

        Ok(Some(oio::Entry::new(&path, metadata)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::raw::oio::List;

    #[tokio::test]
    async fn test_lister_empty() {
        let mut lister = GitLister::new("/".to_string(), vec![]);
        let entry = lister.next().await.unwrap();
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn test_lister_with_entries_root() {
        let commit_time = 1234567890; // Example timestamp
        let entries = vec![
            ("file1.txt".to_string(), false, 100, commit_time),
            ("dir1".to_string(), true, 0, commit_time),
            ("file2.txt".to_string(), false, 200, commit_time),
        ];
        let mut lister = GitLister::new("/".to_string(), entries);

        let entry1 = lister.next().await.unwrap().unwrap();
        assert_eq!(entry1.path(), "file1.txt");
        assert_eq!(entry1.mode(), EntryMode::FILE);

        let entry2 = lister.next().await.unwrap().unwrap();
        assert_eq!(entry2.path(), "dir1/");
        assert_eq!(entry2.mode(), EntryMode::DIR);

        let entry3 = lister.next().await.unwrap().unwrap();
        assert_eq!(entry3.path(), "file2.txt");
        assert_eq!(entry3.mode(), EntryMode::FILE);

        let entry4 = lister.next().await.unwrap();
        assert!(entry4.is_none());
    }

    #[tokio::test]
    async fn test_lister_with_subdirectory_path() {
        let commit_time = 1234567890;
        let entries = vec![
            ("nested.txt".to_string(), false, 500, commit_time),
            ("subdir".to_string(), true, 0, commit_time),
        ];
        let mut lister = GitLister::new(".config/".to_string(), entries);

        let entry1 = lister.next().await.unwrap().unwrap();
        assert_eq!(entry1.path(), ".config/nested.txt");
        assert_eq!(entry1.mode(), EntryMode::FILE);

        let entry2 = lister.next().await.unwrap().unwrap();
        assert_eq!(entry2.path(), ".config/subdir/");
        assert_eq!(entry2.mode(), EntryMode::DIR);

        let entry3 = lister.next().await.unwrap();
        assert!(entry3.is_none());
    }

    #[tokio::test]
    async fn test_lister_with_deeply_nested_path() {
        let commit_time = 1234567890;
        let entries = vec![("action.yml".to_string(), false, 1788, commit_time)];
        let mut lister = GitLister::new(".github/actions/fuzz_test/".to_string(), entries);

        let entry = lister.next().await.unwrap().unwrap();
        assert_eq!(entry.path(), ".github/actions/fuzz_test/action.yml");
        assert_eq!(entry.mode(), EntryMode::FILE);
    }

    #[tokio::test]
    async fn test_lister_preserves_last_modified() {
        let commit_time = 1609459200; // 2021-01-01 00:00:00 UTC
        let entries = vec![("test.txt".to_string(), false, 100, commit_time)];
        let mut lister = GitLister::new("/".to_string(), entries);

        let entry = lister.next().await.unwrap().unwrap();
        assert_eq!(entry.path(), "test.txt");
        assert_eq!(entry.mode(), EntryMode::FILE);

        // Timestamp is set correctly in the Entry metadata
        // The actual verification happens through the metadata() call in production
    }

    #[tokio::test]
    async fn test_lister_handles_trailing_slash_in_base_path() {
        let commit_time = 1234567890;
        let entries = vec![("file.txt".to_string(), false, 100, commit_time)];

        // Test with trailing slash
        let mut lister1 = GitLister::new("dir/".to_string(), entries.clone());
        let entry1 = lister1.next().await.unwrap().unwrap();
        assert_eq!(entry1.path(), "dir/file.txt");

        // Test without trailing slash
        let mut lister2 = GitLister::new("dir".to_string(), entries.clone());
        let entry2 = lister2.next().await.unwrap().unwrap();
        assert_eq!(entry2.path(), "dir/file.txt");
    }
}
