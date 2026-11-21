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

use super::core::*;
use crate::raw::oio;
use crate::raw::*;
use crate::*;

pub struct MemoryDeleter {
    core: Arc<MemoryCore>,
    root: String,
}

impl MemoryDeleter {
    pub fn new(core: Arc<MemoryCore>, root: String) -> Self {
        Self { core, root }
    }
}

impl oio::OneShotDelete for MemoryDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let key = build_absolute_path(&self.root, &path);
        if args.recursive() {
            self.core.delete_prefix(&key)
        } else {
            self.core.delete(&key)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[tokio::test]
    async fn test_delete_non_empty_dir_keeps_entries() -> Result<()> {
        for explicit in [false, true] {
            let op = Operator::new(services::Memory::default().root("/root/"))?;
            let mut expected = vec!["d/a.txt", "d/b.txt"];
            if explicit {
                op.create_dir("d/").await?;
                expected.insert(0, "d/");
            }
            op.write("d/a.txt", "a").await?;
            op.write("d/b.txt", "b").await?;

            let err = op.delete("d/").await.expect_err("directory is non-empty");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "directory not empty");
            let entries = op.list_with("d/").recursive(true).await?;
            let paths: Vec<_> = entries.iter().map(|entry| entry.path()).collect();
            assert_eq!(paths, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_empty_and_missing_dir() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.create_dir("d/").await?;
        op.delete("d/").await?;
        assert!(!op.exists("d/").await?);
        op.delete("d/").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_recursive_removes_markers_and_children() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.create_dir("d/").await?;
        op.create_dir("d/nested/").await?;
        op.write("d/nested/file.txt", "contents").await?;
        op.write("d", "file").await?;
        op.write("dd/file.txt", "sibling").await?;

        op.delete_with("d/").recursive(true).await?;

        assert!(op.list_with("d/").recursive(true).await?.is_empty());
        assert!(op.exists("d").await?);
        assert!(op.exists("dd/file.txt").await?);
        Ok(())
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_remove_all_uses_literal_prefix() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("d", "file").await?;
        op.create_dir("d/").await?;
        op.write("d/a.txt", "child").await?;
        op.write("dd/a.txt", "prefix match").await?;
        op.write("other.txt", "unrelated file").await?;

        op.remove_all("d").await?;

        assert!(op.list_with("d").recursive(true).await?.is_empty());
        assert!(op.exists("other.txt").await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_file_with_same_named_directory() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("d", "file").await?;
        op.write("d/a.txt", "child").await?;

        op.delete("d").await?;
        assert!(!op.exists("d").await?);
        op.delete("d").await?;
        assert!(op.exists("d/a.txt").await?);
        Ok(())
    }
}
