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

use crate::raw::oio::{Delete, FlatLister, List, PrefixLister};
use crate::raw::*;
use crate::*;
use std::sync::Arc;

/// RecursiveDeleter is designed to recursively delete content from storage
pub struct RecursiveDeleter<A, D> {
    acc: Arc<A>,
    recursive: bool,
    deleter: D,
    max_size: usize,
    cur_size: usize,
}

impl<A: Access, D: Delete> RecursiveDeleter<A, D> {
    /// Create a new recursive deleter.
    pub(crate) fn new(acc: Arc<A>, deleter: D) -> Self {
        let recursive = acc.info().native_capability().delete_with_recursive;
        let max_size = acc.info().native_capability().delete_max_size.unwrap_or(1);
        Self {
            acc,
            recursive,
            deleter,
            max_size,
            cur_size: 0,
        }
    }
    /// Recursively delete a path.
    pub async fn recursive_delete(&mut self, path: &str) -> Result<()> {
        if path.ends_with('/') {
            return self.delete_dir(path).await;
        }

        let parent = get_parent(path);
        let (_, lister) = self.acc.list(parent, OpList::default()).await?;
        let mut lister = PrefixLister::new(lister, path);
        while let Some(entry) = lister.next().await? {
            println!("Deleting {}", entry.path());
            if entry.mode().is_dir() {
                self.delete_dir(entry.path()).await?
            } else {
                self.delete(entry.path(), OpDelete::default()).await?;
            }
        }

        Ok(())
    }

    async fn delete_dir(&mut self, dir: &str) -> Result<()> {
        if self.recursive {
            return self
                .delete(dir, OpDelete::default().with_recursive(true))
                .await;
        }

        let mut lister = FlatLister::new(self.acc.clone(), dir);
        while let Some(entry) = lister.next().await? {
            self.delete(entry.path(), OpDelete::default()).await?
        }

        Ok(())
    }

    /// Delete a path
    pub async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        if self.cur_size >= self.max_size {
            let deleted = self.deleter.flush().await?;
            self.cur_size -= deleted;
        }

        self.deleter.delete(path, args)?;
        self.cur_size += 1;

        Ok(())
    }

    /// Flush the deleter, returns the number of deleted items.
    async fn flush(&mut self) -> Result<usize> {
        let deleted = self.deleter.flush().await?;
        self.cur_size -= deleted;

        Ok(deleted)
    }

    /// Flush all items in the deleter.
    pub async fn flush_all(&mut self) -> Result<()> {
        loop {
            self.flush().await?;
            if self.cur_size == 0 {
                break;
            }
        }

        Ok(())
    }
}
