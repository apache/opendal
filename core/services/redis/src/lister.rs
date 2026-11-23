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

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::*;

use super::core::{RedisCore, format_redis_error};

/// Scan keys for a standalone Redis connection.
pub struct RedisLister {
    core: Arc<RedisCore>,
    root: String,
    prefix: String,
    pattern: String,
    count: Option<usize>,
    recursive: bool,
    // Redis-compatible servers can return opaque, non-numeric cursors.
    cursor: Vec<u8>,
    done: bool,
    entries: VecDeque<oio::Entry>,
    seen: HashSet<String>,
}

impl RedisLister {
    pub fn new(core: Arc<RedisCore>, root: &str, path: &str, args: OpList) -> Self {
        let prefix = build_absolute_path(root, path);
        // Some Redis-compatible servers only optimize a literal prefix followed by '*'.
        // Keep the server filter broad and check the full literal prefix locally.
        let pattern = match prefix.find(['*', '?', '[', ']', '\\', '\0']) {
            Some(index) => format!("{}*", &prefix[..index]),
            None => format!("{prefix}*"),
        };
        Self {
            core,
            root: build_absolute_path(root, "/"),
            prefix,
            pattern,
            count: args.limit().map(|limit| limit.clamp(1, i32::MAX as usize)),
            recursive: args.recursive(),
            cursor: b"0".to_vec(),
            done: false,
            entries: VecDeque::new(),
            seen: HashSet::new(),
        }
    }

    async fn next_page(&mut self) -> Result<()> {
        let mut conn = self.core.conn().await?;
        let mut scan = redis::cmd("SCAN");
        scan.arg(&self.cursor[..]).arg("MATCH").arg(&self.pattern);
        if let Some(count) = self.count {
            scan.arg("COUNT").arg(count);
        }
        let (next_cursor, keys): (Vec<u8>, Vec<Vec<u8>>) = scan
            .query_async(&mut *conn)
            .await
            .map_err(format_redis_error)?;

        let mut lengths = redis::pipe();
        let mut paths = Vec::new();
        let relative_prefix_len = self.prefix.len() - self.root.len();
        for key in &keys {
            let Ok(key) = std::str::from_utf8(key) else {
                continue;
            };
            let Some(suffix) = key.strip_prefix(&self.prefix) else {
                continue;
            };
            let path = &key[self.root.len()..];
            // Ignore external keys that OpenDAL cannot address by their exact path.
            if !path.is_empty() && (path == "/" || normalize_path(path) != path) {
                continue;
            }
            let path = if path.is_empty() { "/" } else { path };
            let path = if !self.recursive
                && let Some(slash) = suffix.find('/')
            {
                &path[..relative_prefix_len + slash + 1]
            } else {
                path
            };
            if self.seen.contains(path) {
                continue;
            }
            // Compact metadata uses a 16-bit bitmap and a 16-bit offset for the path.
            if path.len() > u16::MAX as usize - 2 * size_of::<u16>() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Redis key is too long to list",
                ));
            }
            lengths.cmd("STRLEN").arg(key).cmd("TYPE").arg(key);
            paths.push(path);
        }

        if !paths.is_empty() {
            // STRLEN returns zero for a missing key and errors for other data types.
            // TYPE distinguishes empty strings from keys that cannot be read as files.
            let metadata: Vec<(redis::RedisResult<u64>, String)> = lengths
                .ignore_errors()
                .query_async(&mut *conn)
                .await
                .map_err(format_redis_error)?;
            let mut entries = Vec::new();
            for (path, (length, key_type)) in paths.into_iter().zip(metadata) {
                if key_type != "string" {
                    continue;
                }
                let length = length.map_err(format_redis_error)?;
                let metadata = if path.ends_with('/') {
                    MetadataBuilder::dir().build()
                } else {
                    MetadataBuilder::file(length).build()
                };
                entries.push(oio::Entry::new(path, metadata));
            }
            for entry in entries {
                if self.seen.insert(entry.path().to_owned()) {
                    self.entries.push_back(entry);
                }
            }
        }

        self.done = next_cursor == b"0";
        self.cursor = next_cursor;
        Ok(())
    }
}

impl oio::List for RedisLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        while self.entries.is_empty() && !self.done {
            self.next_page().await?;
        }
        Ok(self.entries.pop_front())
    }
}
