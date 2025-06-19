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
use std::vec::IntoIter;

use super::core::MokaCore;
use crate::raw::oio;
use crate::raw::*;
use crate::*;

pub struct MokaLister {
    root: String,
    keys: IntoIter<String>,
}

impl MokaLister {
    pub fn new(core: Arc<MokaCore>, root: String, _path: String) -> Self {
        // Get all keys from the cache
        let keys: Vec<String> = core.cache.iter().map(|kv| kv.0.to_string()).collect();

        Self {
            root,
            keys: keys.into_iter(),
        }
    }
}

impl oio::List for MokaLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self.keys.next() {
            Some(key) => {
                // Convert absolute path to relative path
                let mut path = build_rel_path(&self.root, &key);
                if path.is_empty() {
                    path = "/".to_string();
                }

                // Determine if it's a file or directory based on trailing slash
                let mode = if key.ends_with('/') {
                    EntryMode::DIR
                } else {
                    EntryMode::FILE
                };

                Ok(Some(oio::Entry::new(&path, Metadata::new(mode))))
            }
            None => Ok(None),
        }
    }
}
