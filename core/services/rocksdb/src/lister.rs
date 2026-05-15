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

use opendal_core::raw::*;
use opendal_core::*;

use super::core::*;

pub struct RocksdbLister {
    root: String,
    iter: IntoIter<(String, u64)>,
}

impl RocksdbLister {
    pub fn new(core: Arc<RocksdbCore>, root: String, path: String) -> Result<Self> {
        let entries = core.list(&path)?;

        Ok(Self {
            root,
            iter: entries.into_iter(),
        })
    }
}

impl oio::List for RocksdbLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if let Some((key, value_len)) = self.iter.next() {
            let path = build_rel_path(&self.root, &key);

            // Determine if it's a file or directory based on trailing slash
            let mode = if key.ends_with('/') {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            };
            let mut metadata = Metadata::new(mode);
            if metadata.mode().is_file() {
                metadata.set_content_length(value_len);
            }
            let entry = oio::Entry::new(&path, metadata);
            return Ok(Some(entry));
        }

        Ok(None)
    }
}
