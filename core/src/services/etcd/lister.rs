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

use super::core::EtcdCore;
use crate::raw::oio::Entry;
use crate::raw::{build_abs_path, build_rel_path, oio};
use crate::*;

pub struct EtcdLister {
    root: String,
    path: String,
    iter: IntoIter<String>,
}

impl EtcdLister {
    pub async fn new(core: Arc<EtcdCore>, root: String, path: String) -> Result<Self> {
        let abs_path = build_abs_path(&root, &path);

        // Get all keys with the specified prefix
        let mut client = core.conn().await?;
        let get_options = Some(
            etcd_client::GetOptions::new()
                .with_prefix()
                .with_keys_only(),
        );
        let resp = client
            .get(abs_path.as_str(), get_options)
            .await
            .map_err(super::error::format_etcd_error)?;

        // Collect all keys that match the prefix
        let mut keys = Vec::new();
        for kv in resp.kvs() {
            let key = kv.key_str().map(String::from).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "store key is not valid utf-8 string")
                    .set_source(err)
            })?;
            keys.push(key);
        }

        Ok(Self {
            root,
            path: abs_path,
            iter: keys.into_iter(),
        })
    }
}

impl oio::List for EtcdLister {
    async fn next(&mut self) -> Result<Option<Entry>> {
        for key in self.iter.by_ref() {
            if key.starts_with(&self.path) {
                let path = build_rel_path(&self.root, &key);

                let entry = Entry::new(&path, Metadata::new(EntryMode::from_path(&key)));
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}
