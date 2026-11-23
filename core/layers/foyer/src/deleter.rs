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

use opendal_core::Result;
use opendal_core::raw::OpDelete;
use opendal_core::raw::oio;

use crate::FoyerKey;
use crate::Inner;
use crate::error::extract_err;

#[doc(hidden)]
pub struct Deleter<D> {
    pub(crate) deleter: D,
    pub(crate) keys: Vec<FoyerKey>,
    /// Unversioned keys evicted by versioned deletes. These are removed from
    /// the cache but not marked deleted, since the object may still exist
    /// under a newer version.
    pub(crate) evict_keys: Vec<FoyerKey>,
    pub(crate) recursive: bool,
    pub(crate) inner: Arc<Inner>,
}

impl<D> Deleter<D> {
    pub(crate) fn new(deleter: D, inner: Arc<Inner>) -> Self {
        Self {
            deleter,
            keys: vec![],
            evict_keys: vec![],
            recursive: false,
            inner,
        }
    }
}

impl<D: oio::Delete> oio::Delete for Deleter<D> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.deleter.delete(path, args.clone()).await?;
        self.recursive |= args.recursive();
        let key = FoyerKey {
            path: path.to_string(),
            version: args.version().map(|v| v.to_string()),
        };
        if key.version.is_some() {
            // Deleting one version may supersede the unversioned entry for
            // this path while the object itself still exists.
            self.evict_keys.push(FoyerKey {
                path: key.path.clone(),
                version: None,
            });
        }
        self.keys.push(key);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        {
            let mut deleted_keys = self
                .inner
                .deleted_keys
                .lock()
                .expect("deleted keys lock poisoned");
            for key in &self.evict_keys {
                self.inner.cache.remove(key);
                deleted_keys.remove(key);
            }
            for key in &self.keys {
                self.inner.cache.remove(key);
                deleted_keys.insert(key.clone());
            }
        }
        let close_res = self.deleter.close().await;
        let clear_res = if self.recursive {
            // Foyer only supports removing exact keys or clearing the cache.
            // Buffered deleters may not remove objects until close, so clear
            // afterwards to remove any entries refilled by concurrent reads.
            // Clear after errors too because close may have partially deleted
            // objects before returning the error.
            self.inner.cache.clear().await.map_err(extract_err)
        } else {
            Ok(())
        };
        if self.recursive && close_res.is_ok() && clear_res.is_ok() {
            self.recursive = false;
        }
        close_res?;
        clear_res?;
        Ok(())
    }
}
