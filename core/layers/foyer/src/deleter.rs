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

pub(crate) struct Deleter<D> {
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

    async fn clear_cache(&self) -> Result<()> {
        // Foyer only supports removing exact keys or clearing the cache.
        // Downgrade pending `Store` pins/repairs around `clear()` so
        // completing older reads cannot resurrect recursively deleted entries.
        self.inner.deleted_keys.downgrade_stored_pins();
        let res = self.inner.cache.clear().await.map_err(extract_err);
        self.inner.deleted_keys.downgrade_stored_pins();
        res
    }
}

impl<D: oio::Delete> oio::Delete for Deleter<D> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let recursive = args.recursive();
        let key = FoyerKey {
            path: path.to_string(),
            version: args.version().map(|v| v.to_string()),
        };
        // Deleting one version may supersede the unversioned entry for this
        // path while the object itself still exists.
        let evict_key = key.version.is_some().then(|| FoyerKey {
            path: key.path.clone(),
            version: None,
        });

        let delete_res = self.deleter.delete(path, args).await;
        if delete_res.is_ok() {
            self.recursive |= recursive;
            if let Some(ref evict) = evict_key {
                self.evict_keys.push(evict.clone());
            }
            self.keys.push(key.clone());

            // Invalidate immediately for deleters that commit during `delete`
            // (such as `OneShotDeleter` or a `BatchDeleter` flushing mid-batch),
            // while keeping keys tracked so `close` guards buffered deleters.
            self.inner.deleted_keys.invalidate(
                &self.inner.cache,
                std::slice::from_ref(&key),
                evict_key.as_slice(),
            );
            if recursive {
                self.clear_cache().await?;
            }
            return Ok(());
        }

        // Prior deletes or a partial batch/recursive flush triggered by this
        // call may already have mutated the backend before returning an error.
        let had_prior = !self.keys.is_empty() || !self.evict_keys.is_empty();
        if had_prior {
            if let Some(evict) = evict_key {
                self.evict_keys.push(evict);
            }
            self.keys.push(key);
            self.inner
                .deleted_keys
                .invalidate(&self.inner.cache, &self.keys, &self.evict_keys);
        }
        if self.recursive || recursive {
            self.recursive = true;
            let _ = self.clear_cache().await;
        }
        delete_res
    }

    async fn close(&mut self) -> Result<()> {
        let delete_guard =
            self.inner
                .deleted_keys
                .begin_delete(&self.inner.cache, &self.keys, &self.evict_keys);
        let close_res = self.deleter.close().await;
        let clear_res = if self.recursive {
            // Buffered deleters may not remove objects until close, so clear
            // afterwards (including after errors) to remove any entries
            // refilled by concurrent reads.
            self.clear_cache().await
        } else {
            Ok(())
        };
        drop(delete_guard);
        if close_res.is_ok() {
            self.keys.clear();
            self.evict_keys.clear();
            if self.recursive && clear_res.is_ok() {
                self.recursive = false;
            }
        }
        close_res?;
        clear_res?;
        Ok(())
    }
}
