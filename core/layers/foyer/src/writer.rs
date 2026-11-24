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

use opendal_core::Buffer;
use opendal_core::Metadata;
use opendal_core::Result;
use opendal_core::raw::oio;

use crate::FoyerKey;
use crate::FoyerValue;
use crate::Inner;

pub(crate) struct Writer<W> {
    pub(crate) w: W,
    pub(crate) buf: oio::QueueBuf,
    pub(crate) path: String,
    pub(crate) inner: Arc<Inner>,
    pub(crate) size_limit: std::ops::Range<usize>,
    /// Whether the write appends to an existing object. Appended chunks never
    /// complete the cached object, so they are not buffered and a successful
    /// close evicts the superseded entry instead of inserting one.
    pub(crate) append: bool,
    /// Once the buffered bytes can no longer match the object's final size,
    /// the writer stops buffering and stays uncacheable until close.
    pub(crate) skip_cache: bool,
}

impl<W> Writer<W> {
    pub(crate) fn new(
        w: W,
        path: String,
        inner: Arc<Inner>,
        size_limit: std::ops::Range<usize>,
        append: bool,
    ) -> Self {
        Self {
            w,
            buf: oio::QueueBuf::new(),
            path,
            inner,
            size_limit,
            append,
            skip_cache: false,
        }
    }

    fn unversioned_key(&self) -> FoyerKey {
        FoyerKey {
            path: self.path.clone(),
            version: None,
        }
    }
}

impl<W: oio::Write> oio::Write for Writer<W> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        if self.append {
            let res = self.w.write(bs).await;
            // Append writers may persist bytes to the backend immediately in
            // `write()` without rolling them back on error, `abort()`, or drop.
            // Evict the unversioned entry right away so readers do not observe
            // stale cached bytes.
            let unversioned = self.unversioned_key();
            self.inner
                .deleted_keys
                .evict(&self.inner.cache, &unversioned);
            return res;
        }

        self.w.write(bs.clone()).await?;
        // Once the buffer can no longer hold the full object (cumulative size
        // reaches the limit's end), `skip_cache` stays set so a later small
        // chunk cannot leave a truncated suffix behind in the cache.
        if !self.skip_cache {
            if self.buf.len().saturating_add(bs.len()) < self.size_limit.end {
                self.buf.push(bs);
            } else {
                self.buf.clear();
                self.skip_cache = true;
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let res = self.w.close().await;
        if self.append {
            // Appended bytes are only a suffix of the object, so the cached
            // entry is superseded while the object remains cacheable. Evict on
            // both success and failure because a buffered append writer may
            // have already committed bytes before returning an error.
            let unversioned = self.unversioned_key();
            self.inner
                .deleted_keys
                .evict(&self.inner.cache, &unversioned);
        }
        let metadata = res?;
        if self.append {
            return Ok(metadata);
        }
        // Keep the buffer intact until close succeeds so a failed or
        // cancelled attempt can be retried without caching an empty object.
        let buffer = self.buf.take().collect();
        let key = FoyerKey {
            path: self.path.clone(),
            version: metadata.version().map(|v| v.to_string()),
        };
        let unversioned = self.unversioned_key();
        let cacheable = !self.skip_cache && self.size_limit.contains(&buffer.len());
        if cacheable {
            // On a versioned backend the write produced a new version, so the
            // unversioned entry still points at the superseded object. Evict it
            // without marking deleted so a later read can refill it with the
            // new content.
            let evict_key = (key != unversioned).then_some(&unversioned);
            self.inner
                .deleted_keys
                .store(&self.inner.cache, key, FoyerValue(buffer), evict_key);
        } else {
            // The stored object is outside the size limit, so any cached entry
            // is superseded and refill attempts cannot succeed: invalidate it.
            self.inner.invalidate_unversioned(self.path.clone());
        }
        Ok(metadata)
    }

    async fn abort(&mut self) -> Result<()> {
        // A non-append aborted write does not change the stored object, so the
        // cached entry stays valid. Append writes already evicted the cache
        // entry when `write()` ran.
        self.buf.clear();
        self.w.abort().await
    }
}
