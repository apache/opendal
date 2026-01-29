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
use opendal_core::raw::Access;
use opendal_core::raw::oio;

use crate::FoyerKey;
use crate::FoyerValue;
use crate::Inner;

pub struct Writer<A: Access> {
    pub(crate) w: A::Writer,
    pub(crate) buf: oio::QueueBuf,
    pub(crate) path: String,
    pub(crate) inner: Arc<Inner<A>>,
    pub(crate) size_limit: std::ops::Range<usize>,
    pub(crate) skip_cache: bool,
}

impl<A: Access> Writer<A> {
    pub(crate) fn new(
        w: A::Writer,
        path: String,
        inner: Arc<Inner<A>>,
        size_limit: std::ops::Range<usize>,
    ) -> Self {
        // In chunked mode, skip caching entirely
        let skip_cache = inner.chunk_size.is_some();
        Self {
            w,
            buf: oio::QueueBuf::new(),
            path,
            inner,
            size_limit,
            skip_cache,
        }
    }
}

impl<A: Access> oio::Write for Writer<A> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        // Only buffer if not in chunked mode and within size limit
        if !self.skip_cache && self.size_limit.contains(&(self.buf.len() + bs.len())) {
            self.buf.push(bs.clone());
        } else {
            self.buf.clear();
            self.skip_cache = true;
        }
        self.w.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let buffer = self.buf.clone().collect();
        let metadata = self.w.close().await?;
        // Only cache in full mode (not chunked mode)
        if !self.skip_cache {
            self.inner.cache.insert(
                FoyerKey::Full {
                    path: self.path.clone(),
                    version: metadata.version().map(|v| v.to_string()),
                },
                FoyerValue(buffer),
            );
        }
        Ok(metadata)
    }

    async fn abort(&mut self) -> Result<()> {
        self.buf.clear();
        self.w.abort().await
    }
}
