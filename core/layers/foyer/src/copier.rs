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

use opendal_core::Metadata;
use opendal_core::Result;
use opendal_core::raw::oio;

use crate::Inner;

/// A copier that invalidates the destination's cached data once the copy completes.
pub(crate) struct FoyerCopier<C> {
    copier: C,
    destination: Option<String>,
    inner: Arc<Inner>,
}

impl<C> FoyerCopier<C> {
    pub(crate) fn new(copier: C, destination: String, inner: Arc<Inner>) -> Self {
        Self {
            copier,
            destination: Some(destination),
            inner,
        }
    }

    fn invalidate_destination(&mut self) {
        // The caller may close after observing completion or poll again.
        // Invalidate only once so a later write can repopulate the cache.
        if let Some(destination) = self.destination.take() {
            self.inner.invalidate_unversioned(destination);
        }
    }
}

impl<C: oio::Copy> oio::Copy for FoyerCopier<C> {
    async fn next(&mut self) -> Result<Option<usize>> {
        let progress = self.copier.next().await?;
        // A Copier consumed as a Stream may never call close.
        if progress.is_none() {
            self.invalidate_destination();
        }
        Ok(progress)
    }

    async fn close(&mut self) -> Result<Metadata> {
        let metadata = self.copier.close().await?;
        self.invalidate_destination();
        Ok(metadata)
    }

    async fn abort(&mut self) -> Result<()> {
        self.copier.abort().await?;
        self.destination = None;
        Ok(())
    }
}
