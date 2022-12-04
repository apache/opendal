// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use super::*;
use crate::raw::*;
use crate::*;

#[derive(Debug, Clone)]
pub struct CacheAccessor {
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    policy: Arc<dyn CachePolicy>,
}

impl CacheAccessor {
    pub fn new(
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        policy: Arc<dyn CachePolicy>,
    ) -> Self {
        CacheAccessor {
            inner,
            cache,
            policy,
        }
    }
}

#[async_trait]
impl Accessor for CacheAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.policy
            .on_create(self.inner.clone(), self.cache.clone(), path, args)
            .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        self.policy
            .on_read(self.inner.clone(), self.cache.clone(), path, args)
            .await
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        self.policy
            .on_write(self.inner.clone(), self.cache.clone(), path, args, r)
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.policy
            .on_delete(self.inner.clone(), self.cache.clone(), path, args)
            .await
    }
}
