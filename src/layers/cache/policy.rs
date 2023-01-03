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

use futures::future::BoxFuture;

use crate::raw::*;
use crate::*;

pub type CacheResult<T> = BoxFuture<'static, Result<T>>;

/// CachePolicy allows user to specify the policy while caching.
pub trait CachePolicy: Send + Sync + Debug + 'static {
    /// on_create returns the cache policy on create operation.
    fn on_create(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpCreate,
    ) -> CacheResult<RpCreate> {
        let _ = cache;

        let path = path.to_string();
        Box::pin(async move { inner.create(&path, args).await })
    }

    /// on_read returns the cache policy on read operation.
    fn on_read(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpRead,
    ) -> CacheResult<(RpRead, OutputBytesReader)> {
        let _ = cache;

        let path = path.to_string();
        Box::pin(async move { inner.read(&path, args).await })
    }

    /// on_write returns the cache policy on write operation.
    fn on_write(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpWrite,
        r: BytesReader,
    ) -> CacheResult<RpWrite> {
        let _ = cache;

        let path = path.to_string();
        Box::pin(async move { inner.write(&path, args, r).await })
    }

    /// on_delete returns the cache policy on delete operation.
    fn on_delete(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpDelete,
    ) -> CacheResult<RpDelete> {
        let _ = cache;

        let path = path.to_string();
        Box::pin(async move { inner.delete(&path, args).await })
    }
}

impl<T: CachePolicy> CachePolicy for Arc<T> {
    fn on_create(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpCreate,
    ) -> CacheResult<RpCreate> {
        self.as_ref().on_create(inner, cache, path, args)
    }

    fn on_read(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpRead,
    ) -> CacheResult<(RpRead, OutputBytesReader)> {
        self.as_ref().on_read(inner, cache, path, args)
    }

    fn on_write(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpWrite,
        r: BytesReader,
    ) -> CacheResult<RpWrite> {
        self.as_ref().on_write(inner, cache, path, args, r)
    }

    fn on_delete(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpDelete,
    ) -> CacheResult<RpDelete> {
        self.as_ref().on_delete(inner, cache, path, args)
    }
}

#[derive(Debug)]
pub struct DefaultCachePolicy;

impl CachePolicy for DefaultCachePolicy {}
