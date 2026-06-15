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

use std::fmt::Debug;
use std::sync::Arc;

use crate::raw::*;
use crate::*;

/// Layer intercepts an operator's composed runtime resources.
///
/// A layer receives the current stack for each plane and returns the stack that
/// should be used by operators built with this layer. Implementations can wrap
/// the operation service, HTTP fetcher, task executor, or any combination of
/// them.
///
/// Hooks take `&self`, so layers that keep mutable state must use interior
/// mutability. That state must remain `Send` and `Sync` because layers are
/// shared across cloned operators and concurrent operations.
pub trait Layer: Send + Sync + Debug + Unpin + 'static {
    /// Intercept the operation service stack.
    ///
    /// Operation layers should return a service that forwards unchanged
    /// operations to `inner`.
    fn apply_service(&self, srv: Servicer) -> Servicer {
        srv
    }

    /// Intercept the HTTP fetch stack.
    ///
    /// Return `inner` unchanged for layers that do not affect HTTP requests.
    fn apply_http_fetch(&self, srv: Servicer, inner: HttpFetcher) -> HttpFetcher {
        let _ = srv;
        inner
    }

    /// Intercept the task execution stack.
    ///
    /// Return `inner` unchanged for layers that do not affect spawned tasks.
    fn apply_execute(&self, srv: Servicer, inner: Executor) -> Executor {
        let _ = srv;
        inner
    }
}

/// Wrap a service and transform its capability.
pub fn with_capability(
    inner: Servicer,
    apply: impl Fn(Capability) -> Capability + Send + Sync + Unpin + 'static,
) -> Servicer {
    Arc::new(CapabilityService {
        inner,
        apply: Arc::new(apply),
    })
}

/// Wrap a service and expose its native capability.
pub fn with_native_capability<T: Service>(inner: T, capability: Capability) -> impl Service {
    NativeCapabilityService { inner, capability }
}

#[derive(Debug)]
struct NativeCapabilityService<T> {
    inner: T,
    capability: Capability,
}

impl<T: Service> Service for NativeCapabilityService<T> {
    type Reader = T::Reader;
    type Writer = T::Writer;
    type Lister = T::Lister;
    type Deleter = T::Deleter;
    type Copier = T::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(ctx, path, args).await
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(ctx, path, args).await
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete(ctx).await
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        self.inner.list(ctx, path, args).await
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        self.inner.copy(ctx, from, to, args, opts).await
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

struct CapabilityService {
    inner: Servicer,
    apply: Arc<dyn Fn(Capability) -> Capability + Send + Sync>,
}

impl Debug for CapabilityService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapabilityService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Service for CapabilityService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        (self.apply)(self.inner.capability())
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, oio::Reader)> {
        self.inner.read(ctx, path, args).await
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, oio::Writer)> {
        self.inner.write(ctx, path, args).await
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, oio::Deleter)> {
        self.inner.delete(ctx).await
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, oio::Lister)> {
        self.inner.list(ctx, path, args).await
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, oio::Copier)> {
        self.inner.copy(ctx, from, to, args, opts).await
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}
