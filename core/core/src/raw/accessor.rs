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
use std::future::Future;
use std::sync::Arc;

use crate::raw::*;
use crate::*;

fn unsupported_error() -> Error {
    Error::new(ErrorKind::Unsupported, "operation is not supported")
}

/// Immutable identity facts for a storage service.
///
/// Runtime resources and composed capabilities are kept outside this value so
/// layers can replace them without mutating shared service identity.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ServiceInfo {
    scheme: &'static str,
    root: Arc<str>,
    name: Arc<str>,
}

impl Debug for ServiceInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceInfo")
            .field("scheme", &self.scheme())
            .field("root", &self.root())
            .field("name", &self.name())
            .finish_non_exhaustive()
    }
}

impl ServiceInfo {
    /// Create a new service info value.
    pub fn new(scheme: &'static str, root: impl AsRef<str>, name: impl AsRef<str>) -> Self {
        Self {
            scheme,
            root: Arc::from(root.as_ref()),
            name: Arc::from(name.as_ref()),
        }
    }

    /// Create a new service info value with only scheme.
    pub fn with_scheme(scheme: &'static str) -> Self {
        Self::new(scheme, "", "")
    }

    /// Return a copy of this service info with a different root.
    pub fn with_root(&self, root: impl AsRef<str>) -> Self {
        Self {
            scheme: self.scheme,
            root: Arc::from(root.as_ref()),
            name: self.name.clone(),
        }
    }

    /// Scheme of backend.
    pub fn scheme(&self) -> &'static str {
        self.scheme
    }

    /// Root of backend, will be in format like `/path/to/dir/`.
    pub fn root(&self) -> Arc<str> {
        self.root.clone()
    }

    /// Name of backend, could be empty if underlying backend doesn't have namespace concept.
    ///
    /// For example:
    ///
    /// - `s3` => bucket name
    /// - `azblob` => container name
    /// - `azdfs` => filesystem name
    /// - `azfile` => share name
    pub fn name(&self) -> Arc<str> {
        self.name.clone()
    }
}

/// Composed resources passed from operator to services and layers.
///
/// Layers that replace the HTTP client or executor must keep forwarding
/// requests or tasks to the previous value. Otherwise, composed features such as
/// retry, timeout, tracing, and metrics can be bypassed.
#[derive(Clone, Debug)]
pub struct OperationContext {
    http_client: HttpClient,
    executor: Executor,
}

impl OperationContext {
    /// Create a new operation context from composed resources.
    pub fn new(http_client: HttpClient, executor: Executor) -> Self {
        Self {
            http_client,
            executor,
        }
    }

    /// Get the composed HTTP client.
    pub fn http_client(&self) -> &HttpClient {
        &self.http_client
    }

    /// Get the composed executor.
    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    /// Split into composed resources.
    pub fn into_parts(self) -> (HttpClient, Executor) {
        (self.http_client, self.executor)
    }

    /// Return a copy of this context with a different HTTP client.
    pub fn with_http_client(&self, http_client: HttpClient) -> Self {
        Self {
            http_client,
            executor: self.executor.clone(),
        }
    }

    /// Return a copy of this context with a different executor.
    pub fn with_executor(&self, executor: Executor) -> Self {
        Self {
            http_client: self.http_client.clone(),
            executor,
        }
    }
}

/// Underlying trait of all storage services.
///
/// Every storage backend supported by OpenDAL implements [`Service`]. Backends
/// only need to implement operations they actually support; the default
/// implementation returns [`ErrorKind::Unsupported`].
///
/// # Operations
///
/// - Paths passed into service operations are normalized by the operator.
///   - `/` means the root path.
///   - Paths ending with `/` are directory paths.
///   - Other paths are file paths.
/// - Services report their supported operation set through [`Service::capability`].
/// - The [`OperationContext`] carries layer-composed runtime resources for each
///   operation.
pub trait Service: Send + Sync + Debug + Unpin + 'static {
    /// Reader returned by `read`.
    type Reader: oio::Read;
    /// Writer returned by `write`.
    type Writer: oio::Write;
    /// Lister returned by `list`.
    type Lister: oio::List;
    /// Deleter returned by `delete`.
    type Deleter: oio::Delete;
    /// Copier returned by `copy`.
    type Copier: oio::Copy;

    /// Return immutable identity facts for this service.
    fn info(&self) -> ServiceInfo;

    /// Return the capability of this service stack.
    ///
    /// Layers may transform capabilities, so callers should use this value for
    /// the current stack instead of assuming the backend's native capability.
    fn capability(&self) -> Capability {
        Capability::default()
    }

    /// Invoke the `create` operation on the specified path.
    ///
    /// Requires [`Capability::create_dir`].
    ///
    /// # Behavior
    ///
    /// - `path` is a normalized directory path.
    /// - Creating an existing directory should succeed.
    fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> impl Future<Output = Result<RpCreateDir>> + MaybeSend {
        let (_, _, _) = (ctx, path, args);
        async { Err(unsupported_error()) }
    }

    /// Invoke the `stat` operation on the specified path.
    ///
    /// Requires [`Capability::stat`].
    ///
    /// # Behavior
    ///
    /// - `/` means the service root.
    /// - A path ending with `/` stats a directory.
    /// - Returned metadata must set `mode` and `content_length`.
    fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> impl Future<Output = Result<RpStat>> + MaybeSend {
        let (_, _, _) = (ctx, path, args);
        async { Err(unsupported_error()) }
    }

    /// Invoke the `read` operation on the specified path.
    ///
    /// Requires [`Capability::read`].
    ///
    /// # Behavior
    ///
    /// - `path` is a normalized file path.
    /// - Range I/O is handled by the returned reader.
    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> impl Future<Output = Result<(RpRead, Self::Reader)>> + MaybeSend {
        let (_, _, _) = (ctx, path, args);
        async { Err(unsupported_error()) }
    }

    /// Invoke the `write` operation on the specified path.
    ///
    /// Requires [`Capability::write`].
    ///
    /// # Behavior
    ///
    /// - `path` is a normalized file path.
    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        let (_, _, _) = (ctx, path, args);
        async { Err(unsupported_error()) }
    }

    /// Invoke the `delete` operation.
    ///
    /// Requires [`Capability::delete`].
    ///
    /// # Behavior
    ///
    /// - The returned deleter handles one or more delete requests.
    /// - Deleting a missing path should succeed.
    fn delete(
        &self,
        ctx: &OperationContext,
    ) -> impl Future<Output = Result<(RpDelete, Self::Deleter)>> + MaybeSend {
        let _ = ctx;
        async { Err(unsupported_error()) }
    }

    /// Invoke the `list` operation on the specified path.
    ///
    /// Requires [`Capability::list`].
    ///
    /// # Behavior
    ///
    /// - `path` is a normalized directory path or prefix.
    /// - Listing a non-existing directory should return an empty stream.
    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> impl Future<Output = Result<(RpList, Self::Lister)>> + MaybeSend {
        let (_, _, _) = (ctx, path, args);
        async { Err(unsupported_error()) }
    }

    /// Invoke the `copy` operation on the specified `from` path and `to` path.
    ///
    /// Requires [`Capability::copy`].
    ///
    /// # Behavior
    ///
    /// - `from` and `to` are normalized file paths.
    /// - Copying to an existing file should overwrite and truncate it.
    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> impl Future<Output = Result<(RpCopy, Self::Copier)>> + MaybeSend {
        let (_, _, _, _, _) = (ctx, from, to, args, opts);
        async { Err(unsupported_error()) }
    }

    /// Invoke the `rename` operation on the specified `from` path and `to` path.
    ///
    /// Requires [`Capability::rename`].
    ///
    /// # Behavior
    ///
    /// - `from` and `to` are normalized file paths.
    fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> impl Future<Output = Result<RpRename>> + MaybeSend {
        let (_, _, _, _) = (ctx, from, to, args);
        async { Err(unsupported_error()) }
    }

    /// Invoke the `presign` operation on the specified path.
    ///
    /// Requires [`Capability::presign`] and the matching presign operation
    /// capability.
    fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> impl Future<Output = Result<RpPresign>> + MaybeSend {
        let (_, _, _) = (ctx, path, args);
        async { Err(unsupported_error()) }
    }
}

/// `ServiceDyn` is the dyn version of [`Service`].
pub trait ServiceDyn: Send + Sync + Debug + Unpin + 'static {
    /// Dyn version of [`Service::info`].
    fn info_dyn(&self) -> ServiceInfo;

    /// Dyn version of [`Service::capability`].
    fn capability_dyn(&self) -> Capability;

    /// Dyn version of [`Service::create_dir`].
    fn create_dir_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpCreateDir,
    ) -> BoxedFuture<'a, Result<RpCreateDir>>;

    /// Dyn version of [`Service::stat`].
    fn stat_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpStat,
    ) -> BoxedFuture<'a, Result<RpStat>>;

    /// Dyn version of [`Service::read`].
    fn read_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpRead,
    ) -> BoxedFuture<'a, Result<(RpRead, oio::Reader)>>;

    /// Dyn version of [`Service::write`].
    fn write_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpWrite,
    ) -> BoxedFuture<'a, Result<(RpWrite, oio::Writer)>>;

    /// Dyn version of [`Service::delete`].
    fn delete_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
    ) -> BoxedFuture<'a, Result<(RpDelete, oio::Deleter)>>;

    /// Dyn version of [`Service::list`].
    fn list_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpList,
    ) -> BoxedFuture<'a, Result<(RpList, oio::Lister)>>;

    /// Dyn version of [`Service::copy`].
    fn copy_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        from: &'a str,
        to: &'a str,
        args: OpCopy,
        opts: OpCopier,
    ) -> BoxedFuture<'a, Result<(RpCopy, oio::Copier)>>;

    /// Dyn version of [`Service::rename`].
    fn rename_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        from: &'a str,
        to: &'a str,
        args: OpRename,
    ) -> BoxedFuture<'a, Result<RpRename>>;

    /// Dyn version of [`Service::presign`].
    fn presign_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpPresign,
    ) -> BoxedFuture<'a, Result<RpPresign>>;
}

/// Type-erased service handle used by layer composition and operators.
pub type Servicer = Arc<dyn ServiceDyn>;

impl<S: Service + ?Sized> ServiceDyn for S {
    fn info_dyn(&self) -> ServiceInfo {
        self.info()
    }

    fn capability_dyn(&self) -> Capability {
        self.capability()
    }

    fn create_dir_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpCreateDir,
    ) -> BoxedFuture<'a, Result<RpCreateDir>> {
        Box::pin(self.create_dir(ctx, path, args))
    }

    fn stat_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpStat,
    ) -> BoxedFuture<'a, Result<RpStat>> {
        Box::pin(self.stat(ctx, path, args))
    }

    fn read_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpRead,
    ) -> BoxedFuture<'a, Result<(RpRead, oio::Reader)>> {
        Box::pin(async move {
            let (rp, reader) = self.read(ctx, path, args).await?;
            Ok((rp, Box::new(reader) as oio::Reader))
        })
    }

    fn write_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpWrite,
    ) -> BoxedFuture<'a, Result<(RpWrite, oio::Writer)>> {
        Box::pin(async move {
            let (rp, writer) = self.write(ctx, path, args).await?;
            Ok((rp, Box::new(writer) as oio::Writer))
        })
    }

    fn delete_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
    ) -> BoxedFuture<'a, Result<(RpDelete, oio::Deleter)>> {
        Box::pin(async move {
            let (rp, deleter) = self.delete(ctx).await?;
            Ok((rp, Box::new(deleter) as oio::Deleter))
        })
    }

    fn list_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpList,
    ) -> BoxedFuture<'a, Result<(RpList, oio::Lister)>> {
        Box::pin(async move {
            let (rp, lister) = self.list(ctx, path, args).await?;
            Ok((rp, Box::new(lister) as oio::Lister))
        })
    }

    fn copy_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        from: &'a str,
        to: &'a str,
        args: OpCopy,
        opts: OpCopier,
    ) -> BoxedFuture<'a, Result<(RpCopy, oio::Copier)>> {
        Box::pin(async move {
            let (rp, copier) = self.copy(ctx, from, to, args, opts).await?;
            Ok((rp, Box::new(copier) as oio::Copier))
        })
    }

    fn rename_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        from: &'a str,
        to: &'a str,
        args: OpRename,
    ) -> BoxedFuture<'a, Result<RpRename>> {
        Box::pin(self.rename(ctx, from, to, args))
    }

    fn presign_dyn<'a>(
        &'a self,
        ctx: &'a OperationContext,
        path: &'a str,
        args: OpPresign,
    ) -> BoxedFuture<'a, Result<RpPresign>> {
        Box::pin(self.presign(ctx, path, args))
    }
}

/// Service is used behind a [`Servicer`] everywhere.
impl<T: ServiceDyn + ?Sized> Service for Arc<T> {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.as_ref().info_dyn()
    }

    fn capability(&self) -> Capability {
        self.as_ref().capability_dyn()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.as_ref().create_dir_dyn(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.as_ref().stat_dyn(ctx, path, args).await
    }

    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, oio::Reader)> {
        self.as_ref().read_dyn(ctx, path, args).await
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, oio::Writer)> {
        self.as_ref().write_dyn(ctx, path, args).await
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, oio::Deleter)> {
        self.as_ref().delete_dyn(ctx).await
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, oio::Lister)> {
        self.as_ref().list_dyn(ctx, path, args).await
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, oio::Copier)> {
        self.as_ref().copy_dyn(ctx, from, to, args, opts).await
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.as_ref().rename_dyn(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.as_ref().presign_dyn(ctx, path, args).await
    }
}

/// Dummy implementation of service.
impl Service for () {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("dummy")
    }

    fn capability(&self) -> Capability {
        Capability::default()
    }
}
