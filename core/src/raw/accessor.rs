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
use std::future::ready;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem;
use std::sync::Arc;

use futures::Future;

use crate::raw::*;
use crate::*;

/// Underlying trait of all backends for implementers.
///
/// The actual data access of storage service happens in Accessor layer.
/// Every storage supported by OpenDAL must implement [`Access`] but not all
/// methods of [`Access`] will be implemented according to how the storage service is.
///
/// For example, user can not modify the content from one HTTP file server directly.
/// So [`Http`][crate::services::Http] implements and provides only read related actions.
///
/// [`Access`] gives default implementation for all methods which will raise [`ErrorKind::Unsupported`] error.
/// And what action this [`Access`] supports will be pointed out in [`AccessorInfo`].
///
/// # Note
///
/// Visit [`internals`][crate::docs::internals] for more tutorials.
///
/// # Operations
///
/// - Path in args will all be normalized into the same style, services
///   should handle them based on services' requirement.
///   - Path that ends with `/` means it's Dir, otherwise, it's File.
///   - Root dir is `/`
///   - Path will never be empty.
/// - Operations without capability requirement like `metadata`, `create` are
///   basic operations.
///   - All services must implement them.
///   - Use `unimplemented!()` if not implemented or can't implement.
/// - Operations with capability requirement like `presign` are optional operations.
///   - Services can implement them based on services capabilities.
///   - The default implementation should return [`ErrorKind::Unsupported`].
pub trait Access: Send + Sync + Debug + Unpin + 'static {
    /// Reader is the associated reader returned in `read` operation.
    type Reader: oio::Read;
    /// Writer is the associated writer returned in `write` operation.
    type Writer: oio::Write;
    /// Lister is the associated lister returned in `list` operation.
    type Lister: oio::List;
    /// Deleter is the associated deleter returned in `delete` operation.
    type Deleter: oio::Delete;

    /// BlockingReader is the associated reader returned `blocking_read` operation.
    type BlockingReader: oio::BlockingRead;
    /// BlockingWriter is the associated writer returned `blocking_write` operation.
    type BlockingWriter: oio::BlockingWrite;
    /// BlockingLister is the associated lister returned `blocking_list` operation.
    type BlockingLister: oio::BlockingList;
    /// BlockingDeleter is the associated deleter returned `blocking_delete` operation.
    type BlockingDeleter: oio::BlockingDelete;

    /// Invoke the `info` operation to get metadata of accessor.
    ///
    /// # Notes
    ///
    /// This function is required to be implemented.
    ///
    /// By returning AccessorInfo, underlying services can declare
    /// some useful information about itself.
    ///
    /// - scheme: declare the scheme of backend.
    /// - capabilities: declare the capabilities of current backend.
    fn info(&self) -> Arc<AccessorInfo>;

    /// Invoke the `create` operation on the specified path
    ///
    /// Require [`Capability::create_dir`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST match with EntryMode, DON'T NEED to check mode.
    /// - Create on existing dir SHOULD succeed.
    fn create_dir(
        &self,
        path: &str,
        args: OpCreateDir,
    ) -> impl Future<Output = Result<RpCreateDir>> + MaybeSend {
        let (_, _) = (path, args);

        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `stat` operation on the specified path.
    ///
    /// Require [`Capability::stat`]
    ///
    /// # Behavior
    ///
    /// - `stat` empty path means stat backend's root path.
    /// - `stat` a path endswith "/" means stating a dir.
    /// - `mode` and `content_length` must be set.
    fn stat(&self, path: &str, args: OpStat) -> impl Future<Output = Result<RpStat>> + MaybeSend {
        let (_, _) = (path, args);

        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `read` operation on the specified path, returns a
    /// [`Reader`][crate::Reader] if operate successful.
    ///
    /// Require [`Capability::read`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check mode.
    /// - The returning content length may be smaller than the range specified.
    fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> impl Future<Output = Result<(RpRead, Self::Reader)>> + MaybeSend {
        let (_, _) = (path, args);

        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `write` operation on the specified path, returns a
    /// written size if operate successful.
    ///
    /// Require [`Capability::write`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check mode.
    fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        let (_, _) = (path, args);

        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `delete` operation on the specified path.
    ///
    /// Require [`Capability::delete`]
    ///
    /// # Behavior
    ///
    /// - `delete` is an idempotent operation, it's safe to call `Delete` on the same path multiple times.
    /// - `delete` SHOULD return `Ok(())` if the path is deleted successfully or not exist.
    fn delete(&self) -> impl Future<Output = Result<(RpDelete, Self::Deleter)>> + MaybeSend {
        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `list` operation on the specified path.
    ///
    /// Require [`Capability::list`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST be dir path, DON'T NEED to check mode.
    /// - List non-exist dir should return Empty.
    fn list(
        &self,
        path: &str,
        args: OpList,
    ) -> impl Future<Output = Result<(RpList, Self::Lister)>> + MaybeSend {
        let (_, _) = (path, args);

        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `copy` operation on the specified `from` path and `to` path.
    ///
    /// Require [Capability::copy]
    ///
    /// # Behaviour
    ///
    /// - `from` and `to` MUST be file path, DON'T NEED to check mode.
    /// - Copy on existing file SHOULD succeed.
    /// - Copy on existing file SHOULD overwrite and truncate.
    fn copy(
        &self,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> impl Future<Output = Result<RpCopy>> + MaybeSend {
        let (_, _, _) = (from, to, args);

        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `rename` operation on the specified `from` path and `to` path.
    ///
    /// Require [Capability::rename]
    fn rename(
        &self,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> impl Future<Output = Result<RpRename>> + MaybeSend {
        let (_, _, _) = (from, to, args);

        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `presign` operation on the specified path.
    ///
    /// Require [`Capability::presign`]
    ///
    /// # Behavior
    ///
    /// - This API is optional, return [`std::io::ErrorKind::Unsupported`] if not supported.
    fn presign(
        &self,
        path: &str,
        args: OpPresign,
    ) -> impl Future<Output = Result<RpPresign>> + MaybeSend {
        let (_, _) = (path, args);

        ready(Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        )))
    }

    /// Invoke the `blocking_create` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::create_dir`]
    ///
    /// Require [`Capability::create_dir`] and [`Capability::blocking`]
    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_stat` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::stat`]
    ///
    /// Require [`Capability::stat`] and [`Capability::blocking`]
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_read` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::read`]
    ///
    /// Require [`Capability::read`] and [`Capability::blocking`]
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_write` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::write`]
    ///
    /// Require [`Capability::write`] and [`Capability::blocking`]
    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_delete` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::delete`]
    ///
    /// Require [`Capability::write`] and [`Capability::blocking`]
    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_list` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::list`]
    ///
    /// Require [`Capability::list`] and [`Capability::blocking`]
    ///
    /// # Behavior
    ///
    /// - List non-exist dir should return Empty.
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_copy` operation on the specified `from` path and `to` path.
    ///
    /// This operation is the blocking version of [`Accessor::copy`]
    ///
    /// Require [`Capability::copy`] and [`Capability::blocking`]
    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let (_, _, _) = (from, to, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_rename` operation on the specified `from` path and `to` path.
    ///
    /// This operation is the blocking version of [`Accessor::rename`]
    ///
    /// Require [`Capability::rename`] and [`Capability::blocking`]
    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let (_, _, _) = (from, to, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

/// `AccessDyn` is the dyn version of [`Access`] make it possible to use as
/// `Box<dyn AccessDyn>`.
pub trait AccessDyn: Send + Sync + Debug + Unpin {
    /// Dyn version of [`Accessor::info`]
    fn info_dyn(&self) -> Arc<AccessorInfo>;
    /// Dyn version of [`Accessor::create_dir`]
    fn create_dir_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpCreateDir,
    ) -> BoxedFuture<'a, Result<RpCreateDir>>;
    /// Dyn version of [`Accessor::stat`]
    fn stat_dyn<'a>(&'a self, path: &'a str, args: OpStat) -> BoxedFuture<'a, Result<RpStat>>;
    /// Dyn version of [`Accessor::read`]
    fn read_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpRead,
    ) -> BoxedFuture<'a, Result<(RpRead, oio::Reader)>>;
    /// Dyn version of [`Accessor::write`]
    fn write_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpWrite,
    ) -> BoxedFuture<'a, Result<(RpWrite, oio::Writer)>>;
    /// Dyn version of [`Accessor::delete`]
    fn delete_dyn(&self) -> BoxedFuture<Result<(RpDelete, oio::Deleter)>>;
    /// Dyn version of [`Accessor::list`]
    fn list_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpList,
    ) -> BoxedFuture<'a, Result<(RpList, oio::Lister)>>;
    /// Dyn version of [`Accessor::copy`]
    fn copy_dyn<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        args: OpCopy,
    ) -> BoxedFuture<'a, Result<RpCopy>>;
    /// Dyn version of [`Accessor::rename`]
    fn rename_dyn<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        args: OpRename,
    ) -> BoxedFuture<'a, Result<RpRename>>;
    /// Dyn version of [`Accessor::presign`]
    fn presign_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpPresign,
    ) -> BoxedFuture<'a, Result<RpPresign>>;
    /// Dyn version of [`Accessor::blocking_create_dir`]
    fn blocking_create_dir_dyn(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir>;
    /// Dyn version of [`Accessor::blocking_stat`]
    fn blocking_stat_dyn(&self, path: &str, args: OpStat) -> Result<RpStat>;
    /// Dyn version of [`Accessor::blocking_read`]
    fn blocking_read_dyn(&self, path: &str, args: OpRead) -> Result<(RpRead, oio::BlockingReader)>;
    /// Dyn version of [`Accessor::blocking_write`]
    fn blocking_write_dyn(
        &self,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, oio::BlockingWriter)>;
    /// Dyn version of [`Accessor::blocking_delete`]
    fn blocking_delete_dyn(&self) -> Result<(RpDelete, oio::BlockingDeleter)>;
    /// Dyn version of [`Accessor::blocking_list`]
    fn blocking_list_dyn(&self, path: &str, args: OpList) -> Result<(RpList, oio::BlockingLister)>;
    /// Dyn version of [`Accessor::blocking_copy`]
    fn blocking_copy_dyn(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy>;
    /// Dyn version of [`Accessor::blocking_rename`]
    fn blocking_rename_dyn(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename>;
}

impl<A: ?Sized> AccessDyn for A
where
    A: Access<
        Reader = oio::Reader,
        BlockingReader = oio::BlockingReader,
        Writer = oio::Writer,
        BlockingWriter = oio::BlockingWriter,
        Lister = oio::Lister,
        BlockingLister = oio::BlockingLister,
        Deleter = oio::Deleter,
        BlockingDeleter = oio::BlockingDeleter,
    >,
{
    fn info_dyn(&self) -> Arc<AccessorInfo> {
        self.info()
    }

    fn create_dir_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpCreateDir,
    ) -> BoxedFuture<'a, Result<RpCreateDir>> {
        Box::pin(self.create_dir(path, args))
    }

    fn stat_dyn<'a>(&'a self, path: &'a str, args: OpStat) -> BoxedFuture<'a, Result<RpStat>> {
        Box::pin(self.stat(path, args))
    }

    fn read_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpRead,
    ) -> BoxedFuture<'a, Result<(RpRead, oio::Reader)>> {
        Box::pin(self.read(path, args))
    }

    fn write_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpWrite,
    ) -> BoxedFuture<'a, Result<(RpWrite, oio::Writer)>> {
        Box::pin(self.write(path, args))
    }

    fn delete_dyn(&self) -> BoxedFuture<Result<(RpDelete, oio::Deleter)>> {
        Box::pin(self.delete())
    }

    fn list_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpList,
    ) -> BoxedFuture<'a, Result<(RpList, oio::Lister)>> {
        Box::pin(self.list(path, args))
    }

    fn copy_dyn<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        args: OpCopy,
    ) -> BoxedFuture<'a, Result<RpCopy>> {
        Box::pin(self.copy(from, to, args))
    }

    fn rename_dyn<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        args: OpRename,
    ) -> BoxedFuture<'a, Result<RpRename>> {
        Box::pin(self.rename(from, to, args))
    }

    fn presign_dyn<'a>(
        &'a self,
        path: &'a str,
        args: OpPresign,
    ) -> BoxedFuture<'a, Result<RpPresign>> {
        Box::pin(self.presign(path, args))
    }

    fn blocking_create_dir_dyn(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.blocking_create_dir(path, args)
    }

    fn blocking_stat_dyn(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.blocking_stat(path, args)
    }

    fn blocking_read_dyn(&self, path: &str, args: OpRead) -> Result<(RpRead, oio::BlockingReader)> {
        self.blocking_read(path, args)
    }

    fn blocking_write_dyn(
        &self,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, oio::BlockingWriter)> {
        self.blocking_write(path, args)
    }

    fn blocking_delete_dyn(&self) -> Result<(RpDelete, oio::BlockingDeleter)> {
        self.blocking_delete()
    }

    fn blocking_list_dyn(&self, path: &str, args: OpList) -> Result<(RpList, oio::BlockingLister)> {
        self.blocking_list(path, args)
    }

    fn blocking_copy_dyn(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.blocking_copy(from, to, args)
    }

    fn blocking_rename_dyn(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.blocking_rename(from, to, args)
    }
}

impl Access for dyn AccessDyn {
    type Reader = oio::Reader;
    type BlockingReader = oio::BlockingReader;
    type Writer = oio::Writer;
    type Deleter = oio::Deleter;
    type BlockingWriter = oio::BlockingWriter;
    type Lister = oio::Lister;
    type BlockingLister = oio::BlockingLister;
    type BlockingDeleter = oio::BlockingDeleter;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info_dyn()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.create_dir_dyn(path, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.stat_dyn(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.read_dyn(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.write_dyn(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.delete_dyn().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.list_dyn(path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.copy_dyn(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.rename_dyn(from, to, args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.presign_dyn(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.blocking_create_dir_dyn(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.blocking_read_dyn(path, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.blocking_stat_dyn(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.blocking_write_dyn(path, args)
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.blocking_delete_dyn()
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.blocking_list_dyn(path, args)
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.blocking_copy_dyn(from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.blocking_rename_dyn(from, to, args)
    }
}

/// Dummy implementation of accessor.
impl Access for () {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let ai = AccessorInfo::default();
        ai.set_scheme(Scheme::Custom("dummy"))
            .set_root("")
            .set_name("dummy")
            .set_native_capability(Capability::default());
        ai.into()
    }
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<impl Access>`.
// If we use async fn directly, some weird higher rank trait bound error (`Send`/`Accessor` impl not general enough) will happen.
// Probably related to https://github.com/rust-lang/rust/issues/96865
#[allow(clippy::manual_async_fn)]
impl<T: Access + ?Sized> Access for Arc<T> {
    type Reader = T::Reader;
    type Writer = T::Writer;
    type Lister = T::Lister;
    type Deleter = T::Deleter;
    type BlockingReader = T::BlockingReader;
    type BlockingWriter = T::BlockingWriter;
    type BlockingLister = T::BlockingLister;
    type BlockingDeleter = T::BlockingDeleter;

    fn info(&self) -> Arc<AccessorInfo> {
        self.as_ref().info()
    }

    fn create_dir(
        &self,
        path: &str,
        args: OpCreateDir,
    ) -> impl Future<Output = Result<RpCreateDir>> + MaybeSend {
        async move { self.as_ref().create_dir(path, args).await }
    }

    fn stat(&self, path: &str, args: OpStat) -> impl Future<Output = Result<RpStat>> + MaybeSend {
        async move { self.as_ref().stat(path, args).await }
    }

    fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> impl Future<Output = Result<(RpRead, Self::Reader)>> + MaybeSend {
        async move { self.as_ref().read(path, args).await }
    }

    fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        async move { self.as_ref().write(path, args).await }
    }

    fn delete(&self) -> impl Future<Output = Result<(RpDelete, Self::Deleter)>> + MaybeSend {
        async move { self.as_ref().delete().await }
    }

    fn list(
        &self,
        path: &str,
        args: OpList,
    ) -> impl Future<Output = Result<(RpList, Self::Lister)>> + MaybeSend {
        async move { self.as_ref().list(path, args).await }
    }

    fn copy(
        &self,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> impl Future<Output = Result<RpCopy>> + MaybeSend {
        async move { self.as_ref().copy(from, to, args).await }
    }

    fn rename(
        &self,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> impl Future<Output = Result<RpRename>> + MaybeSend {
        async move { self.as_ref().rename(from, to, args).await }
    }

    fn presign(
        &self,
        path: &str,
        args: OpPresign,
    ) -> impl Future<Output = Result<RpPresign>> + MaybeSend {
        async move { self.as_ref().presign(path, args).await }
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.as_ref().blocking_create_dir(path, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.as_ref().blocking_stat(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.as_ref().blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.as_ref().blocking_write(path, args)
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.as_ref().blocking_delete()
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.as_ref().blocking_list(path, args)
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.as_ref().blocking_copy(from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.as_ref().blocking_rename(from, to, args)
    }
}

/// Accessor is the type erased accessor with `Arc<dyn Accessor>`.
pub type Accessor = Arc<dyn AccessDyn>;

#[derive(Debug, Default)]
struct AccessorInfoInner {
    scheme: Scheme,
    root: Arc<str>,
    name: Arc<str>,

    native_capability: Capability,
    full_capability: Capability,

    http_client: HttpClient,
    executor: Executor,
}

/// Info for the accessor. Users can use this struct to retrieve information about the underlying backend.
///
/// This struct is intentionally not implemented with `Clone` to ensure that all accesses
/// within the same operator, access layers, and services use the same instance of `AccessorInfo`.
/// This is especially important for `HttpClient` and `Executor`.
///
/// ## Panic Safety
///
/// All methods provided by `AccessorInfo` will safely handle lock poisoning scenarios.
/// If the inner `RwLock` is poisoned (which happens when another thread panicked while holding
/// the write lock), this method will gracefully continue execution.
///
/// - For read operations, the method will return the current state.
/// - For write operations, the method will do nothing.
///
/// ## Maintain Notes
///
/// We are using `std::sync::RwLock` to provide thread-safe access to the inner data.
///
/// I have performed [the bench across different arc-swap alike crates](https://github.com/krdln/arc-swap-benches):
///
/// ```txt
/// test arcswap                    ... bench:          14.85 ns/iter (+/- 0.33)
/// test arcswap_full               ... bench:         128.27 ns/iter (+/- 4.30)
/// test baseline                   ... bench:          11.33 ns/iter (+/- 0.76)
/// test mutex_4                    ... bench:         296.73 ns/iter (+/- 49.96)
/// test mutex_unconteded           ... bench:          13.26 ns/iter (+/- 0.56)
/// test rwlock_fast_4              ... bench:         201.60 ns/iter (+/- 7.47)
/// test rwlock_fast_uncontended    ... bench:          12.77 ns/iter (+/- 0.37)
/// test rwlock_parking_4           ... bench:         232.02 ns/iter (+/- 11.14)
/// test rwlock_parking_uncontended ... bench:          13.18 ns/iter (+/- 0.39)
/// test rwlock_std_4               ... bench:         219.56 ns/iter (+/- 5.56)
/// test rwlock_std_uncontended     ... bench:          13.55 ns/iter (+/- 0.33)
/// ```
///
/// The results show that as long as there aren't too many uncontended accesses, `RwLock` is the
/// best choice, allowing for fast access and the ability to modify partial data without cloning
/// everything.
///
/// And it's true: we only update and modify the internal data in a few instances, such as when
/// building an operator or applying new layers.
#[derive(Debug, Default)]
pub struct AccessorInfo {
    inner: std::sync::RwLock<AccessorInfoInner>,
}

impl PartialEq for AccessorInfo {
    fn eq(&self, other: &Self) -> bool {
        self.scheme() == other.scheme()
            && self.root() == other.root()
            && self.name() == other.name()
    }
}

impl Eq for AccessorInfo {}

impl Hash for AccessorInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.scheme().hash(state);
        self.root().hash(state);
        self.name().hash(state);
    }
}

impl AccessorInfo {
    /// [`Scheme`] of backend.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply returning the current scheme.
    pub fn scheme(&self) -> Scheme {
        match self.inner.read() {
            Ok(v) => v.scheme,
            Err(err) => err.get_ref().scheme,
        }
    }

    /// Set [`Scheme`] for backend.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply skipping the update operation
    /// rather than propagating the panic.
    pub fn set_scheme(&self, scheme: Scheme) -> &Self {
        if let Ok(mut v) = self.inner.write() {
            v.scheme = scheme;
        }

        self
    }

    /// Root of backend, will be in format like `/path/to/dir/`
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply returning the current root.
    pub fn root(&self) -> Arc<str> {
        match self.inner.read() {
            Ok(v) => v.root.clone(),
            Err(err) => err.get_ref().root.clone(),
        }
    }

    /// Set root for backend.
    ///
    /// Note: input root must be normalized.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply skipping the update operation
    /// rather than propagating the panic.
    pub fn set_root(&self, root: &str) -> &Self {
        if let Ok(mut v) = self.inner.write() {
            v.root = Arc::from(root);
        }

        self
    }

    /// Name of backend, could be empty if underlying backend doesn't have namespace concept.
    ///
    /// For example:
    ///
    /// - name for `s3` => bucket name
    /// - name for `azblob` => container name
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply returning the current scheme.
    pub fn name(&self) -> Arc<str> {
        match self.inner.read() {
            Ok(v) => v.name.clone(),
            Err(err) => err.get_ref().name.clone(),
        }
    }

    /// Set name of this backend.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply skipping the update operation
    /// rather than propagating the panic.
    pub fn set_name(&self, name: &str) -> &Self {
        if let Ok(mut v) = self.inner.write() {
            v.name = Arc::from(name)
        }

        self
    }

    /// Get backend's native capabilities.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply returning the current native capability.
    pub fn native_capability(&self) -> Capability {
        match self.inner.read() {
            Ok(v) => v.native_capability,
            Err(err) => err.get_ref().native_capability,
        }
    }

    /// Set native capabilities for service.
    ///
    /// # NOTES
    ///
    /// Set native capability will also flush the full capability. The only way to change
    /// full_capability is via `update_full_capability`.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply skipping the update operation
    /// rather than propagating the panic.
    pub fn set_native_capability(&self, capability: Capability) -> &Self {
        if let Ok(mut v) = self.inner.write() {
            v.native_capability = capability;
            v.full_capability = capability;
        }

        self
    }

    /// Get service's full capabilities.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply returning the current native capability.
    pub fn full_capability(&self) -> Capability {
        match self.inner.read() {
            Ok(v) => v.full_capability,
            Err(err) => err.get_ref().full_capability,
        }
    }

    /// Update service's full capabilities.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply skipping the update operation
    /// rather than propagating the panic.
    pub fn update_full_capability(&self, f: impl FnOnce(Capability) -> Capability) -> &Self {
        if let Ok(mut v) = self.inner.write() {
            v.full_capability = f(v.full_capability);
        }

        self
    }

    /// Get http client from the context.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply returning the current http client.
    pub fn http_client(&self) -> HttpClient {
        match self.inner.read() {
            Ok(v) => v.http_client.clone(),
            Err(err) => err.get_ref().http_client.clone(),
        }
    }

    /// Update http client for the context.
    ///
    /// # Note
    ///
    /// Requests must be forwarded to the old HTTP client after the update. Otherwise, features such as retry, tracing, and metrics may not function properly.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply skipping the update operation.
    pub fn update_http_client(&self, f: impl FnOnce(HttpClient) -> HttpClient) -> &Self {
        if let Ok(mut v) = self.inner.write() {
            let client = mem::take(&mut v.http_client);
            v.http_client = f(client);
        }

        self
    }

    /// Get executor from the context.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply returning the current executor.
    pub fn executor(&self) -> Executor {
        match self.inner.read() {
            Ok(v) => v.executor.clone(),
            Err(err) => err.get_ref().executor.clone(),
        }
    }

    /// Update executor for the context.
    ///
    /// # Note
    ///
    /// Tasks must be forwarded to the old executor after the update. Otherwise, features such as retry, timeout, and metrics may not function properly.
    ///
    /// # Panic Safety
    ///
    /// This method safely handles lock poisoning scenarios. If the inner `RwLock` is poisoned,
    /// this method will gracefully continue execution by simply skipping the update operation.
    pub fn update_executor(&self, f: impl FnOnce(Executor) -> Executor) -> &Self {
        if let Ok(mut v) = self.inner.write() {
            let executor = mem::take(&mut v.executor);
            v.executor = f(executor);
        }

        self
    }
}
