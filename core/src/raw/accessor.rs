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

use async_trait::async_trait;

use crate::raw::*;
use crate::*;

/// Underlying trait of all backends for implementors.
///
/// The actual data access of storage service happens in Accessor layer.
/// Every storage supported by OpenDAL must implement [`Accessor`] but not all
/// methods of [`Accessor`] will be implemented according to how the storage service is.
///
/// For example, user can not modify the content from one HTTP file server directly.
/// So [`Http`][crate::services::Http] implements and provides only read related actions.
///
/// [`Accessor`] gives default implementation for all methods which will raise [`ErrorKind::Unsupported`] error.
/// And what action this [`Accessor`] supports will be pointed out in [`AccessorInfo`].
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
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Accessor: Send + Sync + Debug + Unpin + 'static {
    /// Reader is the associated reader the could return in `read` operation.
    type Reader: oio::Read;
    /// BlockingReader is the associated reader that could return in
    /// `blocking_read` operation.
    type BlockingReader: oio::BlockingRead;
    /// Writer is the associated writer the could return in `write` operation.
    type Writer: oio::Write;
    /// BlockingWriter is the associated writer the could return in
    /// `blocking_write` operation.
    type BlockingWriter: oio::BlockingWrite;
    /// Lister is the associated lister that return in `list` operation.
    type Lister: oio::List;
    /// BlockingLister is the associated lister that could return in
    /// `blocking_list` operation.
    type BlockingLister: oio::BlockingList;

    /// Invoke the `info` operation to get metadata of accessor.
    ///
    /// # Notes
    ///
    /// This function is required to be implemented.
    ///
    /// By returning AccessorInfo, underlying services can declare
    /// some useful information about it self.
    ///
    /// - scheme: declare the scheme of backend.
    /// - capabilities: declare the capabilities of current backend.
    fn info(&self) -> AccessorInfo;

    /// Invoke the `create` operation on the specified path
    ///
    /// Require [`Capability::create_dir`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST match with EntryMode, DON'T NEED to check mode.
    /// - Create on existing dir SHOULD succeed.
    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `write` operation on the specified path, returns a
    /// written size if operate successful.
    ///
    /// Require [`Capability::write`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check mode.
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let (_, _, _) = (from, to, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `rename` operation on the specified `from` path and `to` path.
    ///
    /// Require [Capability::rename]
    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let (_, _, _) = (from, to, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `delete` operation on the specified path.
    ///
    /// Require [`Capability::delete`]
    ///
    /// # Behavior
    ///
    /// - `delete` is an idempotent operation, it's safe to call `Delete` on the same path multiple times.
    /// - `delete` SHOULD return `Ok(())` if the path is deleted successfully or not exist.
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `list` operation on the specified path.
    ///
    /// Require [`Capability::list`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST be dir path, DON'T NEED to check mode.
    /// - List non-exist dir should return Empty.
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `presign` operation on the specified path.
    ///
    /// Require [`Capability::presign`]
    ///
    /// # Behavior
    ///
    /// - This API is optional, return [`std::io::ErrorKind::Unsupported`] if not supported.
    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `batch` operations.
    ///
    /// Require [`Capability::batch`]
    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let _ = args;

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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

    /// Invoke the `blocking_delete` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::delete`]
    ///
    /// Require [`Capability::write`] and [`Capability::blocking`]
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let (_, _) = (path, args);

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
}

/// Dummy implementation of accessor.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Accessor for () {
    type Reader = ();
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Lister = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        AccessorInfo {
            scheme: Scheme::Custom("dummy"),
            root: "".to_string(),
            name: "dummy".to_string(),
            native_capability: Capability::default(),
            full_capability: Capability::default(),
        }
    }
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<dyn Accessor>`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T: Accessor + ?Sized> Accessor for Arc<T> {
    type Reader = T::Reader;
    type BlockingReader = T::BlockingReader;
    type Writer = T::Writer;
    type BlockingWriter = T::BlockingWriter;
    type Lister = T::Lister;
    type BlockingLister = T::BlockingLister;

    fn info(&self) -> AccessorInfo {
        self.as_ref().info()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.as_ref().create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.as_ref().read(path, args).await
    }
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.as_ref().write(path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.as_ref().copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.as_ref().rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.as_ref().stat(path, args).await
    }
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.as_ref().delete(path, args).await
    }
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.as_ref().list(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.as_ref().batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.as_ref().presign(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.as_ref().blocking_create_dir(path, args)
    }
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.as_ref().blocking_read(path, args)
    }
    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.as_ref().blocking_write(path, args)
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.as_ref().blocking_copy(from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.as_ref().blocking_rename(from, to, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.as_ref().blocking_stat(path, args)
    }
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.as_ref().blocking_delete(path, args)
    }
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.as_ref().blocking_list(path, args)
    }
}

/// FusedAccessor is the type erased accessor with `Arc<dyn Accessor>`.
pub type FusedAccessor = Arc<
    dyn Accessor<
        Reader = oio::Reader,
        BlockingReader = oio::BlockingReader,
        Writer = oio::Writer,
        BlockingWriter = oio::BlockingWriter,
        Lister = oio::Lister,
        BlockingLister = oio::BlockingLister,
    >,
>;

/// Metadata for accessor, users can use this metadata to get information of underlying backend.
#[derive(Clone, Debug, Default)]
pub struct AccessorInfo {
    scheme: Scheme,
    root: String,
    name: String,

    native_capability: Capability,
    full_capability: Capability,
}

impl AccessorInfo {
    /// [`Scheme`] of backend.
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// Set [`Scheme`] for backend.
    pub fn set_scheme(&mut self, scheme: Scheme) -> &mut Self {
        self.scheme = scheme;
        self
    }

    /// Root of backend, will be in format like `/path/to/dir/`
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Set root for backend.
    ///
    /// Note: input root must be normalized.
    pub fn set_root(&mut self, root: &str) -> &mut Self {
        self.root = root.to_string();
        self
    }

    /// Name of backend, could be empty if underlying backend doesn't have namespace concept.
    ///
    /// For example:
    ///
    /// - name for `s3` => bucket name
    /// - name for `azblob` => container name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set name of this backend.
    pub fn set_name(&mut self, name: &str) -> &mut Self {
        self.name = name.to_string();
        self
    }

    /// Get backend's native capabilities.
    pub fn native_capability(&self) -> Capability {
        self.native_capability
    }

    /// Set native capabilities for service.
    ///
    /// # NOTES
    ///
    /// Set native capability will also flush the full capability. The only way to change
    /// full_capability is via `full_capability_mut`.
    pub fn set_native_capability(&mut self, capability: Capability) -> &mut Self {
        self.native_capability = capability;
        self.full_capability = capability;
        self
    }

    /// Get service's full capabilities.
    pub fn full_capability(&self) -> Capability {
        self.full_capability
    }

    /// Get service's full capabilities.
    pub fn full_capability_mut(&mut self) -> &mut Capability {
        &mut self.full_capability
    }
}
