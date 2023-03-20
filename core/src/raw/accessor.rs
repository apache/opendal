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
use flagset::flags;
use flagset::FlagSet;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Underlying trait of all backends for implementors.
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
#[async_trait]
pub trait Accessor: Send + Sync + Debug + Unpin + 'static {
    /// Reader is the associated reader the could return in `read` operation.
    type Reader: oio::Read;
    /// BlockingReader is the associated reader that could return in
    /// `blocking_read` operation.
    type BlockingReader: oio::BlockingRead;
    /// Reader is the associated writer the could return in `write` operation.
    type Writer: oio::Write;
    /// BlockingWriter is the associated writer the could return in
    /// `blocking_write` operation.
    type BlockingWriter: oio::BlockingWrite;
    /// Pager is the associated page that return in `list` or `scan` operation.
    type Pager: oio::Page;
    /// BlockingPager is the associated pager that could return in
    /// `blocking_list` or `scan` operation.
    type BlockingPager: oio::BlockingPage;

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
    /// - hints: declare the hints of current backend
    fn info(&self) -> AccessorInfo;

    /// Invoke the `create` operation on the specified path
    ///
    /// Require [`AccessorCapability::Write`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST match with EntryMode, DON'T NEED to check mode.
    /// - Create on existing dir SHOULD succeed.
    /// - Create on existing file SHOULD overwrite and truncate.
    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `read` operation on the specified path, returns a
    /// [`Reader`][crate::Reader] if operate successful.
    ///
    /// Require [`AccessorCapability::Read`]
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
    /// Require [`AccessorCapability::Write`]
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

    /// Invoke the `stat` operation on the specified path.
    ///
    /// Require [`AccessorCapability::Read`]
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
    /// Require [`AccessorCapability::Write`]
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
    /// Require [`AccessorCapability::List`]
    ///
    /// # Behavior
    ///
    /// - Input path MUST be dir path, DON'T NEED to check mode.
    /// - List non-exist dir should return Empty.
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `scan` operation on the specified path.
    ///
    /// Require [`AccessorCapability::Scan`]
    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `presign` operation on the specified path.
    ///
    /// Require [`AccessorCapability::Presign`]
    ///
    /// # Behavior
    ///
    /// - This API is optional, return [`std::io::ErrorKind::Unsupported`] if not supported.
    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `batch` operations.
    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let _ = args;

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_create` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::create`]
    ///
    /// Require [`AccessorCapability::Write`] and [`AccessorCapability::Blocking`]
    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
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
    /// Require [`AccessorCapability::Read`] and [`AccessorCapability::Blocking`]
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
    /// Require [`AccessorCapability::Write`] and [`AccessorCapability::Blocking`]
    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
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
    /// Require [`AccessorCapability::Read`] and [`AccessorCapability::Blocking`]
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
    /// Require [`AccessorCapability::Write`] and [`AccessorCapability::Blocking`]
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
    /// Require [`AccessorCapability::List`] and [`AccessorCapability::Blocking`]
    ///
    /// # Behavior
    ///
    /// - List non-exist dir should return Empty.
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_scan` operation on the specified path.
    ///
    /// Require [`AccessorCapability::Scan`] and [`AccessorCapability::Blocking`]
    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

/// Dummy implementation of accessor.
#[async_trait]
impl Accessor for () {
    type Reader = ();
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        AccessorInfo {
            scheme: Scheme::Custom("dummy"),
            root: "".to_string(),
            name: "dummy".to_string(),
            max_batch_operations: None,
            capabilities: None.into(),
            hints: None.into(),
        }
    }
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<dyn Accessor>`.
#[async_trait]
impl<T: Accessor + ?Sized> Accessor for Arc<T> {
    type Reader = T::Reader;
    type BlockingReader = T::BlockingReader;
    type Writer = T::Writer;
    type BlockingWriter = T::BlockingWriter;
    type Pager = T::Pager;
    type BlockingPager = T::BlockingPager;

    fn info(&self) -> AccessorInfo {
        self.as_ref().info()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.as_ref().create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.as_ref().read(path, args).await
    }
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.as_ref().write(path, args).await
    }
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.as_ref().stat(path, args).await
    }
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.as_ref().delete(path, args).await
    }
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.as_ref().list(path, args).await
    }
    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        self.as_ref().scan(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.as_ref().batch(args).await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.as_ref().presign(path, args)
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.as_ref().blocking_create(path, args)
    }
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.as_ref().blocking_read(path, args)
    }
    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.as_ref().blocking_write(path, args)
    }
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.as_ref().blocking_stat(path, args)
    }
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.as_ref().blocking_delete(path, args)
    }
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.as_ref().blocking_list(path, args)
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.as_ref().blocking_scan(path, args)
    }
}

/// FusedAccessor is the type erased accessor with `Box<dyn Read>`.
pub type FusedAccessor = Arc<
    dyn Accessor<
        Reader = oio::Reader,
        BlockingReader = oio::BlockingReader,
        Writer = oio::Writer,
        BlockingWriter = oio::BlockingWriter,
        Pager = oio::Pager,
        BlockingPager = oio::BlockingPager,
    >,
>;

/// Metadata for accessor, users can use this metadata to get information of underlying backend.
#[derive(Clone, Debug, Default)]
pub struct AccessorInfo {
    scheme: Scheme,
    root: String,
    name: String,
    /// limit of batch operation
    /// only meaningful when accessor supports batch operation
    max_batch_operations: Option<usize>,
    capabilities: FlagSet<AccessorCapability>,
    hints: FlagSet<AccessorHint>,
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

    /// backend's number limitation of operations in a single batch.
    ///
    /// # Note
    /// - Got Some(x): limitation is x
    /// - Got None: no limitation
    pub(crate) fn max_batch_operations(&self) -> Option<usize> {
        self.max_batch_operations
    }

    /// Set batch size limit for backend.
    pub(crate) fn set_max_batch_operations(&mut self, limit: usize) -> &mut Self {
        self.max_batch_operations = Some(limit);
        self
    }

    /// Get backend's capabilities.
    pub fn capabilities(&self) -> FlagSet<AccessorCapability> {
        self.capabilities
    }

    /// Set capabilities for backend.
    pub fn set_capabilities(
        &mut self,
        capabilities: impl Into<FlagSet<AccessorCapability>>,
    ) -> &mut Self {
        self.capabilities = capabilities.into();
        self
    }

    /// Get backend's hints.
    pub fn hints(&self) -> FlagSet<AccessorHint> {
        self.hints
    }

    /// Set hints for backend.
    pub fn set_hints(&mut self, hints: impl Into<FlagSet<AccessorHint>>) -> &mut Self {
        self.hints = hints.into();
        self
    }
}

flags! {
    /// AccessorCapability describes accessor's advanced capability.
    pub enum AccessorCapability: u32 {
        /// Add this capability if service supports `read` and `stat`
        Read,
        /// Add this capability if service supports `write` and `delete`
        Write,
        /// Add this capability if service supports `list`
        List,
        /// Add this capability if service supports `scan`
        Scan,
        /// Add this capability if service supports `presign`
        Presign,
        /// Add this capability if service supports `blocking`
        Blocking,
        /// Add this capability if service supports `batch`
        Batch,
    }
}

flags! {
    /// AccessorHint describes accessor's hint.
    ///
    /// Hint means developers can do optimize for this accessor.
    ///
    /// All hints are internal used only and will not be exposed to users.
    pub enum AccessorHint: u64 {
        /// Read seekable means the underlying read is seekable.
        ///
        /// We can reuse the same reader instead of always creating new one.
        ReadSeekable,
        /// Read streamable means the underlying read is streamable.
        ///
        /// It's better to use stream to reading data.
        ReadStreamable,
    }
}
