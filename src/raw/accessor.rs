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
use flagset::flags;
use flagset::FlagSet;

use crate::raw::*;
use crate::*;

/// Underlying trait of all backends for implementors.
///
/// # Note
///
/// Only service implementor should care about this trait, users need to
/// use [`Operator`][crate::Operator] instead.
///
/// # Operations
///
/// | Name | Capability |
/// | ---- | ---------- |
/// | [`metadata`][Accessor::metadata] | - |
/// | [`create`][Accessor::create] | - |
/// | [`read`][Accessor::read] | - |
/// | [`write`][Accessor::write] | - |
/// | [`delete`][Accessor::delete] | - |
/// | [`list`][Accessor::list] | - |
/// | [`open`][Accessor::open] | `Open` |
/// | [`presign`][Accessor::presign] | `Presign` |
/// | [`create_multipart`][Accessor::create_multipart] | `Multipart` |
/// | [`write_multipart`][Accessor::write_multipart] | `Multipart` |
/// | [`complete_multipart`][Accessor::complete_multipart] | `Multipart` |
/// | [`abort_multipart`][Accessor::abort_multipart] | `Multipart` |
/// | [`blocking_create`][Accessor::blocking_create] | `Blocking` |
/// | [`blocking_read`][Accessor::blocking_read] | `Blocking` |
/// | [`blocking_write`][Accessor::blocking_write] | `Blocking` |
/// | [`blocking_delete`][Accessor::blocking_delete] | `Blocking` |
/// | [`blocking_list`][Accessor::blocking_list] | `Blocking` |
/// | [`blocking_open`][Accessor::blocking_open | `Blocking` && `Open` |
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
///   - The default implementation should return [`std::io::ErrorKind::Unsupported`].
#[async_trait]
pub trait Accessor: Send + Sync + Debug + 'static {
    /// Return the inner accessor if there is one.
    ///
    /// # Behavior
    ///
    /// - Service should not implement this method.
    /// - Layers can implement this method to forward API call to inner accessor.
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        None
    }

    /// Invoke the `metadata` operation to get metadata of accessor.
    fn metadata(&self) -> AccessorMetadata {
        match self.inner() {
            None => {
                unimplemented!()
            }
            Some(inner) => inner.metadata(),
        }
    }

    /// Invoke the `create` operation on the specified path
    ///
    /// # Behavior
    ///
    /// - Input path MUST match with ObjectMode, DON'T NEED to check object mode.
    /// - Create on existing dir SHOULD succeed.
    /// - Create on existing file SHOULD overwrite and truncate.
    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        match self.inner() {
            Some(inner) => inner.create(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `read` operation on the specified path, returns a
    /// [`ObjectReader`][crate::ObjectReader] if operate successful.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check object mode.
    /// - The returning contnet length may be smaller than the range specifed.
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, OutputBytesReader)> {
        match self.inner() {
            Some(inner) => inner.read(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `write` operation on the specified path, returns a
    /// written size if operate successful.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check object mode.
    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        match self.inner() {
            Some(inner) => inner.write(path, args, r).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `stat` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - `stat` empty path means stat backend's root path.
    /// - `stat` a path endswith "/" means stating a dir.
    /// - `mode` and `content_length` must be set.
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        match self.inner() {
            Some(inner) => inner.stat(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `delete` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - `delete` is an idempotent operation, it's safe to call `Delete` on the same path multiple times.
    /// - `delete` SHOULD return `Ok(())` if the path is deleted successfully or not exist.
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        match self.inner() {
            Some(inner) => inner.delete(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `list` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be dir path, DON'T NEED to check object mode.
    /// - List non-exist dir should return Empty.
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, ObjectPager)> {
        match self.inner() {
            Some(inner) => inner.list(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `open` operation on the specified path.
    async fn open(&self, path: &str, args: OpOpen) -> Result<(RpOpen, BytesHandler)> {
        match self.inner() {
            Some(inner) => inner.open(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `presign` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Presign`
    /// - This API is optional, return [`std::io::ErrorKind::Unsupported`] if not supported.
    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        match self.inner() {
            Some(inner) => inner.presign(path, args),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `create_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    /// - This op returns a `upload_id` which is required to for following APIs.
    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        match self.inner() {
            Some(inner) => inner.create_multipart(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `write_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<RpWriteMultipart> {
        match self.inner() {
            Some(inner) => inner.write_multipart(path, args, r).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `complete_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        match self.inner() {
            Some(inner) => inner.complete_multipart(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `abort_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        match self.inner() {
            Some(inner) => inner.abort_multipart(path, args).await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `blocking_create` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::create`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        match self.inner() {
            Some(inner) => inner.blocking_create(path, args),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `blocking_read` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::read`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, BlockingBytesReader)> {
        match self.inner() {
            Some(inner) => inner.blocking_read(path, args),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `blocking_write` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::write`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<RpWrite> {
        match self.inner() {
            Some(inner) => inner.blocking_write(path, args, r),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `blocking_stat` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::stat`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        match self.inner() {
            Some(inner) => inner.blocking_stat(path, args),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `blocking_delete` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::delete`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        match self.inner() {
            Some(inner) => inner.blocking_delete(path, args),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invoke the `blocking_list` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::list`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    /// - List non-exist dir should return Empty.
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        match self.inner() {
            Some(inner) => inner.blocking_list(path, args),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }

    /// Invode the `blocking_open` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::open`]
    fn blocking_open(&self, path: &str, args: OpOpen) -> Result<(RpOpen, BlockingBytesHandler)> {
        match self.inner() {
            Some(inner) => inner.blocking_open(path, args),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<dyn Accessor>`.
#[async_trait]
impl<T: Accessor> Accessor for Arc<T> {
    fn metadata(&self) -> AccessorMetadata {
        self.as_ref().metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.as_ref().create(path, args).await
    }
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, OutputBytesReader)> {
        self.as_ref().read(path, args).await
    }
    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        self.as_ref().write(path, args, r).await
    }
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.as_ref().stat(path, args).await
    }
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.as_ref().delete(path, args).await
    }
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, ObjectPager)> {
        self.as_ref().list(path, args).await
    }
    async fn open(&self, path: &str, args: OpOpen) -> Result<(RpOpen, BytesHandler)> {
        self.as_ref().open(path, args).await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.as_ref().presign(path, args)
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        self.as_ref().create_multipart(path, args).await
    }
    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<RpWriteMultipart> {
        self.as_ref().write_multipart(path, args, r).await
    }
    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        self.as_ref().complete_multipart(path, args).await
    }
    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        self.as_ref().abort_multipart(path, args).await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.as_ref().blocking_create(path, args)
    }
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, BlockingBytesReader)> {
        self.as_ref().blocking_read(path, args)
    }
    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<RpWrite> {
        self.as_ref().blocking_write(path, args, r)
    }
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.as_ref().blocking_stat(path, args)
    }
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.as_ref().blocking_delete(path, args)
    }
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        self.as_ref().blocking_list(path, args)
    }
    fn blocking_open(&self, path: &str, args: OpOpen) -> Result<(RpOpen, BlockingBytesHandler)> {
        self.as_ref().blocking_open(path, args)
    }
}

/// Metadata for accessor, users can use this metadata to get information of underlying backend.
#[derive(Clone, Debug, Default)]
pub struct AccessorMetadata {
    scheme: Scheme,
    root: String,
    name: String,
    capabilities: FlagSet<AccessorCapability>,
}

impl AccessorMetadata {
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
        /// Add this capability if service supports `presign`
        Presign,
        /// Add this capability if service supports `multipart`
        Multipart,
        /// Add this capability if service supports `blocking`
        Blocking,
        /// Add this capability if service supports `open`
        Open,
    }
}
