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

use crate::ops::*;
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
pub trait Accessor: Send + Sync + Debug + Unpin + 'static {
    /// Reader is the associated reader the could return in `read` operation.
    type Reader: output::Read;
    /// BlockingReader is the associated reader that could return in
    /// `blocking_read` operation.
    type BlockingReader: output::BlockingRead;

    /// Invoke the `metadata` operation to get metadata of accessor.
    fn metadata(&self) -> AccessorMetadata {
        unimplemented!("metadata() is required to be implemented")
    }

    /// Invoke the `create` operation on the specified path
    ///
    /// # Behavior
    ///
    /// - Input path MUST match with ObjectMode, DON'T NEED to check object mode.
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
    /// [`ObjectReader`][crate::ObjectReader] if operate successful.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check object mode.
    /// - The returning contnet length may be smaller than the range specifed.
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
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check object mode.
    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let (_, _, _) = (path, args, r);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `stat` operation on the specified path.
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
    /// # Behavior
    ///
    /// - Input path MUST be dir path, DON'T NEED to check object mode.
    /// - List non-exist dir should return Empty.
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, ObjectPager)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `presign` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Presign`
    /// - This API is optional, return [`std::io::ErrorKind::Unsupported`] if not supported.
    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
        r: input::Reader,
    ) -> Result<RpWriteMultipart> {
        let (_, _, _) = (path, args, r);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_create` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::create`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
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
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
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
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
        let (_, _, _) = (path, args, r);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    /// Invoke the `blocking_stat` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::stat`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
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
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
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
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    /// - List non-exist dir should return Empty.
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        let (_, _) = (path, args);

        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<dyn Accessor>`.
#[async_trait]
impl<T: Accessor> Accessor for Arc<T> {
    type Reader = T::Reader;
    type BlockingReader = T::BlockingReader;

    fn metadata(&self) -> AccessorMetadata {
        self.as_ref().metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.as_ref().create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.as_ref().read(path, args).await
    }
    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
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
        r: input::Reader,
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
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.as_ref().blocking_read(path, args)
    }
    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
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
}

/// FusedAccessor is the type erased accessor with `Box<dyn Reader>`.
pub type FusedAccessor =
    Arc<dyn Accessor<Reader = output::Reader, BlockingReader = output::BlockingReader>>;

#[async_trait]
impl Accessor for FusedAccessor {
    type Reader = output::Reader;
    type BlockingReader = output::BlockingReader;

    fn metadata(&self) -> AccessorMetadata {
        self.as_ref().metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.as_ref().create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.as_ref().read(path, args).await
    }
    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
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
        r: input::Reader,
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
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.as_ref().blocking_read(path, args)
    }
    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
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
}

/// Metadata for accessor, users can use this metadata to get information of underlying backend.
#[derive(Clone, Debug, Default)]
pub struct AccessorMetadata {
    scheme: Scheme,
    root: String,
    name: String,
    capabilities: FlagSet<AccessorCapability>,
    hints: FlagSet<AccessorHint>,
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
        /// Add this capability if service supports `presign`
        Presign,
        /// Add this capability if service supports `multipart`
        Multipart,
        /// Add this capability if service supports `blocking`
        Blocking,
    }
}

flags! {
    /// AccessorHint describes accessor's hint.
    ///
    /// Hint means developers can do optimize for this accessor.
    ///
    /// All hints are internal used only and will not be exposed to users.
    pub enum AccessorHint: u64 {
        /// Read is seekable means the underlying read is seekable.
        ///
        /// We can reuse the same reader instead of always creating new one.
        ReadIsSeekable,
        /// Read is seekable means the underlying read is streamable.
        ///
        /// It's better to use stream to reading data.
        ReadIsStreamable,
    }
}
