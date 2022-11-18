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

use crate::error::new_unsupported_object_error;
use crate::ops::OpAbortMultipart;
use crate::ops::OpCompleteMultipart;
use crate::ops::OpCreate;
use crate::ops::OpCreateMultipart;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::OpWriteMultipart;
use crate::ops::Operation;
use crate::ops::PresignedRequest;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::ObjectIterator;
use crate::ObjectMetadata;
use crate::ObjectPart;
use crate::ObjectReader;
use crate::ObjectStreamer;
use crate::Result;
use crate::Scheme;

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
/// | [`metadata`][crate::Accessor::metadata] | - |
/// | [`create`][crate::Accessor::create] | - |
/// | [`read`][crate::Accessor::read] | - |
/// | [`write`][crate::Accessor::write] | - |
/// | [`delete`][crate::Accessor::delete] | - |
/// | [`list`][crate::Accessor::list] | - |
/// | [`presign`][crate::Accessor::presign] | `Presign` |
/// | [`create_multipart`][crate::Accessor::create_multipart] | `Multipart` |
/// | [`write_multipart`][crate::Accessor::write_multipart] | `Multipart` |
/// | [`complete_multipart`][crate::Accessor::complete_multipart] | `Multipart` |
/// | [`abort_multipart`][crate::Accessor::abort_multipart] | `Multipart` |
/// | [`blocking_create`][crate::Accessor::blocking_create] | `Blocking` |
/// | [`blocking_read`][crate::Accessor::blocking_read] | `Blocking` |
/// | [`blocking_write`][crate::Accessor::blocking_write] | `Blocking` |
/// | [`blocking_delete`][crate::Accessor::blocking_delete] | `Blocking` |
/// | [`blocking_list`][crate::Accessor::blocking_list] | `Blocking` |
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
    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        match self.inner() {
            Some(inner) => inner.create(path, args).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::Create,
                path,
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
    async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {
        match self.inner() {
            Some(inner) => inner.read(path, args).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::Read,
                path,
            )),
        }
    }

    /// Invoke the `write` operation on the specified path, returns a
    /// written size if operate successful.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check object mode.
    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        match self.inner() {
            Some(inner) => inner.write(path, args, r).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::Write,
                path,
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
    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        match self.inner() {
            Some(inner) => inner.stat(path, args).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::Stat,
                path,
            )),
        }
    }

    /// Invoke the `delete` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - `delete` is an idempotent operation, it's safe to call `Delete` on the same path multiple times.
    /// - `delete` SHOULD return `Ok(())` if the path is deleted successfully or not exist.
    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        match self.inner() {
            Some(inner) => inner.delete(path, args).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::Delete,
                path,
            )),
        }
    }

    /// Invoke the `list` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be dir path, DON'T NEED to check object mode.
    /// - List non-exist dir should return Empty.
    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        match self.inner() {
            Some(inner) => inner.list(path, args).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::List,
                path,
            )),
        }
    }

    /// Invoke the `presign` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Presign`
    /// - This API is optional, return [`std::io::ErrorKind::Unsupported`] if not supported.
    fn presign(&self, path: &str, args: OpPresign) -> Result<PresignedRequest> {
        match self.inner() {
            Some(inner) => inner.presign(path, args),
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::Presign,
                path,
            )),
        }
    }

    /// Invoke the `create_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    /// - This op returns a `upload_id` which is required to for following APIs.
    async fn create_multipart(&self, path: &str, args: OpCreateMultipart) -> Result<String> {
        match self.inner() {
            Some(inner) => inner.create_multipart(path, args).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::CreateMultipart,
                path,
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
    ) -> Result<ObjectPart> {
        match self.inner() {
            Some(inner) => inner.write_multipart(path, args, r).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::WriteMultipart,
                path,
            )),
        }
    }

    /// Invoke the `complete_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    async fn complete_multipart(&self, path: &str, args: OpCompleteMultipart) -> Result<()> {
        match self.inner() {
            Some(inner) => inner.complete_multipart(path, args).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::CompleteMultipart,
                path,
            )),
        }
    }

    /// Invoke the `abort_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    async fn abort_multipart(&self, path: &str, args: OpAbortMultipart) -> Result<()> {
        match self.inner() {
            Some(inner) => inner.abort_multipart(path, args).await,
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::AbortMultipart,
                path,
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
    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        match self.inner() {
            Some(inner) => inner.blocking_create(path, args),
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::BlockingCreate,
                path,
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
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        match self.inner() {
            Some(inner) => inner.blocking_read(path, args),
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::BlockingRead,
                path,
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
    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        match self.inner() {
            Some(inner) => inner.blocking_write(path, args, r),
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::BlockingWrite,
                path,
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
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        match self.inner() {
            Some(inner) => inner.blocking_stat(path, args),
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::BlockingStat,
                path,
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
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<()> {
        match self.inner() {
            Some(inner) => inner.blocking_delete(path, args),
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::BlockingDelete,
                path,
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
    fn blocking_list(&self, path: &str, args: OpList) -> Result<ObjectIterator> {
        match self.inner() {
            Some(inner) => inner.blocking_list(path, args),
            None => Err(new_unsupported_object_error(
                self.metadata().scheme(),
                Operation::BlockingList,
                path,
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

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.as_ref().create(path, args).await
    }
    async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {
        self.as_ref().read(path, args).await
    }
    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        self.as_ref().write(path, args, r).await
    }
    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        self.as_ref().stat(path, args).await
    }
    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.as_ref().delete(path, args).await
    }
    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        self.as_ref().list(path, args).await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<PresignedRequest> {
        self.as_ref().presign(path, args)
    }

    async fn create_multipart(&self, path: &str, args: OpCreateMultipart) -> Result<String> {
        self.as_ref().create_multipart(path, args).await
    }
    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<ObjectPart> {
        self.as_ref().write_multipart(path, args, r).await
    }
    async fn complete_multipart(&self, path: &str, args: OpCompleteMultipart) -> Result<()> {
        self.as_ref().complete_multipart(path, args).await
    }
    async fn abort_multipart(&self, path: &str, args: OpAbortMultipart) -> Result<()> {
        self.as_ref().abort_multipart(path, args).await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.as_ref().blocking_create(path, args)
    }
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        self.as_ref().blocking_read(path, args)
    }
    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        self.as_ref().blocking_write(path, args, r)
    }
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        self.as_ref().blocking_stat(path, args)
    }
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.as_ref().blocking_delete(path, args)
    }
    fn blocking_list(&self, path: &str, args: OpList) -> Result<ObjectIterator> {
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

    /// Check if current backend supports [`Accessor::read`] or not.
    pub fn can_read(&self) -> bool {
        self.capabilities.contains(AccessorCapability::Read)
    }

    /// Check if current backend supports [`Accessor::write`] or not.
    pub fn can_write(&self) -> bool {
        self.capabilities.contains(AccessorCapability::Write)
    }

    /// Check if current backend supports [`Accessor::list`] or not.
    pub fn can_list(&self) -> bool {
        self.capabilities.contains(AccessorCapability::List)
    }

    /// Check if current backend supports [`Accessor::presign`] or not.
    pub fn can_presign(&self) -> bool {
        self.capabilities.contains(AccessorCapability::Presign)
    }

    /// Check if current backend supports multipart operations or not.
    pub fn can_multipart(&self) -> bool {
        self.capabilities.contains(AccessorCapability::Multipart)
    }

    /// Check if current backend supports blocking operations or not.
    pub fn can_blocking(&self) -> bool {
        self.capabilities.contains(AccessorCapability::Blocking)
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
    }
}
