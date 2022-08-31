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
use std::io::Result;
use std::sync::Arc;

use async_trait::async_trait;
use flagset::flags;
use flagset::FlagSet;

use crate::error::new_unsupported_object_error;
use crate::multipart::ObjectPart;
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
use crate::BytesReader;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::Scheme;
use crate::{BlockingBytesReader, DirIterator};

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
/// - Operations without capability requirement like `metadata`, `create` are
///   basic operations.
///   - All services must implement them.
///   - Use `unimplemented!()` if not implemented or can't implement.
/// - Operations with capability requirement like `presign` are optional operations.
///   - Services can implement them based on services capabilities.
///   - The default implementation should return [`std::io::ErrorKind::Unsupported`].
#[async_trait]
pub trait Accessor: Send + Sync + Debug {
    /// Invoke the `metadata` operation to get metadata of accessor.
    fn metadata(&self) -> AccessorMetadata {
        unimplemented!()
    }

    /// Invoke the `create` operation on the specified path
    ///
    /// # Behavior
    ///
    /// - Input path MUST match with ObjectMode, DON'T NEED to check object mode.
    /// - Create on existing dir SHOULD succeed.
    /// - Create on existing file SHOULD overwrite and truncate.
    async fn create(&self, args: &OpCreate) -> Result<()> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `read` operation on the specified path, returns a
    /// [`BytesReader`][crate::BytesReader] if operate successful.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check object mode.
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `write` operation on the specified path, returns a
    /// written size if operate successful.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check object mode.
    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let (_, _) = (args, r);
        unimplemented!()
    }

    /// Invoke the `stat` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - `stat` empty path means stat backend's root path.
    /// - `stat` a path endswith "/" means stating a dir.
    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `delete` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - `delete` is an idempotent operation, it's safe to call `Delete` on the same path multiple times.
    /// - `delete` SHOULD return `Ok(())` if the path is deleted successfully or not exist.
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `list` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be dir path, DON'T NEED to check object mode.
    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `presign` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Presign`
    /// - This API is optional, return [`std::io::ErrorKind::Unsupported`] if not supported.
    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        return Err(new_unsupported_object_error(
            Operation::Presign,
            args.path(),
        ));
    }

    /// Invoke the `create_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    /// - This op returns a `upload_id` which is required to for following APIs.
    async fn create_multipart(&self, args: &OpCreateMultipart) -> Result<String> {
        let _ = args;

        return Err(new_unsupported_object_error(
            Operation::CreateMultipart,
            args.path(),
        ));
    }

    /// Invoke the `write_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    async fn write_multipart(&self, args: &OpWriteMultipart, r: BytesReader) -> Result<ObjectPart> {
        let (_, _) = (args, r);

        return Err(new_unsupported_object_error(
            Operation::WriteMultipart,
            args.path(),
        ));
    }

    /// Invoke the `complete_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    async fn complete_multipart(&self, args: &OpCompleteMultipart) -> Result<()> {
        let _ = args;

        return Err(new_unsupported_object_error(
            Operation::CompleteMultipart,
            args.path(),
        ));
    }

    /// Invoke the `abort_multipart` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - Require capability: `Multipart`
    async fn abort_multipart(&self, args: &OpAbortMultipart) -> Result<()> {
        let _ = args;

        return Err(new_unsupported_object_error(
            Operation::AbortMultipart,
            args.path(),
        ));
    }

    /// Invoke the `blocking_create` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::create`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_create(&self, args: &OpCreate) -> Result<()> {
        let _ = args;

        return Err(new_unsupported_object_error(
            Operation::BlockingCreate,
            args.path(),
        ));
    }

    /// Invoke the `blocking_read` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::read`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_read(&self, args: &OpRead) -> Result<BlockingBytesReader> {
        let _ = args;

        return Err(new_unsupported_object_error(
            Operation::BlockingRead,
            args.path(),
        ));
    }

    /// Invoke the `blocking_write` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::write`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_write(&self, args: &OpWrite, r: BlockingBytesReader) -> Result<u64> {
        let (_, _) = (args, r);

        return Err(new_unsupported_object_error(
            Operation::BlockingWrite,
            args.path(),
        ));
    }

    /// Invoke the `blocking_stat` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::stat`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let _ = args;

        return Err(new_unsupported_object_error(
            Operation::BlockingStat,
            args.path(),
        ));
    }

    /// Invoke the `blocking_delete` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::delete`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_delete(&self, args: &OpDelete) -> Result<()> {
        let _ = args;

        return Err(new_unsupported_object_error(
            Operation::BlockingDelete,
            args.path(),
        ));
    }

    /// Invoke the `blocking_list` operation on the specified path.
    ///
    /// This operation is the blocking version of [`Accessor::list`]
    ///
    /// # Behavior
    ///
    /// - Require capability: `Blocking`
    fn blocking_list(&self, args: &OpList) -> Result<DirIterator> {
        let _ = args;

        return Err(new_unsupported_object_error(
            Operation::BlockingList,
            args.path(),
        ));
    }
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<dyn Accessor>`.
#[async_trait]
impl<T: Accessor> Accessor for Arc<T> {
    fn metadata(&self) -> AccessorMetadata {
        self.as_ref().metadata()
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        self.as_ref().create(args).await
    }
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        self.as_ref().read(args).await
    }
    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        self.as_ref().write(args, r).await
    }
    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        self.as_ref().stat(args).await
    }
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        self.as_ref().delete(args).await
    }
    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        self.as_ref().list(args).await
    }

    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        self.as_ref().presign(args)
    }

    async fn create_multipart(&self, args: &OpCreateMultipart) -> Result<String> {
        self.as_ref().create_multipart(args).await
    }
    async fn write_multipart(&self, args: &OpWriteMultipart, r: BytesReader) -> Result<ObjectPart> {
        self.as_ref().write_multipart(args, r).await
    }
    async fn complete_multipart(&self, args: &OpCompleteMultipart) -> Result<()> {
        self.as_ref().complete_multipart(args).await
    }
    async fn abort_multipart(&self, args: &OpAbortMultipart) -> Result<()> {
        self.as_ref().abort_multipart(args).await
    }

    fn blocking_create(&self, args: &OpCreate) -> Result<()> {
        self.as_ref().blocking_create(args)
    }
    fn blocking_read(&self, args: &OpRead) -> Result<BlockingBytesReader> {
        self.as_ref().blocking_read(args)
    }
    fn blocking_write(&self, args: &OpWrite, r: BlockingBytesReader) -> Result<u64> {
        self.as_ref().blocking_write(args, r)
    }
    fn blocking_stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        self.as_ref().blocking_stat(args)
    }
    fn blocking_delete(&self, args: &OpDelete) -> Result<()> {
        self.as_ref().blocking_delete(args)
    }
    fn blocking_list(&self, args: &OpList) -> Result<DirIterator> {
        self.as_ref().blocking_list(args)
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

    pub(crate) fn set_scheme(&mut self, scheme: Scheme) -> &mut Self {
        self.scheme = scheme;
        self
    }

    /// Root of backend, will be in format like `/path/to/dir/`
    pub fn root(&self) -> &str {
        &self.root
    }

    pub(crate) fn set_root(&mut self, root: &str) -> &mut Self {
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

    pub(crate) fn set_name(&mut self, name: &str) -> &mut Self {
        self.name = name.to_string();
        self
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

    pub(crate) fn set_capabilities(
        &mut self,
        capabilities: impl Into<FlagSet<AccessorCapability>>,
    ) -> &mut Self {
        self.capabilities = capabilities.into();
        self
    }
}

flags! {
    /// AccessorCapability describes accessor's advanced capability.
    pub(crate) enum AccessorCapability: u32 {
        /// Add this capability if service supports `presign`
        Presign,
        /// Add this capability if service supports `multipart`
        Multipart,
        /// Add this capability if service supports `blocking`
        Blocking,
    }
}
