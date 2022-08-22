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
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use flagset::flags;
use flagset::FlagSet;

use crate::error::ObjectError;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::PresignedRequest;
use crate::BytesReader;
use crate::BytesWriter;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::Scheme;

/// Underlying trait of all backends for implementors.
///
/// # Note to users
///
/// Only service implementor should care about this trait, users need to
/// use [`Operator`][crate::Operator] instead.
///
/// # Note to services
///
/// - Path in args will all be normalized into the same style, services
///   should handle them based on services' requirement.
/// - `metadata`, `create`, `read`, `write`, `stat`, `delete` and `list`
///   are basic functions while required to be implemented, use `unimplemented!()`
///   if not implemented or can't implement.
/// - Other APIs are optional and should be recorded in `AccessorCapability`.
///   Return [`std::io::ErrorKind::Unsupported`] if not supported.
#[async_trait]
pub trait Accessor: Send + Sync + Debug {
    /// Required APIs.
    /// -------------

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
    /// [`BytesWriter`][crate::BytesWriter] if operate successful.
    ///
    /// # Behavior
    ///
    /// - Input path MUST be file path, DON'T NEED to check object mode.
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        let _ = args;
        unimplemented!()
    }

    async fn writex(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
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

    /// Optional APIs (Capabilities)
    /// ---------------------------

    /// Invoke the `presign` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - This API is optional, return [`std::io::ErrorKind::Unsupported`] if not supported.
    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        return Err(Error::new(
            ErrorKind::Unsupported,
            ObjectError::new("presign", args.path(), anyhow!("")),
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
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        self.as_ref().write(args).await
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
    }
}
