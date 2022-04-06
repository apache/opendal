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

use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::BytesReader;
use crate::BytesWriter;
use crate::Metadata;
use crate::ObjectStreamer;

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
///   should handle them based on services requirement.
#[async_trait]
pub trait Accessor: Send + Sync + Debug {
    /// Invoke the `read` operation on the specified path, returns corresponding
    /// [`Metadata`] if operate successful.
    ///
    /// # Behavior
    ///
    /// | path type | mode | result   |
    /// | --------- | ---- | ------   |
    /// | file path | FILE | `Ok(_)`  |
    /// | dir path  | FILE | `Err(_)` |
    /// | file path | DIR  | `Err(_)`  |
    /// | dir path  | DIR  | `Ok(_)`  |
    async fn create(&self, args: &OpCreate) -> Result<()> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `read` operation on the specified path, returns a
    /// [`BytesReader`][crate::BytesReader] if operate successful.
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `write` operation on the specified path, returns a
    /// [`BytesWriter`][crate::BytesWriter] if operate successful.
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `stat` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - `stat` empty path means stat backend's root path.
    /// - `stat` a path endswith "/" means stating a dir.
    ///   - On fs, an error could return if not a dir.
    ///   - On s3 alike backends, a dir object will return no matter it exist or not.
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `delete` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - `delete` is an idempotent operation, it's safe to call `Delete` on the same path multiple times.
    /// - `delete` will return `Ok(())` if the path is deleted successfully or not exist.
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let _ = args;
        unimplemented!()
    }

    /// Invoke the `list` operation on the specified path.
    ///
    /// # Behavior
    ///
    /// - list on file path should emit an `NotADirectory` error.
    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        let _ = args;
        unimplemented!()
    }
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<dyn Accessor>`.
#[async_trait]
impl<T: Accessor> Accessor for Arc<T> {
    async fn create(&self, args: &OpCreate) -> Result<()> {
        self.as_ref().create(args).await
    }
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        self.as_ref().read(args).await
    }
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        self.as_ref().write(args).await
    }
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        self.as_ref().stat(args).await
    }
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        self.as_ref().delete(args).await
    }
    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        self.as_ref().list(args).await
    }
}
