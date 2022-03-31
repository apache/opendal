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

use crate::error::Result;
use crate::io::{BytesSink, BytesStream};
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::BoxedObjectStream;
use crate::Metadata;

/// Underlying trait of all backends for implementors.
///
/// # Note
///
/// Only service implementor should care about this trait, users need to
/// use [`Operator`][crate::Operator] instead.
#[async_trait]
pub trait Accessor: Send + Sync + Debug {
    async fn read(&self, args: &OpRead) -> Result<BytesStream> {
        let _ = args;
        unimplemented!()
    }
    async fn write(&self, args: &OpWrite) -> Result<BytesSink> {
        let _ = args;
        unimplemented!()
    }
    /// Invoke the `stat` operation on the specified path.
    ///
    /// ## Behavior
    ///
    /// - `Stat` empty path means stat backend's root path.
    /// - `Stat` a path endswith "/" means stating a dir.
    ///   - On fs, an error could return if not a dir.
    ///   - On s3 alike backends, a dir object will return no matter it exist or not.
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let _ = args;
        unimplemented!()
    }
    /// `Delete` will invoke the `delete` operation.
    ///
    /// ## Behavior
    ///
    /// - `Delete` is an idempotent operation, it's safe to call `Delete` on the same path multiple times.
    /// - `Delete` will return `Ok(())` if the path is deleted successfully or not exist.
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let _ = args;
        unimplemented!()
    }

    async fn list(&self, args: &OpList) -> Result<BoxedObjectStream> {
        let _ = args;
        unimplemented!()
    }
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<dyn Accessor>`.
#[async_trait]
impl<T: Accessor> Accessor for Arc<T> {
    async fn read(&self, args: &OpRead) -> Result<BytesStream> {
        self.as_ref().read(args).await
    }
    async fn write(&self, args: &OpWrite) -> Result<BytesSink> {
        self.as_ref().write(args).await
    }
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        self.as_ref().stat(args).await
    }
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        self.as_ref().delete(args).await
    }
    async fn list(&self, args: &OpList) -> Result<BoxedObjectStream> {
        self.as_ref().list(args).await
    }
}
