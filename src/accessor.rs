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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::object::BoxedObjectStream;
use crate::object::Metadata;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::BoxedAsyncReader;

#[async_trait]
pub trait Accessor: Send + Sync + Debug {
    /// Read data from the underlying storage into input writer.
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        let _ = args;
        unimplemented!()
    }
    /// Write data from input reader to the underlying storage.
    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let (_, _) = (r, args);
        unimplemented!()
    }
    /// Invoke the `stat` operation on the specified path.
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

    fn metrics(&self) -> &AccessorMetrics;
}

/// All functions in `Accessor` only requires `&self`, so it's safe to implement
/// `Accessor` for `Arc<dyn Accessor>`.
#[async_trait]
impl<T: Accessor> Accessor for Arc<T> {
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        self.as_ref().read(args).await
    }
    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        self.as_ref().write(r, args).await
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

    fn metrics(&self) -> &AccessorMetrics {
        self.as_ref().metrics()
    }
}

#[derive(Debug, Default)]
pub struct AccessorMetrics {
    pub read_count: AtomicU64,
    pub read_bytes: AtomicU64,
    pub write_count: AtomicU64,
    pub write_bytes: AtomicU64,

    pub seek_count: AtomicU64,
    pub stat_count: AtomicU64,
    pub delete_count: AtomicU64,
    pub list_count: AtomicU64,
}

impl AccessorMetrics {
    pub fn incr_read(&self, read_bytes: u64) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(read_bytes, Ordering::Relaxed);
    }

    pub fn incr_write(&self, write_bytes: u64) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(write_bytes, Ordering::Relaxed);
    }

    pub fn incr_seek(&self) {
        self.seek_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_stat(&self) {
        self.stat_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_delete(&self) {
        self.delete_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_list(&self) {
        self.list_count.fetch_add(1, Ordering::Relaxed);
    }
}
