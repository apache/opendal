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

use async_trait::async_trait;
use await_tree::InstrumentAwait;

use crate::raw::*;
use crate::*;

/// Add a Instrument await-tree for actor-based applications to the underlying services.
///
/// # AwaitTree
///
/// await-tree allows developers to dump this execution tree at runtime,
/// with the span of each Future annotated by instrument_await.
/// Read more about [await-tree](https://docs.rs/await-tree/latest/await_tree/)
///
/// # Examples
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::layers::AwaitTreeLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(AwaitTreeLayer::new())
///     .finish();
/// ```
#[derive(Clone, Default)]
pub struct AwaitTreeLayer {}

impl AwaitTreeLayer {
    /// Create a new `AwaitTreeLayer`.
    pub fn new() -> Self {
        Self {}
    }
}

impl<A: Accessor> Layer<A> for AwaitTreeLayer {
    type LayeredAccessor = AwaitTreeAccessor<A>;

    fn layer(&self, accessor: A) -> Self::LayeredAccessor {
        AwaitTreeAccessor { inner: accessor }
    }
}

#[derive(Debug, Clone)]
pub struct AwaitTreeAccessor<A: Accessor> {
    inner: A,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for AwaitTreeAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .instrument_await(format!("opendal::{}", Operation::Read))
            .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .instrument_await(format!("opendal::{}", Operation::Write))
            .await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner()
            .copy(from, to, args)
            .instrument_await(format!("opendal::{}", Operation::Copy))
            .await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner()
            .rename(from, to, args)
            .instrument_await(format!("opendal::{}", Operation::Rename))
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner
            .stat(path, args)
            .instrument_await(format!("opendal::{}", Operation::Stat))
            .await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner
            .delete(path, args)
            .instrument_await(format!("opendal::{}", Operation::Delete))
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner
            .list(path, args)
            .instrument_await(format!("opendal::{}", Operation::List))
            .await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner
            .presign(path, args)
            .instrument_await(format!("opendal::{}", Operation::Presign))
            .await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner
            .batch(args)
            .instrument_await(format!("opendal::{}", Operation::Batch))
            .await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }
}
