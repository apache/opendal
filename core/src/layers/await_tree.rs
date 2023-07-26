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

use crate::raw::*;
use crate::*;

use async_trait::async_trait;

use await_tree::InstrumentAwait;

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
/// ```
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

#[async_trait]
impl<A: Accessor> LayeredAccessor for AwaitTreeAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Appender = A::Appender;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .instrument_await("opendal::read")
            .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .instrument_await("opendal::write")
            .await
    }

    async fn append(&self, path: &str, args: OpAppend) -> Result<(RpAppend, Self::Appender)> {
        self.inner
            .append(path, args)
            .instrument_await("opendal::append")
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner
            .list(path, args)
            .instrument_await("opendal::list")
            .await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }
}
