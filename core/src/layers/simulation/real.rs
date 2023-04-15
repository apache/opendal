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

use crate::ops::{OpList, OpRead, OpScan, OpWrite};
use crate::raw::{Accessor, Layer, LayeredAccessor, RpList, RpRead, RpScan, RpWrite};
use async_trait::async_trait;

#[derive(Debug, Copy, Clone, Default)]
pub struct MadsimLayer;

impl<A: Accessor> Layer<A> for MadsimLayer {
    type LayeredAccessor = MadsimAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        MadsimAccessor { inner }
    }
}

#[derive(Debug)]
pub struct MadsimAccessor<A: Accessor> {
    inner: A,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for MadsimAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> crate::Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> crate::Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> crate::Result<(RpList, Self::Pager)> {
        self.inner.list(path, args).await
    }

    async fn scan(&self, path: &str, args: OpScan) -> crate::Result<(RpScan, Self::Pager)> {
        self.inner.scan(path, args).await
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> crate::Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> crate::Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> crate::Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }

    fn blocking_scan(
        &self,
        path: &str,
        args: OpScan,
    ) -> crate::Result<(RpScan, Self::BlockingPager)> {
        self.inner.blocking_scan(path, args)
    }
}
