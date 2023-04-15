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

/// Add deterministic simulation for async operations, powered by [`madsim`](https://docs.rs/madsim/latest/madsim/).
///
/// # Note
///
/// - blocking operations are not supported, as [`madsim`](https://docs.rs/madsim/latest/madsim/) is async only.
///
///
/// # Examples
///
/// ```
/// use opendal::Operator;
/// use opendal::services;
/// use opendal::layers::MadsimLayer;
/// use opendal::layers::SimServer;
/// use madsim::{net::NetSim, runtime::Handle, time::sleep};
/// use std::time::Duration;
///
/// #[cfg(madsim)]
/// #[madsim::test]
/// async fn deterministic_simulation_test(){
///     let handle = Handle::current();
///     let ip1 = "10.0.0.1".parse().unwrap();
///     let ip2 = "10.0.0.2".parse().unwrap();
///     let sim_server_socket = "10.0.0.1:2379".parse().unwrap();
///     let server = handle.create_node().name("server").ip(ip1).build();
///     let client = handle.create_node().name("client").ip(ip2).build();
///
///     server.spawn(async move {
///          SimServer::serve(sim_server_socket).await.unwrap();
///     });
///     sleep(Duration::from_secs(1)).await;
///
///     let handle = client.spawn(async move {
///     let mut builder = services::Fs::default();
///     builder.root(".");
///     let op = Operator::new(builder)
///         .unwrap()
///         .layer(MadsimLayer::new(sim_server_socket))
///         .finish();
///
///          let path = "hello.txt";
///          let data = "Hello, World!";
///          op.write(path, data).await.unwrap();
///          assert_eq!(data.as_bytes(), op.read(path).await.unwrap());
///      });
///      handle.await.unwrap();
/// }
/// ```
/// To enable logging output, please set `RUSTFLAGS="--cfg madsim"`:
/// ```shell
/// RUSTFLAGS="--cfg madsim" cargo test
/// ```
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
