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

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Result;
use std::io::SeekFrom;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use madsim::net::Endpoint;
use madsim::net::Payload;

use crate::ops::*;
use crate::raw::oio;
use crate::raw::oio::Entry;
use crate::raw::*;
use crate::*;

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
/// use std::time::Duration;
///
/// use madsim::net::NetSim;
/// use madsim::runtime::Handle;
/// use madsim::time::sleep;
/// use opendal::layers::MadsimLayer;
/// use opendal::layers::MadsimServer;
/// use opendal::services;
/// use opendal::Operator;
///
/// #[cfg(madsim)]
/// #[madsim::test]
/// async fn deterministic_simulation_test() {
///     let handle = Handle::current();
///     let ip1 = "10.0.0.1".parse().unwrap();
///     let ip2 = "10.0.0.2".parse().unwrap();
///     let server_addr = "10.0.0.1:2379".parse().unwrap();
///     let server = handle.create_node().name("server").ip(ip1).build();
///     let client = handle.create_node().name("client").ip(ip2).build();
///
///     server.spawn(async move {
///         SimServer::serve(server_addr).await.unwrap();
///     });
///     sleep(Duration::from_secs(1)).await;
///
///     let handle = client.spawn(async move {
///         let mut builder = services::Fs::default();
///         builder.root(".");
///         let op = Operator::new(builder)
///             .unwrap()
///             .layer(MadsimLayer::new(server_addr))
///             .finish();
///
///         let path = "hello.txt";
///         let data = "Hello, World!";
///         op.write(path, data).await.unwrap();
///         assert_eq!(data.as_bytes(), op.read(path).await.unwrap());
///     });
///     handle.await.unwrap();
/// }
/// ```
/// To enable logging output, please set `RUSTFLAGS="--cfg madsim"`:
/// ```shell
/// RUSTFLAGS="--cfg madsim" cargo test
/// ```
#[derive(Debug, Copy, Clone)]
pub struct MadsimLayer {
    addr: SocketAddr,
}

impl MadsimLayer {
    pub fn new(endpoint: &str) -> Self {
        Self {
            addr: endpoint.parse().unwrap(),
        }
    }
}

impl<A: Accessor> Layer<A> for MadsimLayer {
    type LayeredAccessor = MadsimAccessor;

    fn layer(&self, _: A) -> Self::LayeredAccessor {
        MadsimAccessor { addr: self.addr }
    }
}

#[derive(Debug)]
pub struct MadsimAccessor {
    addr: SocketAddr,
}

#[async_trait]
impl LayeredAccessor for MadsimAccessor {
    type Inner = ();
    type Reader = MadsimReader;
    type BlockingReader = ();
    type Writer = MadsimWriter;
    type BlockingWriter = ();
    type Pager = MadsimPager;
    type BlockingPager = ();

    fn inner(&self) -> &Self::Inner {
        &()
    }

    fn metadata(&self) -> AccessorInfo {
        let mut info = AccessorInfo::default();
        info.set_name("madsim");
        info.set_capabilities(AccessorCapability::Read | AccessorCapability::Write);
        info
    }

    async fn read(&self, path: &str, args: OpRead) -> crate::Result<(RpRead, Self::Reader)> {
        let req = Request::Read(path.to_string(), args);
        let ep = Endpoint::connect(self.addr)
            .await
            .expect("fail to connect to sim server");
        let (tx, mut rx) = ep
            .connect1(self.addr)
            .await
            .expect("fail to connect1 to sim server");
        tx.send(Box::new(req))
            .await
            .expect("fail to send request to sim server");
        let resp = rx
            .recv()
            .await
            .expect("fail to recv response from sim server");
        let resp = resp
            .downcast::<ReadResponse>()
            .expect("fail to downcast response to ReadResponse");
        let content_length = resp.data.as_ref().map(|b| b.len()).unwrap_or(0);
        Ok((
            RpRead::new(content_length as u64),
            MadsimReader { data: resp.data },
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> crate::Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            MadsimWriter {
                path: path.to_string(),
                args,
                addr: self.addr,
            },
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> crate::Result<(RpList, Self::Pager)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        ))
    }

    async fn scan(&self, path: &str, args: OpScan) -> crate::Result<(RpScan, Self::Pager)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        ))
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> crate::Result<(RpRead, Self::BlockingReader)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will not be supported in MadsimLayer",
        ))
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> crate::Result<(RpWrite, Self::BlockingWriter)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will not be supported in MadsimLayer",
        ))
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> crate::Result<(RpList, Self::BlockingPager)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will not be supported in MadsimLayer",
        ))
    }

    fn blocking_scan(
        &self,
        path: &str,
        args: OpScan,
    ) -> crate::Result<(RpScan, Self::BlockingPager)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will not be supported in MadsimLayer",
        ))
    }
}

pub struct MadsimReader {
    data: Option<Bytes>,
}

impl oio::Read for MadsimReader {
    fn poll_read(&mut self, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<crate::Result<usize>> {
        if let Some(ref data) = self.data {
            let len = data.len();
            buf[..len].copy_from_slice(data);
            Poll::Ready(Ok(len))
        } else {
            Poll::Ready(Ok(0))
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<crate::Result<u64>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        )))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<crate::Result<Bytes>>> {
        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        ))))
    }
}

pub struct MadsimWriter {
    path: String,
    args: OpWrite,
    addr: SocketAddr,
}

#[async_trait]
impl oio::Write for MadsimWriter {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        let req = Request::Write(self.path.to_string(), bs);
        let ep = Endpoint::connect(self.addr).await?;
        let (tx, mut rx) = ep.connect1(self.addr).await?;
        tx.send(Box::new(req)).await?;
        rx.recv().await?;
        Ok(())
    }

    async fn append(&mut self, bs: Bytes) -> crate::Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        ))
    }

    async fn abort(&mut self) -> crate::Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        ))
    }

    async fn close(&mut self) -> crate::Result<()> {
        Ok(())
    }
}

pub struct MadsimPager {}

#[async_trait]
impl oio::Page for MadsimPager {
    async fn next(&mut self) -> crate::Result<Option<Vec<Entry>>> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        ))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::new(ErrorKind::Unexpected, "madsim error")
    }
}

/// A simulated server.This an experimental feature, docs are not ready yet.
#[derive(Default, Clone)]
pub struct MadsimServer;

impl MadsimServer {
    pub async fn serve(addr: SocketAddr) -> Result<()> {
        let ep = Endpoint::bind(addr).await?;
        let service = Arc::new(SimService::default());
        loop {
            let (tx, mut rx, _) = ep.accept1().await?;
            let service = service.clone();
            madsim::task::spawn(async move {
                let request = *rx
                    .recv()
                    .await?
                    .downcast::<Request>()
                    .expect("invalid request");
                let response = match request {
                    Request::Read(path, args) => {
                        Box::new(service.read(&path, args).await) as Payload
                    }
                    Request::Write(path, args) => {
                        Box::new(service.write(&path, args).await) as Payload
                    }
                };
                tx.send(response).await?;
                Ok(()) as Result<()>
            });
        }
    }
}

enum Request {
    Read(String, OpRead),
    Write(String, Bytes),
}

#[derive(Default)]
pub struct SimService {
    inner: Mutex<HashMap<String, Bytes>>,
}

impl SimService {
    async fn read(&self, path: &str, args: OpRead) -> ReadResponse {
        let inner = self.inner.lock().unwrap();
        let data = inner.get(path);
        ReadResponse {
            data: data.cloned(),
        }
    }

    async fn write(&self, path: &str, data: Bytes) -> WriteResponse {
        let mut inner = self.inner.lock().unwrap();
        inner.insert(path.to_string(), data);
        WriteResponse {}
    }
}

struct ReadResponse {
    data: Option<Bytes>,
}

struct WriteResponse {}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use madsim::runtime::Handle;
    use madsim::time::sleep;

    use super::*;
    use crate::services;
    use crate::Operator;

    #[madsim::test]
    async fn test_madsim_layer() {
        let handle = Handle::current();
        let ip1 = "10.0.0.1".parse().unwrap();
        let ip2 = "10.0.0.2".parse().unwrap();
        let server_addr = "10.0.0.1:2379";
        let server = handle.create_node().name("server").ip(ip1).build();
        let client = handle.create_node().name("client").ip(ip2).build();

        server.spawn(async move {
            MadsimServer::serve(server_addr.parse().unwrap())
                .await
                .unwrap();
        });
        sleep(Duration::from_secs(1)).await;

        let handle = client.spawn(async move {
            let mut builder = services::Fs::default();
            builder.root(".");
            let op = Operator::new(builder)
                .unwrap()
                .layer(MadsimLayer::new(server_addr))
                .finish();

            let path = "hello.txt";
            let data = "Hello, World!";
            op.write(path, data).await.unwrap();
            assert_eq!(data.as_bytes(), op.read(path).await.unwrap());
        });
        handle.await.unwrap();
    }
}
