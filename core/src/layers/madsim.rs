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

// This module requires to be work under `cfg(madsim)` so we will allow
// dead code and unused variables.
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(dead_code)]

use std::any::Any;
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::io::Result;
use std::io::SeekFrom;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
#[cfg(madsim)]
use madsim::net::Endpoint;
#[cfg(madsim)]
use madsim::net::Payload;

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
    #[cfg(madsim)]
    addr: SocketAddr,
}

impl MadsimLayer {
    /// Create new madsim layer
    pub fn new(endpoint: &str) -> Self {
        #[cfg(madsim)]
        {
            Self {
                addr: endpoint.parse().unwrap(),
            }
        }
        #[cfg(not(madsim))]
        {
            unreachable!("madsim is not enabled")
        }
    }
}

impl<A: Accessor> Layer<A> for MadsimLayer {
    type LayeredAccessor = MadsimAccessor;

    fn layer(&self, _: A) -> Self::LayeredAccessor {
        #[cfg(madsim)]
        {
            MadsimAccessor { addr: self.addr }
        }
        #[cfg(not(madsim))]
        {
            unreachable!("madsim is not enabled")
        }
    }
}

#[derive(Debug)]
pub struct MadsimAccessor {
    #[cfg(madsim)]
    addr: SocketAddr,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl LayeredAccessor for MadsimAccessor {
    type Inner = ();
    type Reader = MadsimReader;
    type BlockingReader = ();
    type Writer = MadsimWriter;
    type BlockingWriter = ();
    type Lister = MadsimLister;
    type BlockingLister = ();

    fn inner(&self) -> &Self::Inner {
        &()
    }

    fn metadata(&self) -> AccessorInfo {
        let mut info = AccessorInfo::default();
        info.set_name("madsim");

        info.set_native_capability(Capability {
            read: true,
            write: true,
            ..Default::default()
        });

        info
    }

    async fn read(&self, path: &str, args: OpRead) -> crate::Result<(RpRead, Self::Reader)> {
        #[cfg(madsim)]
        {
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
            Ok((RpRead::new(), MadsimReader { data: resp.data }))
        }
        #[cfg(not(madsim))]
        {
            unreachable!("madsim is not enabled")
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> crate::Result<(RpWrite, Self::Writer)> {
        #[cfg(madsim)]
        {
            Ok((
                RpWrite::default(),
                MadsimWriter {
                    path: path.to_string(),
                    args,
                    addr: self.addr,
                },
            ))
        }
        #[cfg(not(madsim))]
        {
            unreachable!("madsim is not enabled")
        }
    }

    async fn list(&self, path: &str, args: OpList) -> crate::Result<(RpList, Self::Lister)> {
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
    ) -> crate::Result<(RpList, Self::BlockingLister)> {
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
    async fn read(&mut self, size: usize) -> crate::Result<Bytes> {
        if let Some(ref data) = self.data {
            let size = min(size, data.len());
            Ok(data.clone().split_to(size))
        } else {
            Ok(Bytes::new())
        }
    }

    async fn seek(&mut self, _: SeekFrom) -> crate::Result<u64> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        ))
    }
}

pub struct MadsimWriter {
    #[cfg(madsim)]
    path: String,
    #[cfg(madsim)]
    args: OpWrite,
    #[cfg(madsim)]
    addr: SocketAddr,
}

impl oio::Write for MadsimWriter {
    async fn write(&mut self, bs: Bytes) -> crate::Result<usize> {
        #[cfg(madsim)]
        {
            let req = Request::Write(self.path.to_string(), bs);
            let ep = Endpoint::bind(self.addr).await?;
            let (tx, mut rx) = ep.connect1(self.addr).await?;
            tx.send(Box::new(req)).await?;
            rx.recv().await?;
            Ok(())
        }
        #[cfg(not(madsim))]
        {
            unreachable!("madsim is not enabled")
        }
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

pub struct MadsimLister {}

impl oio::List for MadsimLister {
    async fn next(&mut self) -> crate::Result<Option<oio::Entry>> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "will be supported in the future",
        ))
    }
}

/// A simulated server.This an experimental feature, docs are not ready yet.
#[derive(Default, Clone)]
pub struct MadsimServer;

impl MadsimServer {
    /// Start serving as madsim server.
    pub async fn serve(addr: SocketAddr) -> Result<()> {
        #[cfg(madsim)]
        {
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
        #[cfg(not(madsim))]
        {
            unreachable!("madsim is not enabled")
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
#[cfg(madsim)]
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
