use std::collections::HashMap;
use crate::ops::{OpList, OpRead, OpScan, OpWrite};
use crate::raw::oio::Entry;
use crate::raw::{oio, Accessor, Layer, LayeredAccessor, RpList, RpRead, RpScan, RpWrite};
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;
use std::io::SeekFrom;
use std::task::{Context, Poll};
use madsim::net::Endpoint;
use std::io::Result;
use std::net::SocketAddr;
use std::sync::Arc;

const SIM_SERVER_ADDR: &str = "10.0.0.1:2379";

#[derive(Debug, Copy, Clone, Default)]
pub struct MadsimLayer;

impl<A: Accessor> Layer<A> for MadsimLayer {
    type LayeredAccessor = MadsimAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let addr = SIM_SERVER_ADDR.parse().unwrap();

        let runtime = madsim::runtime::Runtime::new();
        std::thread::spawn(move || {
            runtime.block_on(async {
                SimServer::serve(addr).await.unwrap();
                Endpoint::connect(addr).await.unwrap();
            })
        });
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
    type Reader = MadsimReader<A::Reader>;
    type BlockingReader = MadsimReader<A::BlockingReader>;
    type Writer = MadsimWriter<A::Writer>;
    type BlockingWriter = MadsimWriter<A::BlockingWriter>;
    type Pager = MadsimPager<A::Pager>;
    type BlockingPager = MadsimPager<A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> crate::Result<(RpRead, Self::Reader)> {
        todo!()
    }

    async fn write(&self, path: &str, args: OpWrite) -> crate::Result<(RpWrite, Self::Writer)> {
        todo!()
    }

    async fn list(&self, path: &str, args: OpList) -> crate::Result<(RpList, Self::Pager)> {
        todo!()
    }

    async fn scan(&self, path: &str, args: OpScan) -> crate::Result<(RpScan, Self::Pager)> {
        todo!()
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> crate::Result<(RpRead, Self::BlockingReader)> {
        todo!()
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> crate::Result<(RpWrite, Self::BlockingWriter)> {
        todo!()
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> crate::Result<(RpList, Self::BlockingPager)> {
        todo!()
    }

    fn blocking_scan(
        &self,
        path: &str,
        args: OpScan,
    ) -> crate::Result<(RpScan, Self::BlockingPager)> {
        todo!()
    }
}

pub struct MadsimReader<R> {
    inner: R,
}

impl<R: oio::Read> oio::Read for MadsimReader<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<crate::Result<usize>> {
        todo!()
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<crate::Result<u64>> {
        todo!()
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<crate::Result<Bytes>>> {
        todo!()
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for MadsimReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> crate::Result<usize> {
        todo!()
    }

    fn seek(&mut self, pos: SeekFrom) -> crate::Result<u64> {
        todo!()
    }

    fn next(&mut self) -> Option<crate::Result<Bytes>> {
        todo!()
    }
}

pub struct MadsimWriter<W> {
    inner: W,
}

impl<W: oio::BlockingWrite> oio::BlockingWrite for MadsimWriter<W> {
    fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        todo!()
    }

    fn append(&mut self, bs: Bytes) -> crate::Result<()> {
        todo!()
    }

    fn close(&mut self) -> crate::Result<()> {
        todo!()
    }
}

#[async_trait]
impl<W: oio::Write> oio::Write for MadsimWriter<W> {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        todo!()
    }

    async fn append(&mut self, bs: Bytes) -> crate::Result<()> {
        todo!()
    }

    async fn close(&mut self) -> crate::Result<()> {
        todo!()
    }
}

pub struct MadsimPager<P> {
    inner: P,
}

#[async_trait]
impl<P: oio::Page> oio::Page for MadsimPager<P> {
    async fn next(&mut self) -> crate::Result<Option<Vec<Entry>>> {
        todo!()
    }
}

impl<P: oio::BlockingPage> oio::BlockingPage for MadsimPager<P> {
    fn next(&mut self) -> crate::Result<Option<Vec<Entry>>> {
        todo!()
    }
}


/// A simulated server.
#[derive(Default, Clone)]
pub struct SimServer;

impl SimServer {
    pub async fn serve(addr: SocketAddr) -> Result<()> {
        let ep = Endpoint::bind(addr).await?;
        let service = Arc::new(SimService::default());
        loop {
            let (tx, mut rx, _) = ep.accept1().await?;
            let service = service.clone();
            madsim::task::spawn(async move {
                let request = *rx.recv().await?.downcast::<Request>().unwrap();
                let response = match request {
                    Request::Read(path, args) => Box::new(SimServerResponse::Read(service.read(&path, args).await)),
                    Request::Write(path, args) => Box::new(SimServerResponse::Write(service.write(&path, args).await)),
                };
                tx.send(response).await?;
                Ok(()) as Result<()>
            });
        }
    }
}

enum Request {
    Read(String, OpRead),
    Write(String, OpWrite),
}


#[derive(Default)]
pub struct SimService {
    data: HashMap<String, Vec<u8>>,
}

impl SimService {
    async fn read(&self, path: &str, args: OpRead) -> ReadResponse {
        todo!()
    }

    async fn write(&self, path: &str, args: OpWrite) -> WriteResponse {
        todo!()
    }
}

struct ReadResponse {
    data: Option<Vec<u8>>,
}

struct WriteResponse {}

enum SimServerResponse {
    Read(ReadResponse),
    Write(WriteResponse),
}