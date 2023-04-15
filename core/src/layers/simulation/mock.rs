use crate::ops::{OpList, OpRead, OpScan, OpWrite};
use crate::raw::oio::Entry;
use crate::raw::{oio, Accessor, Layer, LayeredAccessor, RpList, RpRead, RpScan, RpWrite};
use crate::EntryMode;
use async_trait::async_trait;
use bytes::Bytes;
use madsim::net::Endpoint;
use madsim::net::Payload;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::Result;
use std::io::SeekFrom;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

#[derive(Debug, Copy, Clone)]
pub struct MadsimLayer {
    sim_server_socket: SocketAddr,
}

impl MadsimLayer {
    pub fn new(sim_server_socket: SocketAddr) -> Self {
        Self { sim_server_socket }
    }
}

impl<A: Accessor> Layer<A> for MadsimLayer {
    type LayeredAccessor = MadsimAccessor;

    fn layer(&self, _inner: A) -> Self::LayeredAccessor {
        MadsimAccessor {
            sim_server_socket: self.sim_server_socket,
        }
    }
}

#[derive(Debug)]
pub struct MadsimAccessor {
    sim_server_socket: SocketAddr,
}

#[async_trait]
impl LayeredAccessor for MadsimAccessor {
    type Inner = ();
    type Reader = MadsimReader;
    type BlockingReader = MadsimReader;
    type Writer = MadsimWriter;
    type BlockingWriter = MadsimWriter;
    type Pager = MadsimPager;
    type BlockingPager = MadsimPager;

    fn inner(&self) -> &Self::Inner {
        &()
    }

    async fn read(&self, path: &str, args: OpRead) -> crate::Result<(RpRead, Self::Reader)> {
        let req = Request::Read(path.to_string(), args);
        let ep = Endpoint::connect(self.sim_server_socket)
            .await
            .expect("fail to connect to sim server");
        let (tx, mut rx) = ep
            .connect1(self.sim_server_socket)
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
                sim_server_socket: self.sim_server_socket,
            },
        ))
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
        todo!()
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<crate::Result<Bytes>>> {
        todo!()
    }
}

impl oio::BlockingRead for MadsimReader {
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

pub struct MadsimWriter {
    path: String,
    args: OpWrite,
    sim_server_socket: SocketAddr,
}

impl oio::BlockingWrite for MadsimWriter {
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
impl oio::Write for MadsimWriter {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        let req = Request::Write(self.path.to_string(), bs);
        let ep = Endpoint::connect(self.sim_server_socket)
            .await
            .expect("fail to connect to sim server");
        let (tx, mut rx) = ep
            .connect1(self.sim_server_socket)
            .await
            .expect("fail to connect1 to sim server");
        tx.send(Box::new(req))
            .await
            .expect("fail to send request to sim server");
        rx.recv()
            .await
            .expect("fail to recv response from sim server");
        Ok(())
    }

    async fn append(&mut self, bs: Bytes) -> crate::Result<()> {
        todo!()
    }

    async fn close(&mut self) -> crate::Result<()> {
        Ok(())
    }
}

pub struct MadsimPager {}

#[async_trait]
impl oio::Page for MadsimPager {
    async fn next(&mut self) -> crate::Result<Option<Vec<Entry>>> {
        todo!()
    }
}

impl oio::BlockingPage for MadsimPager {
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
