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

use std::future::Future;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::AsyncRead;
use futures::AsyncSeek;

use crate::error::Result;
use crate::ops::OpRandomRead;
use crate::ops::OpWrite;
use crate::Accessor;

pub type BoxedAsyncRead = Box<dyn AsyncRead + Unpin + Send>;
pub type BoxedAsyncReadSeek = Box<dyn AsyncReadSeek + Unpin + Send>;
pub trait AsyncReadSeek: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncReadSeek for T {}

pub struct Reader {
    acc: Arc<dyn Accessor>,
    path: String,

    total_size: Option<u64>,

    state: ReadState,
}

enum ReadState {
    Idle,
    Sending(BoxFuture<'static, Result<BoxedAsyncReadSeek>>),
    Reading(BoxedAsyncReadSeek),
}

impl Reader {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
            total_size: None,
            state: ReadState::Idle,
        }
    }
    pub fn total_size(&mut self, size: u64) -> &mut Self {
        self.total_size = Some(size);
        self
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            match &mut self.state {
                ReadState::Idle => {
                    let acc = self.acc.clone();
                    let op = OpRandomRead {
                        path: self.path.clone(),
                        total: self.total_size,
                    };
                    let future = async move { acc.random_read(&op).await };

                    self.state = ReadState::Sending(Box::pin(future));
                }
                ReadState::Sending(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(r)) => self.state = ReadState::Reading(r),
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)))
                    }
                    Poll::Pending => continue,
                },
                ReadState::Reading(r) => return Pin::new(r).poll_read(cx, buf),
            }
        }
    }
}

impl AsyncSeek for Reader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        loop {
            match &mut self.state {
                ReadState::Idle => {
                    let acc = self.acc.clone();
                    let op = OpRandomRead {
                        path: self.path.clone(),
                        total: self.total_size,
                    };
                    let future = async move { acc.random_read(&op).await };

                    self.state = ReadState::Sending(Box::pin(future));
                }
                ReadState::Sending(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(r)) => self.state = ReadState::Reading(r),
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)))
                    }
                    Poll::Pending => continue,
                },
                ReadState::Reading(r) => return Pin::new(r).poll_seek(cx, pos),
            }
        }
    }
}

// TODO: maybe we can implement `AsyncWrite` for `Writer`
pub struct Writer {
    acc: Arc<dyn Accessor>,
    path: String,
}

impl Writer {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
        }
    }

    pub async fn write_bytes(self, bs: Vec<u8>) -> Result<usize> {
        let op = &OpWrite {
            path: self.path.clone(),
            size: bs.len() as u64,
        };
        let r = Box::new(futures::io::Cursor::new(bs));

        self.acc.write(r, op).await
    }
    pub async fn write_reader(self, r: BoxedAsyncRead, size: u64) -> Result<usize> {
        let op = &OpWrite {
            path: self.path.clone(),
            size,
        };

        self.acc.write(r, op).await
    }
}
