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

use aws_config::load_from_env;
use std::future::Future;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::{AsyncRead, AsyncSeek};

use crate::error::Result;
use crate::ops::{OpRandomRead, OpSequentialRead, OpWrite};
use crate::{Accessor, Object};

pub type BoxedAsyncRead = Box<dyn AsyncRead + Unpin + Send>;
pub type BoxedAsyncReadSeek = Box<dyn AsyncReadSeek + Unpin + Send>;
pub trait AsyncReadSeek: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncReadSeek for T {}

pub struct SequentialReader {
    acc: Arc<dyn Accessor>,
    path: String,
    offset: Option<u64>,
    size: Option<u64>,

    state: SequentialState,
}

enum SequentialState {
    Idle,
    Sending(BoxFuture<'static, Result<BoxedAsyncRead>>),
    Reading(BoxedAsyncRead),
}

impl SequentialReader {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
            offset: None,
            size: None,
            state: SequentialState::Idle,
        }
    }
    pub fn offset(&mut self, offset: u64) -> &mut Self {
        assert!(matches!(self.state, SequentialState::Idle));

        self.offset = Some(offset);
        self
    }
    pub fn size(&mut self, size: u64) -> &mut Self {
        assert!(matches!(self.state, SequentialState::Idle));

        self.size = Some(size);
        self
    }
}

impl AsyncRead for SequentialReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            match &mut self.state {
                SequentialState::Idle => {
                    let acc = self.acc.clone();
                    let op = OpSequentialRead {
                        path: self.path.clone(),
                        offset: self.offset,
                        size: self.size,
                    };
                    let future = async move { acc.sequential_read(&op).await };

                    self.state = SequentialState::Sending(Box::pin(future));
                }
                SequentialState::Sending(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(r)) => self.state = SequentialState::Reading(r),
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)))
                    }
                    Poll::Pending => continue,
                },
                SequentialState::Reading(r) => return Pin::new(r).poll_read(cx, buf),
            }
        }
    }
}

pub struct RandomReader {
    object: Object,

    state: RandomState,
}

enum RandomState {
    Idle,
    Sending(BoxFuture<'static, Result<BoxedAsyncReadSeek>>),
    Reading(BoxedAsyncReadSeek),
}

impl RandomReader {
    pub fn new(o: &Object) -> Self {
        Self {
            object: o.clone(),
            state: RandomState::Idle,
        }
    }
}

impl AsyncRead for RandomReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            match &mut self.state {
                RandomState::Idle => {
                    let o = self.object.clone();
                    let acc = o.accessor();
                    let op = OpRandomRead {
                        object: self.object.clone(),
                    };
                    let future = async move { acc.random_read(&op).await };

                    self.state = RandomState::Sending(Box::pin(future));
                }
                RandomState::Sending(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(r)) => self.state = RandomState::Reading(r),
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)))
                    }
                    Poll::Pending => continue,
                },
                RandomState::Reading(r) => return Pin::new(r).poll_read(cx, buf),
            }
        }
    }
}

impl AsyncSeek for RandomReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        loop {
            match &mut self.state {
                RandomState::Idle => {
                    let o = self.object.clone();
                    let acc = o.accessor();
                    let op = OpRandomRead {
                        object: self.object.clone(),
                    };
                    let future = async move { acc.random_read(&op).await };

                    self.state = RandomState::Sending(Box::pin(future));
                }
                RandomState::Sending(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(r)) => self.state = RandomState::Reading(r),
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)))
                    }
                    Poll::Pending => continue,
                },
                RandomState::Reading(r) => return Pin::new(r).poll_seek(cx, pos),
            }
        }
    }
}
