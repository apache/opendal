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

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::Future;
use futures::Stream;

use crate::error::Result;
use crate::raw::*;
use crate::ObjectMetadata;
use crate::OpRead;
use crate::OpStat;
use parking_lot::Mutex;

/// ObjectReader
pub struct ObjectReader {
    acc: Arc<dyn Accessor>,
    path: String,
    meta: Arc<Mutex<ObjectMetadata>>,
    op: OpRead,

    state: State,
    total_size: Option<u64>,
    current_pos: u64,
}

impl ObjectReader {
    /// Create a new object reader.
    pub(crate) fn new(
        acc: Arc<dyn Accessor>,
        path: &str,
        meta: Arc<Mutex<ObjectMetadata>>,
        op: OpRead,
    ) -> Self {
        let total_size = { meta.lock().content_length_raw() };

        ObjectReader {
            acc,
            path: path.to_string(),
            meta,
            op,

            state: State::Idle,
            total_size,
            current_pos: 0,
        }
    }

    fn current_offset(&self) -> u64 {
        self.op.range().offset().unwrap_or_default() + self.current_pos
    }

    fn current_size(&self) -> Option<u64> {
        match self.op.range().size() {
            Some(size) => Some(size - self.current_pos),
            None => self.total_size.map(|total_size| {
                total_size - self.op.range().offset().unwrap_or_default() - self.current_pos
            }),
        }
    }
}

enum State {
    Idle,
    Stating(BoxFuture<'static, Result<RpStat>>),
    Sending(BoxFuture<'static, Result<(RpRead, OutputBytesReader)>>),
    Reading(OutputBytesReader),
}

/// State will only changed under &mut.
unsafe impl Sync for State {}

impl OutputBytesRead for ObjectReader {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match &mut self.state {
            State::Idle => {
                let acc = self.acc.clone();
                let path = self.path.clone();
                let op = OpRead::default().with_range(BytesRange::new(
                    Some(self.current_offset()),
                    self.current_size(),
                ));

                // TODO: use returning content bytes range to fill size.
                let future = async move { acc.read(&path, op).await };

                self.state = State::Sending(Box::pin(future));
                self.poll_read(cx, buf)
            }
            State::Sending(future) => {
                let (_, mut r) = ready!(Pin::new(future).poll(cx))?;
                if !r.is_streamable() {
                    // TODO: make this configurable.
                    r = Box::new(into_stream(r, 1024 * 1024));
                }
                // TODO: Make sure returning reader is streamable.
                self.state = State::Reading(r);
                self.poll_read(cx, buf)
            }
            State::Reading(r) => match ready!(Pin::new(r).poll_read(cx, buf)) {
                Ok(n) => {
                    self.current_pos += n as u64;
                    Poll::Ready(Ok(n))
                }
                Err(e) => Poll::Ready(Err(e)),
            },
            State::Stating(fut) => {
                let rp = ready!(Pin::new(fut).poll(cx))?;
                *self.meta.lock() = rp.into_metadata();
                self.state = State::Idle;
                self.poll_read(cx, buf)
            }
        }
    }

    fn is_seekable(&mut self) -> bool {
        true
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<io::Result<u64>> {
        match &mut self.state {
            State::Idle => {
                let cur = self.current_pos as i64;
                let cur = match pos {
                    io::SeekFrom::Start(off) => off as i64,
                    io::SeekFrom::Current(off) => cur + off,
                    io::SeekFrom::End(off) => {
                        // Stat the object to get it's content-length.
                        if self.total_size.is_none() {
                            let acc = self.acc.clone();
                            let path = self.path.clone();

                            let future = async move { acc.stat(&path, OpStat::new()).await };

                            self.state = State::Stating(Box::pin(future));
                            return self.poll_seek(cx, pos);
                        }

                        let total_size = self.total_size.expect("must have valid total_size");

                        total_size as i64 + off
                    }
                };

                self.current_pos = cur as u64;
                Poll::Ready(Ok(self.current_pos))
            }
            State::Sending(future) => {
                let (_, mut r) = ready!(Pin::new(future).poll(cx))?;
                if !r.is_streamable() {
                    // TODO: make this configurable.
                    r = Box::new(into_stream(r, 1024 * 1024));
                }
                // TODO: Make sure returning reader is streamable.
                self.state = State::Reading(r);
                self.poll_seek(cx, pos)
            }
            State::Reading(r) => {
                if r.is_seekable() {
                    r.poll_seek(cx, pos)
                } else {
                    if self.current_size().is_none() && self.total_size.is_none() {
                        let acc = self.acc.clone();
                        let path = self.path.clone();

                        let future = async move { acc.stat(&path, OpStat::new()).await };
                        self.state = State::Stating(Box::pin(future));
                    };

                    self.poll_seek(cx, pos)
                }
            }
            State::Stating(fut) => {
                let rp = ready!(Pin::new(fut).poll(cx))?;
                let meta = rp.into_metadata();
                self.total_size = Some(meta.content_length());
                *self.meta.lock() = meta;
                self.state = State::Idle;
                self.poll_seek(cx, pos)
            }
        }
    }

    fn is_streamable(&mut self) -> bool {
        true
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        match &mut self.state {
            State::Idle => {
                let acc = self.acc.clone();
                let path = self.path.clone();
                let op = OpRead::default().with_range(BytesRange::new(
                    Some(self.current_offset()),
                    self.current_size(),
                ));

                let future = async move { acc.read(&path, op).await };

                self.state = State::Sending(Box::pin(future));
                self.poll_next(cx)
            }
            State::Sending(future) => {
                let (_, r) = ready!(Pin::new(future).poll(cx))?;
                self.state = State::Reading(r);
                self.poll_next(cx)
            }
            State::Reading(r) => match ready!(Pin::new(r).poll_next(cx)) {
                Some(Ok(bs)) => {
                    self.current_pos += bs.len() as u64;
                    Poll::Ready(Some(Ok(bs)))
                }
                Some(Err(e)) => Poll::Ready(Some(Err(e))),
                None => Poll::Ready(None),
            },
            State::Stating(fut) => {
                let rp = ready!(Pin::new(fut).poll(cx))?;
                let meta = rp.into_metadata();
                self.total_size = Some(meta.content_length());
                *self.meta.lock() = meta;
                self.state = State::Idle;
                self.poll_next(cx)
            }
        }
    }
}

impl AsyncRead for ObjectReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this: &mut dyn OutputBytesRead = &mut (*self);
        this.poll_read(cx, buf)
    }
}

impl AsyncSeek for ObjectReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this: &mut dyn OutputBytesRead = &mut (*self);
        this.poll_seek(cx, pos)
    }
}

impl Stream for ObjectReader {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this: &mut dyn OutputBytesRead = &mut (*self);
        this.poll_next(cx)
    }
}
