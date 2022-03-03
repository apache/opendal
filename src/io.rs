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

use std::cmp::min;
use std::future::Future;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncSeek;

use crate::error::Result;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::Metadata;

/// BoxedAsyncReader is a boxed AsyncRead.
pub type BoxedAsyncReader = Box<dyn AsyncRead + Unpin + Send>;

/// Reader is used for reading data from underlying backend.
///
/// # Lazy Stat
///
/// We will fetch the object's content-length while the first time
/// caller try to seek with `SeekFrom::End(pos)` and the total size is `None`.
pub struct Reader {
    acc: Arc<dyn Accessor>,
    path: String,
    offset: Option<u64>,
    size: Option<u64>,
    mode: ReadMode,

    remaining: Option<u64>,
    pos: u64,
    state: ReadState,
}

/// ReadMode is used to indicate which read pattern we will take for reading.
///
/// - `ReadMode::Random`: read part of data, the default mode. In this mode, we
///   will try our best to fit the buffer size, only starting read while existing
///   data has been consumed.
/// - `ReadMode::Sequential`: read all data at once. In this mode, we will only start
///   reading once to consume the whole reader.
#[derive(Copy, Clone)]
pub enum ReadMode {
    Random,
    Sequential,
}

enum ReadState {
    Idle,
    Sending(BoxFuture<'static, Result<BoxedAsyncReader>>),
    Seeking(BoxFuture<'static, Result<Metadata>>),
    Reading(BoxedAsyncReader),
}

impl Reader {
    pub fn new(acc: Arc<dyn Accessor>, path: &str, offset: Option<u64>, size: Option<u64>) -> Self {
        Self {
            acc,
            path: path.to_string(),
            offset,
            size,
            mode: ReadMode::Random,

            remaining: None,
            pos: 0,
            state: ReadState::Idle,
        }
    }

    pub fn mode(mut self, mode: ReadMode) -> Self {
        self.mode = mode;
        self
    }

    fn current_offset(&self) -> u64 {
        self.offset.unwrap_or_default() + self.pos
    }

    /// Calculate read size based on current states:
    ///
    /// - If we don't know the reader size, return `None` to read all remaining data.
    /// - If the reader size is known
    ///   - Return `current_size` on `ReadMode::Sequential`
    ///   - Return the minimum of `current_size` and `buf_size` on `ReadMode::Random`
    fn read_size(&self, buf_size: usize) -> Option<u64> {
        match self.size {
            None => None,
            Some(v) => match self.mode {
                ReadMode::Sequential => Some(v - self.pos),
                ReadMode::Random => Some(min(v - self.pos, buf_size as u64)),
            },
        }
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.state {
            ReadState::Idle => {
                let acc = self.acc.clone();
                let size = self.read_size(buf.len());
                let op = OpRead {
                    path: self.path.to_string(),
                    offset: Some(self.current_offset()),
                    size,
                };

                let future = async move { acc.read(&op).await };

                self.remaining = size;
                self.state = ReadState::Sending(Box::pin(future));
                self.poll_read(cx, buf)
            }
            ReadState::Sending(future) => match ready!(Pin::new(future).poll(cx)) {
                Ok(r) => {
                    self.state = ReadState::Reading(r);
                    self.poll_read(cx, buf)
                }
                Err(e) => Poll::Ready(Err(io::Error::from(e))),
            },
            ReadState::Reading(r) => match ready!(Pin::new(r).poll_read(cx, buf)) {
                Ok(n) => {
                    self.pos += n as u64;
                    self.remaining = self.remaining.map(|v| v - n as u64);
                    // If all remaining data has been consumed, reset stat to Idle
                    // to start a new reading.
                    if let Some(0) = self.remaining {
                        self.state = ReadState::Idle;
                    }

                    Poll::Ready(Ok(n))
                }
                Err(e) => Poll::Ready(Err(e)),
            },
            _ => unreachable!("read while seeking is invalid"),
        }
    }
}

impl AsyncSeek for Reader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        if let ReadState::Seeking(future) = &mut self.state {
            match ready!(Pin::new(future).poll(cx)) {
                Ok(meta) => {
                    self.size = Some(meta.content_length() - self.offset.unwrap_or_default())
                }
                Err(e) => return Poll::Ready(Err(io::Error::from(e))),
            }
        }

        let cur = self.pos as i64;
        let cur = match pos {
            SeekFrom::Start(off) => off as i64,
            SeekFrom::Current(off) => cur + off,
            SeekFrom::End(off) => {
                // Stat the object to get it's content-length.
                if self.size.is_none() {
                    let acc = self.acc.clone();
                    let op = OpStat::new(&self.path);

                    let future = async move { acc.stat(&op).await };

                    self.state = ReadState::Seeking(Box::pin(future));
                    return self.poll_seek(cx, pos);
                }

                let total_size = self.size.expect("must have valid total_size");

                total_size as i64 + off
            }
        };

        self.pos = cur as u64;

        self.state = ReadState::Idle;
        Poll::Ready(Ok(self.pos))
    }
}

/// Writer is used to write data into underlying backend.
///
/// # TODO
///
/// maybe we can implement `AsyncWrite` for `Writer`
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
    pub async fn write_reader(self, r: BoxedAsyncReader, size: u64) -> Result<usize> {
        let op = &OpWrite {
            path: self.path.clone(),
            size,
        };

        self.acc.write(r, op).await
    }
}
