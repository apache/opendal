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
use futures::ready;
use futures::AsyncRead;
use futures::AsyncSeek;

use crate::error::Result;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::Metadata;

/// BoxedAsyncRead is a boxed AsyncRead.
pub type BoxedAsyncRead = Box<dyn AsyncRead + Unpin + Send>;

/// Reader is used for reading data from underlying backend.
///
/// # Lazy Stat
///
/// We will fetch the object's content-length while the first time
/// caller try to seek with `SeekFrom::End(pos)` and the total size is `None`.
pub struct Reader {
    acc: Arc<dyn Accessor>,
    path: String,

    total_size: Option<u64>,
    pos: u64,
    state: ReadState,
}

enum ReadState {
    Idle,
    Sending(BoxFuture<'static, Result<BoxedAsyncRead>>),
    Seeking(BoxFuture<'static, Result<Metadata>>),
    Reading(BoxedAsyncRead),
}

impl Reader {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
            total_size: None,
            pos: 0,
            state: ReadState::Idle,
        }
    }
    /// Change the total size of this Reader.
    ///
    /// # Note
    ///
    /// We consume the whole reader here to indicate that we can't change it
    /// once we start reading.
    pub fn total_size(mut self, size: u64) -> Self {
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
        match &mut self.state {
            ReadState::Idle => {
                let acc = self.acc.clone();
                let pos = self.pos;
                let op = OpRead {
                    path: self.path.to_string(),
                    offset: Some(pos),
                    size: None,
                };

                let future = async move { acc.read(&op).await };

                self.state = ReadState::Sending(Box::pin(future));
                self.poll_read(cx, buf)
            }
            ReadState::Sending(future) => match ready!(Pin::new(future).poll(cx)) {
                Ok(r) => {
                    self.state = ReadState::Reading(r);
                    self.poll_read(cx, buf)
                }
                Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
            },
            ReadState::Reading(r) => match ready!(Pin::new(r).poll_read(cx, buf)) {
                Ok(n) => {
                    self.pos += n as u64;
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
            println!("poll seek");
            match ready!(Pin::new(future).poll(cx)) {
                Ok(meta) => self.total_size = Some(meta.content_length()),
                Err(e) => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
            }
        }

        self.pos = match pos {
            SeekFrom::Start(off) => off,
            SeekFrom::Current(off) => (self.pos as i64).checked_add(off).expect("overflow") as u64,
            SeekFrom::End(off) => {
                // Stat the object to get it's content-length.
                if self.total_size.is_none() {
                    let acc = self.acc.clone();
                    let op = OpStat::new(&self.path);

                    let future = async move { acc.stat(&op).await };

                    println!("into seek state");
                    self.state = ReadState::Seeking(Box::pin(future));
                    return self.poll_seek(cx, pos);
                }

                let total_size = self.total_size.expect("must have valid total_size") as i64;

                (total_size.checked_add(off).expect("overflow")) as u64
            }
        };

        self.state = ReadState::Idle;
        Poll::Ready(Ok(self.pos as u64))
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
    pub async fn write_reader(self, r: BoxedAsyncRead, size: u64) -> Result<usize> {
        let op = &OpWrite {
            path: self.path.clone(),
            size,
        };

        self.acc.write(r, op).await
    }
}
