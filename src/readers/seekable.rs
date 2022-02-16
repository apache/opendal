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
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::{cmp, io};

use futures::future::BoxFuture;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::AsyncSeek;

use crate::Operator;

const DEFAULT_REQUEST_SIZE: usize = 4 * 1024 * 1024; // default to 4mb.

/// If we already know a file's total size, we can implement Seek for it.
///
/// - Every time we call `read` we will send a new http request to fetch data from cloud storage like s3.
/// - Every time we call `seek` we will update the `pos` field just in memory.
///
/// # NOTE
///
/// It's better to use SeekableReader as an inner reader inside BufReader.
///
/// # Acknowledgement
///
/// Current implementation is highly inspired by [ranged-reader-rs](https://github.com/DataEngineeringLabs/ranged-reader-rs)
///
/// # TODO
///
/// We need use update the metrics.
pub struct SeekableReader {
    op: Operator,
    key: String,
    total: u64,

    pos: u64,
    state: State,
}

impl SeekableReader {
    pub fn new(op: Operator, key: &str, total: u64) -> Self {
        SeekableReader {
            op,
            key: key.to_string(),
            total,

            pos: 0,
            state: State::Chunked(Chunk {
                offset: 0,
                data: vec![],
            }),
        }
    }

    fn remaining(&self) -> u64 {
        self.total - self.pos as u64
    }

    // Calculate the user requested range.
    fn request_range(&self, size: usize) -> Range {
        Range {
            offset: self.pos,
            size: cmp::min(size, self.remaining() as usize),
        }
    }

    // Calculate the next prefetch range.
    fn prefetch_range(&self, size: usize) -> Range {
        // Pick the max size between hint and default size.
        let size = cmp::max(size, DEFAULT_REQUEST_SIZE);

        Range {
            offset: self.pos,
            // Make sure request size will not exceeding the total size.
            size: cmp::min(size, self.remaining() as usize),
        }
    }
}

impl AsyncRead for SeekableReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let request_range = self.request_range(buf.len());

        match &mut self.state {
            State::Chunked(chunk) => {
                if request_range.size == 0 {
                    return Poll::Ready(Ok(0));
                }

                let exist_range = chunk.as_range();
                if exist_range.includes(request_range) {
                    let offset = (request_range.offset - exist_range.offset) as usize;
                    let length = request_range.size;
                    let (buf, _) = buf.split_at_mut(length);
                    buf.copy_from_slice(&chunk.data[offset..offset + length]);
                    self.pos += length as u64;

                    Poll::Ready(Ok(length))
                } else {
                    let op = self.op.clone();
                    let key = self.key.clone();
                    let prefetch_range = self.prefetch_range(buf.len());

                    let future = async move {
                        let mut r = op
                            .read(&key)
                            .offset(prefetch_range.offset as u64)
                            .size(prefetch_range.size as u64)
                            .run()
                            .await
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                        // TODO: Can we reuse the same slice?
                        let mut data = vec![];
                        let _ = r.read_to_end(&mut data).await?;

                        Ok(Chunk {
                            offset: prefetch_range.offset as u64,
                            data,
                        })
                    };

                    self.state = State::Reading(Box::pin(future));
                    self.poll_read(cx, buf)
                }
            }
            State::Reading(future) => match Pin::new(future).poll(cx) {
                Poll::Ready(Ok(chunk)) => {
                    self.state = State::Chunked(chunk);
                    self.poll_read(cx, buf)
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl AsyncSeek for SeekableReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        off: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        match off {
            SeekFrom::Start(off) => {
                self.pos = off;
            }
            SeekFrom::End(off) => {
                self.pos = (self.total as i64).checked_add(off).expect("overflow") as u64;
            }
            SeekFrom::Current(off) => {
                self.pos = (self.pos as i64).checked_add(off).expect("overflow") as u64;
            }
        }

        Poll::Ready(Ok(self.pos as u64))
    }
}

#[derive(Debug, Clone)]
struct Chunk {
    offset: u64,
    data: Vec<u8>,
}

impl Chunk {
    fn as_range(&self) -> Range {
        Range {
            offset: self.offset,
            size: self.data.len(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Range {
    offset: u64,
    size: usize,
}

impl Range {
    // Check whether self includes `rhs` or not.
    fn includes(&self, rhs: Self) -> bool {
        if rhs.offset < self.offset {
            return false;
        }
        if rhs.offset + rhs.size as u64 > self.offset + self.size as u64 {
            return false;
        }
        true
    }
}

enum State {
    Chunked(Chunk),
    Reading(BoxFuture<'static, io::Result<Chunk>>),
}
