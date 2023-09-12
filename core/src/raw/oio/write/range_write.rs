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

use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;

use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::*;

/// RangeWrite is used to implement [`Write`] based on range write.
///
/// # Services
///
/// Services like gcs support range write via [GCS Resumable Upload](https://cloud.google.com/storage/docs/resumable-uploads).
///
/// GCS will support upload content by specifying the range of the file in `CONTENT-RANGE`.
///
/// Most range based services will have the following limitations:
///
/// - The size of chunk per upload must be aligned to a certain size. For example, GCS requires
///   to align with 256KiB.
/// - Some services requires to complete the write at the last chunk with the total size.
///
/// # Architecture
///
/// The architecture after adopting [`RangeWrite`]:
///
/// - Services impl `RangeWrite`
/// - `RangeWriter` impl `Write`
/// - Expose `RangeWriter` as `Accessor::Writer`
#[async_trait]
pub trait RangeWrite: Send + Sync + Unpin + 'static {
    /// Initiate range the range write, the returning value is the location.
    async fn initiate_range(&self) -> Result<String>;

    /// write_range will write a range of data.
    async fn write_range(
        &self,
        location: &str,
        written: u64,
        size: u64,
        body: AsyncBody,
    ) -> Result<()>;

    /// complete_range will complete the range write by uploading the last chunk.
    async fn complete_range(
        &self,
        location: &str,
        written: u64,
        size: u64,
        body: AsyncBody,
    ) -> Result<()>;

    /// abort_range will abort the range write by abort all already uploaded data.
    async fn abort_range(&self, location: &str) -> Result<()>;
}

/// RangeWriter will implements [`Write`] based on range write.
pub struct RangeWriter<W: RangeWrite> {
    location: Option<String>,
    written: u64,
    align_size: usize,
    align_buffer: oio::ChunkedCursor,

    state: State<W>,
}

enum State<W> {
    Idle(Option<W>),
    Init(BoxFuture<'static, (W, Result<String>)>),
    /// The returning value is (consume size, written size).
    Write(BoxFuture<'static, (W, Result<(usize, u64)>)>),
    Complete(BoxFuture<'static, (W, Result<()>)>),
    Abort(BoxFuture<'static, (W, Result<()>)>),
}

/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl<W: RangeWrite> Sync for State<W> {}

impl<W: RangeWrite> RangeWriter<W> {
    /// Create a new MultipartUploadWriter.
    pub fn new(inner: W) -> Self {
        Self {
            state: State::Idle(Some(inner)),

            location: None,
            written: 0,
            align_size: 256 * 1024,
            align_buffer: oio::ChunkedCursor::default(),
        }
    }

    /// Set the align size.
    ///
    /// The size is default to 256 KiB.
    ///
    /// # Note
    ///
    /// Please don't mix this with the buffer size. Align size is usually the hard
    /// limit for the service to accept the chunk.
    pub fn with_align_size(mut self, size: usize) -> Self {
        self.align_size = size;
        self
    }
}

impl<W: RangeWrite> oio::Write for RangeWriter<W> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn WriteBuf) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    match self.location.clone() {
                        Some(location) => {
                            let remaining = bs.remaining();
                            let current_size = self.align_buffer.len();
                            let mut total_size = current_size + remaining;

                            if total_size <= self.align_size {
                                let bs = bs.bytes(remaining);
                                self.align_buffer.push(bs);
                                return Poll::Ready(Ok(remaining));
                            }
                            // If total_size is aligned, we need to write one less chunk to make sure
                            // that the file has at least one chunk during complete stage.
                            if total_size % self.align_size == 0 {
                                total_size -= self.align_size;
                            }

                            let consume = total_size - total_size % self.align_size - current_size;
                            let mut align_buffer = self.align_buffer.clone();
                            let bs = bs.bytes(consume);
                            align_buffer.push(bs);

                            let written = self.written;
                            let w = w.take().unwrap();
                            let fut = async move {
                                let size = align_buffer.len() as u64;
                                let res = w
                                    .write_range(
                                        &location,
                                        written,
                                        size,
                                        AsyncBody::Stream(Box::new(align_buffer)),
                                    )
                                    .await;

                                (w, res.map(|_| (consume, size)))
                            };
                            self.state = State::Write(Box::pin(fut));
                        }
                        None => {
                            let w = w.take().unwrap();
                            let fut = async move {
                                let res = w.initiate_range().await;

                                (w, res)
                            };
                            self.state = State::Init(Box::pin(fut));
                        }
                    }
                }
                State::Init(fut) => {
                    let (w, res) = ready!(fut.poll_unpin(cx));
                    self.state = State::Idle(Some(w));
                    self.location = Some(res?);
                }
                State::Write(fut) => {
                    let (w, res) = ready!(fut.poll_unpin(cx));
                    self.state = State::Idle(Some(w));
                    let (consume, written) = res?;
                    self.written += written;
                    self.align_buffer.clear();
                    // It's possible that the buffer is already aligned, no bytes has been consumed.
                    if consume != 0 {
                        return Poll::Ready(Ok(consume));
                    }
                }
                State::Complete(_) => {
                    unreachable!("RangeWriter must not go into State::Complete during poll_write")
                }
                State::Abort(_) => {
                    unreachable!("RangeWriter must not go into State::Abort during poll_write")
                }
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().unwrap();
                    match self.location.clone() {
                        Some(location) => {
                            let align_buffer = self.align_buffer.clone();

                            let written = self.written;
                            let fut = async move {
                                let size = align_buffer.len() as u64;
                                let res = w
                                    .complete_range(
                                        &location,
                                        written,
                                        size,
                                        AsyncBody::Stream(Box::new(align_buffer)),
                                    )
                                    .await;

                                (w, res)
                            };
                            self.state = State::Complete(Box::pin(fut));
                        }
                        None => return Poll::Ready(Ok(())),
                    }
                }
                State::Init(_) => {
                    unreachable!("RangeWriter must not go into State::Init during poll_close")
                }
                State::Write(_) => {
                    unreachable!("RangeWriter must not go into State::Write during poll_close")
                }
                State::Complete(fut) => {
                    let (w, res) = ready!(fut.poll_unpin(cx));
                    self.state = State::Idle(Some(w));
                    self.align_buffer.clear();
                    return Poll::Ready(res);
                }
                State::Abort(_) => {
                    unreachable!("RangeWriter must not go into State::Abort during poll_close")
                }
            }
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().unwrap();
                    match self.location.clone() {
                        Some(location) => {
                            let fut = async move {
                                let res = w.abort_range(&location).await;

                                (w, res)
                            };
                            self.state = State::Abort(Box::pin(fut));
                        }
                        None => return Poll::Ready(Ok(())),
                    }
                }
                State::Init(_) => {
                    unreachable!("RangeWriter must not go into State::Init during poll_close")
                }
                State::Write(_) => {
                    unreachable!("RangeWriter must not go into State::Write during poll_close")
                }
                State::Complete(_) => {
                    unreachable!("RangeWriter must not go into State::Complete during poll_close")
                }
                State::Abort(fut) => {
                    let (w, res) = ready!(fut.poll_unpin(cx));
                    self.state = State::Idle(Some(w));
                    self.align_buffer.clear();
                    return Poll::Ready(res);
                }
            }
        }
    }
}
