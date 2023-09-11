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

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::task::{ready, Context, Poll};

use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::*;

#[async_trait]
pub trait RangeWrite: Send + Sync + Unpin + 'static {
    async fn initiate_range(&self) -> Result<String>;

    async fn write_range(
        &self,
        location: &str,
        written: u64,
        size: u64,
        body: AsyncBody,
    ) -> Result<()>;

    async fn complete_range(
        &self,
        location: &str,
        written: u64,
        size: u64,
        body: AsyncBody,
    ) -> Result<()>;

    async fn abort_range(&self, location: &str) -> Result<()>;
}

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
                    let w = w.take().unwrap();
                    match self.location.clone() {
                        Some(location) => {
                            let remaining = bs.remaining();
                            let current_size = self.align_buffer.len();
                            let mut total_size = current_size + remaining;

                            if total_size <= self.align_size {
                                let bs = bs.copy_to_bytes(remaining);
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
                            let bs = bs.copy_to_bytes(consume);
                            align_buffer.push(bs);

                            let written = self.written;
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
                    return Poll::Ready(Ok(consume));
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
                            debug_assert!(
                                self.align_buffer.len() > 0,
                                "RangeWriter requires to have last chunk"
                            );
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
