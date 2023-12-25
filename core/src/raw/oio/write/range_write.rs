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
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait RangeWrite: Send + Sync + Unpin + 'static {
    /// write_once is used to write the data to underlying storage at once.
    ///
    /// RangeWriter will call this API when:
    ///
    /// - All the data has been written to the buffer and we can perform the upload at once.
    async fn write_once(&self, size: u64, body: AsyncBody) -> Result<()>;

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
    buffer: Option<oio::ChunkedBytes>,

    state: State<W>,
}

enum State<W> {
    Idle(Option<W>),
    Init(BoxedFuture<(W, Result<String>)>),
    Write(BoxedFuture<(W, Result<u64>)>),
    Complete(BoxedFuture<(W, Result<()>)>),
    Abort(BoxedFuture<(W, Result<()>)>),
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this state.
unsafe impl<S: RangeWrite> Send for State<S> {}

/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl<W: RangeWrite> Sync for State<W> {}

impl<W: RangeWrite> RangeWriter<W> {
    /// Create a new MultipartUploadWriter.
    pub fn new(inner: W) -> Self {
        Self {
            state: State::Idle(Some(inner)),

            buffer: None,
            location: None,
            written: 0,
        }
    }
}

impl<W: RangeWrite> oio::Write for RangeWriter<W> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn WriteBuf) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    match self.location.clone() {
                        Some(location) => {
                            let written = self.written;

                            let buffer = self.buffer.clone().expect("cache must be valid").clone();
                            let w = w.take().expect("writer must be valid");
                            self.state = State::Write(Box::pin(async move {
                                let size = buffer.len() as u64;
                                let res = w
                                    .write_range(
                                        &location,
                                        written,
                                        size,
                                        AsyncBody::ChunkedBytes(buffer),
                                    )
                                    .await;

                                (w, res.map(|_| size))
                            }));
                        }
                        None => {
                            // Fill cache with the first write.
                            if self.buffer.is_none() {
                                let size = bs.remaining();
                                let cb = oio::ChunkedBytes::from_vec(bs.vectored_bytes(size));
                                self.buffer = Some(cb);
                                return Poll::Ready(Ok(size));
                            }

                            let w = w.take().expect("writer must be valid");
                            self.state = State::Init(Box::pin(async move {
                                let location = w.initiate_range().await;
                                (w, location)
                            }));
                        }
                    }
                }
                State::Init(fut) => {
                    let (w, res) = ready!(fut.poll_unpin(cx));
                    self.state = State::Idle(Some(w));
                    self.location = Some(res?);
                }
                State::Write(fut) => {
                    let (w, size) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));
                    // Update the written.
                    self.written += size?;

                    // Replace the cache when last write succeeded
                    let size = bs.remaining();
                    let cb = oio::ChunkedBytes::from_vec(bs.vectored_bytes(size));
                    self.buffer = Some(cb);
                    return Poll::Ready(Ok(size));
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
                    let w = w.take().expect("writer must be valid");
                    match self.location.clone() {
                        Some(location) => {
                            let written = self.written;
                            match self.buffer.clone() {
                                Some(bs) => {
                                    self.state = State::Complete(Box::pin(async move {
                                        let res = w
                                            .complete_range(
                                                &location,
                                                written,
                                                bs.len() as u64,
                                                AsyncBody::ChunkedBytes(bs),
                                            )
                                            .await;
                                        (w, res)
                                    }));
                                }
                                None => {
                                    unreachable!("It's must be bug that RangeWrite is in State::Idle with no cache but has location")
                                }
                            }
                        }
                        None => match self.buffer.clone() {
                            Some(bs) => {
                                self.state = State::Complete(Box::pin(async move {
                                    let size = bs.len();
                                    let res = w
                                        .write_once(size as u64, AsyncBody::ChunkedBytes(bs))
                                        .await;
                                    (w, res)
                                }));
                            }
                            None => {
                                // Call write_once if there is no data in buffer and no location.
                                self.state = State::Complete(Box::pin(async move {
                                    let res = w.write_once(0, AsyncBody::Empty).await;
                                    (w, res)
                                }));
                            }
                        },
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
                    // We should check res first before clean up cache.
                    res?;

                    self.buffer = None;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
