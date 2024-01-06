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

use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::Future;
use futures::FutureExt;
use futures::StreamExt;

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
        offset: u64,
        size: u64,
        body: AsyncBody,
    ) -> Result<()>;

    /// complete_range will complete the range write by uploading the last chunk.
    async fn complete_range(
        &self,
        location: &str,
        offset: u64,
        size: u64,
        body: AsyncBody,
    ) -> Result<()>;

    /// abort_range will abort the range write by abort all already uploaded data.
    async fn abort_range(&self, location: &str) -> Result<()>;
}

struct WriteRangeFuture(BoxedFuture<Result<()>>);

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this WriteRangeFuture.
unsafe impl Send for WriteRangeFuture {}

/// # Safety
///
/// We will only take `&mut Self` reference for WriteRangeFuture.
unsafe impl Sync for WriteRangeFuture {}

impl Future for WriteRangeFuture {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_unpin(cx)
    }
}

/// RangeWriter will implements [`Write`] based on range write.
pub struct RangeWriter<W: RangeWrite> {
    location: Option<String>,
    next_offset: u64,
    buffer: Option<oio::ChunkedBytes>,
    futures: ConcurrentFutures<WriteRangeFuture>,

    w: Arc<W>,
    state: State,
}

enum State {
    Idle,
    Init(BoxedFuture<Result<String>>),
    Complete(BoxedFuture<Result<()>>),
    Abort(BoxedFuture<Result<()>>),
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this state.
unsafe impl Send for State {}

/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl Sync for State {}

impl<W: RangeWrite> RangeWriter<W> {
    /// Create a new MultipartUploadWriter.
    pub fn new(inner: W, concurrent: usize) -> Self {
        Self {
            state: State::Idle,
            w: Arc::new(inner),

            futures: ConcurrentFutures::new(1.max(concurrent)),
            buffer: None,
            location: None,
            next_offset: 0,
        }
    }

    fn fill_cache(&mut self, bs: &dyn WriteBuf) -> usize {
        let size = bs.remaining();
        let bs = oio::ChunkedBytes::from_vec(bs.vectored_bytes(size));
        assert!(self.buffer.is_none());
        self.buffer = Some(bs);
        size
    }
}

impl<W: RangeWrite> oio::Write for RangeWriter<W> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn WriteBuf) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    match self.location.clone() {
                        Some(location) => {
                            if self.futures.has_remaining() {
                                let cache = self.buffer.take().expect("cache must be valid");
                                let size = cache.len() as u64;
                                let offset = self.next_offset;
                                self.next_offset += size;
                                let w = self.w.clone();
                                self.futures.push(WriteRangeFuture(Box::pin(async move {
                                    w.write_range(
                                        &location,
                                        offset,
                                        size,
                                        AsyncBody::ChunkedBytes(cache),
                                    )
                                    .await
                                })));
                                let size = self.fill_cache(bs);
                                return Poll::Ready(Ok(size));
                            } else if let Some(result) = ready!(self.futures.poll_next_unpin(cx)) {
                                result?;
                            }
                        }
                        None => {
                            // Fill cache with the first write.
                            if self.buffer.is_none() {
                                let size = self.fill_cache(bs);
                                return Poll::Ready(Ok(size));
                            }

                            let w = self.w.clone();
                            self.state =
                                State::Init(Box::pin(async move { w.initiate_range().await }));
                        }
                    }
                }
                State::Init(fut) => {
                    let res = ready!(fut.poll_unpin(cx));
                    self.state = State::Idle;
                    self.location = Some(res?);
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
                State::Idle => {
                    let w = self.w.clone();
                    match self.location.clone() {
                        Some(location) => {
                            if !self.futures.is_empty() {
                                while let Some(result) = ready!(self.futures.poll_next_unpin(cx)) {
                                    result?;
                                }
                            }
                            match self.buffer.take() {
                                Some(bs) => {
                                    let offset = self.next_offset;
                                    self.state = State::Complete(Box::pin(async move {
                                        w.complete_range(
                                            &location,
                                            offset,
                                            bs.len() as u64,
                                            AsyncBody::ChunkedBytes(bs),
                                        )
                                        .await
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
                                    w.write_once(size as u64, AsyncBody::ChunkedBytes(bs)).await
                                }));
                            }
                            None => {
                                // Call write_once if there is no data in buffer and no location.
                                self.state = State::Complete(Box::pin(async move {
                                    w.write_once(0, AsyncBody::Empty).await
                                }));
                            }
                        },
                    }
                }
                State::Init(_) => {
                    unreachable!("RangeWriter must not go into State::Init during poll_close")
                }
                State::Complete(fut) => {
                    let res = ready!(fut.poll_unpin(cx));
                    self.state = State::Idle;
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
                State::Idle => match self.location.clone() {
                    Some(location) => {
                        let w = self.w.clone();
                        self.futures.clear();
                        self.state =
                            State::Abort(Box::pin(async move { w.abort_range(&location).await }));
                    }
                    None => return Poll::Ready(Ok(())),
                },
                State::Init(_) => {
                    unreachable!("RangeWriter must not go into State::Init during poll_close")
                }
                State::Complete(_) => {
                    unreachable!("RangeWriter must not go into State::Complete during poll_close")
                }
                State::Abort(fut) => {
                    let res = ready!(fut.poll_unpin(cx));
                    self.state = State::Idle;
                    // We should check res first before clean up cache.
                    res?;

                    self.buffer = None;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
