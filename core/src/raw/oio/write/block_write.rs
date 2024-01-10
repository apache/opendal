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

use crate::raw::*;
use crate::*;

/// BlockWrite is used to implement [`Write`] based on block
/// uploads. By implementing BlockWrite, services don't need to
/// care about the details of uploading blocks.
///
/// # Architecture
///
/// The architecture after adopting [`BlockWrite`]:
///
/// - Services impl `BlockWrite`
/// - `BlockWriter` impl `Write`
/// - Expose `BlockWriter` as `Accessor::Writer`
///
/// # Notes
///
/// `BlockWrite` has an oneshot optimization when `write` has been called only once:
///
/// ```no_build
/// w.write(bs).await?;
/// w.close().await?;
/// ```
///
/// We will use `write_once` instead of starting a new block upload.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait BlockWrite: Send + Sync + Unpin + 'static {
    /// write_once is used to write the data to underlying storage at once.
    ///
    /// BlockWriter will call this API when:
    ///
    /// - All the data has been written to the buffer and we can perform the upload at once.
    async fn write_once(&self, size: u64, body: AsyncBody) -> Result<()>;

    /// write_block will write a block of the data and returns the result
    /// [`Block`].
    ///
    /// BlockWriter will call this API and stores the result in
    /// order.
    ///
    /// - block_id is the id of the block.
    async fn write_block(&self, size: u64, block_id: String, body: AsyncBody) -> Result<()>;

    /// complete_block will complete the block upload to build the final
    /// file.
    async fn complete_block(&self, block_ids: Vec<String>) -> Result<()>;

    /// abort_block will cancel the block upload and purge all data.
    async fn abort_block(&self, block_ids: Vec<String>) -> Result<()>;
}

struct WriteBlockFuture(BoxedFuture<Result<()>>);

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this WriteBlockFuture.
unsafe impl Send for WriteBlockFuture {}

/// # Safety
///
/// We will only take `&mut Self` reference for WriteBlockFuture.
unsafe impl Sync for WriteBlockFuture {}

impl Future for WriteBlockFuture {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_unpin(cx)
    }
}

/// BlockWriter will implements [`Write`] based on block
/// uploads.
pub struct BlockWriter<W: BlockWrite> {
    state: State,
    w: Arc<W>,

    block_ids: Vec<String>,
    cache: Option<oio::ChunkedBytes>,
    futures: ConcurrentFutures<WriteBlockFuture>,
}

enum State {
    Idle,
    Close(BoxedFuture<Result<()>>),
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

impl<W: BlockWrite> BlockWriter<W> {
    /// Create a new BlockWriter.
    pub fn new(inner: W, concurrent: usize) -> Self {
        Self {
            state: State::Idle,

            w: Arc::new(inner),
            block_ids: Vec::new(),
            cache: None,
            futures: ConcurrentFutures::new(1.max(concurrent)),
        }
    }

    fn fill_cache(&mut self, bs: &dyn oio::WriteBuf) -> usize {
        let size = bs.remaining();
        let bs = oio::ChunkedBytes::from_vec(bs.vectored_bytes(size));
        assert!(self.cache.is_none());
        self.cache = Some(bs);
        size
    }
}

impl<W> oio::Write for BlockWriter<W>
where
    W: BlockWrite,
{
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    if self.futures.has_remaining() {
                        // Fill cache with the first write.
                        if self.cache.is_none() {
                            let size = self.fill_cache(bs);
                            return Poll::Ready(Ok(size));
                        }
                        let cache = self.cache.take().expect("pending write must exist");
                        let block_id = uuid::Uuid::new_v4().to_string();
                        self.block_ids.push(block_id.clone());
                        let w = self.w.clone();
                        let size = cache.len();
                        self.futures
                            .push_back(WriteBlockFuture(Box::pin(async move {
                                w.write_block(size as u64, block_id, AsyncBody::ChunkedBytes(cache))
                                    .await
                            })));
                        let size = self.fill_cache(bs);
                        return Poll::Ready(Ok(size));
                    } else if let Some(res) = ready!(self.futures.poll_next_unpin(cx)) {
                        res?;
                    }
                }
                State::Close(_) => {
                    unreachable!("BlockWriter must not go into State::Close during poll_write")
                }
                State::Abort(_) => {
                    unreachable!("BlockWriter must not go into State::Abort during poll_write")
                }
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    let w = self.w.clone();
                    let block_ids = self.block_ids.clone();

                    if self.block_ids.is_empty() {
                        match &self.cache {
                            Some(cache) => {
                                let w = self.w.clone();
                                let bs = cache.clone();
                                self.state = State::Close(Box::pin(async move {
                                    let size = bs.len();
                                    w.write_once(size as u64, AsyncBody::ChunkedBytes(bs)).await
                                }));
                            }
                            None => {
                                let w = self.w.clone();
                                // Call write_once if there is no data in cache.
                                self.state = State::Close(Box::pin(async move {
                                    w.write_once(0, AsyncBody::Empty).await
                                }));
                            }
                        }
                    } else if self.futures.is_empty() && self.cache.is_none() {
                        self.state =
                            State::Close(Box::pin(
                                async move { w.complete_block(block_ids).await },
                            ));
                    } else {
                        if self.futures.has_remaining() {
                            if let Some(cache) = self.cache.take() {
                                let block_id = uuid::Uuid::new_v4().to_string();
                                self.block_ids.push(block_id.clone());
                                let size = cache.len();
                                let w = self.w.clone();
                                self.futures
                                    .push_back(WriteBlockFuture(Box::pin(async move {
                                        w.write_block(
                                            size as u64,
                                            block_id,
                                            AsyncBody::ChunkedBytes(cache),
                                        )
                                        .await
                                    })));
                            }
                        }
                        while let Some(res) = ready!(self.futures.poll_next_unpin(cx)) {
                            res?;
                        }
                    }
                }
                State::Close(fut) => {
                    let res = futures::ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle;
                    // We should check res first before clean up cache.
                    res?;
                    self.cache = None;

                    return Poll::Ready(Ok(()));
                }
                State::Abort(_) => {
                    unreachable!("BlockWriter must not go into State::Abort during poll_close")
                }
            }
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    let w = self.w.clone();
                    let block_ids = self.block_ids.clone();
                    self.futures.clear();
                    self.state =
                        State::Abort(Box::pin(async move { w.abort_block(block_ids).await }));
                }
                State::Abort(fut) => {
                    let res = futures::ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle;
                    return Poll::Ready(res);
                }
                State::Close(_) => {
                    unreachable!("BlockWriter must not go into State::Close during poll_abort")
                }
            }
        }
    }
}
