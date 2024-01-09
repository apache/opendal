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

use futures::Future;
use futures::FutureExt;
use futures::StreamExt;

use crate::raw::*;
use crate::*;

/// MultipartUploadWrite is used to implement [`Write`] based on multipart
/// uploads. By implementing MultipartUploadWrite, services don't need to
/// care about the details of uploading parts.
///
/// # Architecture
///
/// The architecture after adopting [`MultipartUploadWrite`]:
///
/// - Services impl `MultipartUploadWrite`
/// - `MultipartUploadWriter` impl `Write`
/// - Expose `MultipartUploadWriter` as `Accessor::Writer`
///
/// # Notes
///
/// `MultipartUploadWrite` has an oneshot optimization when `write` has been called only once:
///
/// ```no_build
/// w.write(bs).await?;
/// w.close().await?;
/// ```
///
/// We will use `write_once` instead of starting a new multipart upload.
pub trait MultipartUploadWrite: Send + Sync + Unpin + 'static {
    /// write_once is used to write the data to underlying storage at once.
    ///
    /// MultipartUploadWriter will call this API when:
    ///
    /// - All the data has been written to the buffer and we can perform the upload at once.
    fn write_once(&self, size: u64, body: AsyncBody) -> impl MaybeSendFuture<Result<()>>;

    /// initiate_part will call start a multipart upload and return the upload id.
    ///
    /// MultipartUploadWriter will call this when:
    ///
    /// - the total size of data is unknown.
    /// - the total size of data is known, but the size of current write
    /// is less then the total size.
    fn initiate_part(&self) -> impl MaybeSendFuture<Result<String>>;

    /// write_part will write a part of the data and returns the result
    /// [`MultipartUploadPart`].
    ///
    /// MultipartUploadWriter will call this API and stores the result in
    /// order.
    ///
    /// - part_number is the index of the part, starting from 0.
    fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: AsyncBody,
    ) -> impl MaybeSendFuture<Result<MultipartUploadPart>>;

    /// complete_part will complete the multipart upload to build the final
    /// file.
    fn complete_part(
        &self,
        upload_id: &str,
        parts: &[MultipartUploadPart],
    ) -> impl MaybeSendFuture<Result<()>>;

    /// abort_part will cancel the multipart upload and purge all data.
    fn abort_part(&self, upload_id: &str) -> impl MaybeSendFuture<Result<()>>;
}

/// The result of [`MultipartUploadWrite::write_part`].
///
/// services implement should convert MultipartUploadPart to their own represents.
///
/// - `part_number` is the index of the part, starting from 0.
/// - `etag` is the `ETag` of the part.
#[derive(Clone)]
pub struct MultipartUploadPart {
    /// The number of the part, starting from 0.
    pub part_number: usize,
    /// The etag of the part.
    pub etag: String,
}

struct WritePartFuture(BoxedFuture<Result<MultipartUploadPart>>);

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this WritePartFuture.
unsafe impl Send for WritePartFuture {}

/// # Safety
///
/// We will only take `&mut Self` reference for WritePartFuture.
unsafe impl Sync for WritePartFuture {}

impl Future for WritePartFuture {
    type Output = Result<MultipartUploadPart>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_unpin(cx)
    }
}

/// MultipartUploadWriter will implements [`Write`] based on multipart
/// uploads.
pub struct MultipartUploadWriter<W: MultipartUploadWrite> {
    state: State,
    w: Arc<W>,

    upload_id: Option<Arc<String>>,
    parts: Vec<MultipartUploadPart>,
    cache: Option<oio::ChunkedBytes>,
    futures: ConcurrentFutures<WritePartFuture>,
    next_part_number: usize,
}

enum State {
    Idle,
    Init(BoxedFuture<Result<String>>),
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

impl<W: MultipartUploadWrite> MultipartUploadWriter<W> {
    /// Create a new MultipartUploadWriter.
    pub fn new(inner: W, concurrent: usize) -> Self {
        Self {
            state: State::Idle,

            w: Arc::new(inner),
            upload_id: None,
            parts: Vec::new(),
            cache: None,
            futures: ConcurrentFutures::new(1.max(concurrent)),
            next_part_number: 0,
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

impl<W> oio::Write for MultipartUploadWriter<W>
where
    W: MultipartUploadWrite,
{
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    match self.upload_id.as_ref() {
                        Some(upload_id) => {
                            let upload_id = upload_id.clone();
                            if self.futures.has_remaining() {
                                let cache = self.cache.take().expect("pending write must exist");
                                let part_number = self.next_part_number;
                                self.next_part_number += 1;
                                let w = self.w.clone();
                                let size = cache.len();
                                self.futures.push(WritePartFuture(Box::pin(async move {
                                    w.write_part(
                                        &upload_id,
                                        part_number,
                                        size as u64,
                                        AsyncBody::ChunkedBytes(cache),
                                    )
                                    .await
                                })));
                                let size = self.fill_cache(bs);
                                return Poll::Ready(Ok(size));
                            } else if let Some(part) = ready!(self.futures.poll_next_unpin(cx)) {
                                self.parts.push(part?);
                            }
                        }
                        None => {
                            // Fill cache with the first write.
                            if self.cache.is_none() {
                                let size = self.fill_cache(bs);
                                return Poll::Ready(Ok(size));
                            }

                            let w = self.w.clone();
                            self.state =
                                State::Init(Box::pin(async move { w.initiate_part().await }));
                        }
                    }
                }
                State::Init(fut) => {
                    let upload_id = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle;
                    self.upload_id = Some(Arc::new(upload_id?));
                }
                State::Close(_) => {
                    unreachable!(
                        "MultipartUploadWriter must not go into State::Close during poll_write"
                    )
                }
                State::Abort(_) => {
                    unreachable!(
                        "MultipartUploadWriter must not go into State::Abort during poll_write"
                    )
                }
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    match self.upload_id.clone() {
                        Some(upload_id) => {
                            let w = self.w.clone();
                            if self.futures.is_empty() && self.cache.is_none() {
                                let upload_id = upload_id.clone();
                                let parts = self.parts.clone();
                                self.state = State::Close(Box::pin(async move {
                                    w.complete_part(&upload_id, &parts).await
                                }));
                            } else {
                                if self.futures.has_remaining() {
                                    if let Some(cache) = self.cache.take() {
                                        let upload_id = upload_id.clone();
                                        let part_number = self.next_part_number;
                                        self.next_part_number += 1;
                                        let size = cache.len();
                                        let w = self.w.clone();
                                        self.futures.push(WritePartFuture(Box::pin(async move {
                                            w.write_part(
                                                &upload_id,
                                                part_number,
                                                size as u64,
                                                AsyncBody::ChunkedBytes(cache),
                                            )
                                            .await
                                        })));
                                    }
                                }
                                while let Some(part) = ready!(self.futures.poll_next_unpin(cx)) {
                                    self.parts.push(part?);
                                }
                            }
                        }
                        None => match &self.cache {
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
                                // Call write_once if there is no data in cache and no upload_id.
                                self.state = State::Close(Box::pin(async move {
                                    w.write_once(0, AsyncBody::Empty).await
                                }));
                            }
                        },
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
                State::Init(_) => unreachable!(
                    "MultipartUploadWriter must not go into State::Init during poll_close"
                ),
                State::Abort(_) => unreachable!(
                    "MultipartUploadWriter must not go into State::Abort during poll_close"
                ),
            }
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    let w = self.w.clone();
                    match self.upload_id.clone() {
                        Some(upload_id) => {
                            self.futures.clear();
                            self.state =
                                State::Abort(Box::pin(
                                    async move { w.abort_part(&upload_id).await },
                                ));
                        }
                        None => {
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                State::Abort(fut) => {
                    let res = futures::ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle;
                    return Poll::Ready(res);
                }
                State::Init(_) => unreachable!(
                    "MultipartUploadWriter must not go into State::Init during poll_abort"
                ),
                State::Close(_) => unreachable!(
                    "MultipartUploadWriter must not go into State::Close during poll_abort"
                ),
            }
        }
    }
}
