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

use std::cmp::min;
use std::collections::VecDeque;
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
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait MultipartUploadWrite: Send + Sync + Unpin + 'static {
    /// write_once is used to write the data to underlying storage at once.
    ///
    /// MultipartUploadWriter will call this API when:
    ///
    /// - All the data has been written to the buffer and we can perform the upload at once.
    async fn write_once(&self, size: u64, body: AsyncBody) -> Result<()>;

    /// initiate_part will call start a multipart upload and return the upload id.
    ///
    /// MultipartUploadWriter will call this when:
    ///
    /// - the total size of data is unknown.
    /// - the total size of data is known, but the size of current write
    /// is less then the total size.
    async fn initiate_part(&self) -> Result<String>;

    /// write_part will write a part of the data and returns the result
    /// [`MultipartUploadPart`].
    ///
    /// MultipartUploadWriter will call this API and stores the result in
    /// order.
    ///
    /// - part_number is the index of the part, starting from 0.
    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: AsyncBody,
    ) -> Result<MultipartUploadPart>;

    /// complete_part will complete the multipart upload to build the final
    /// file.
    async fn complete_part(&self, upload_id: &str, parts: &[MultipartUploadPart]) -> Result<()>;

    /// abort_part will cancel the multipart upload and purge all data.
    async fn abort_part(&self, upload_id: &str) -> Result<()>;
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

struct UploadFuture(BoxedFuture<Result<MultipartUploadPart>>);

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this UploadFuture.
unsafe impl Send for UploadFuture {}

/// # Safety
///
/// We will only take `&mut Self` reference for UploadFuture.
unsafe impl Sync for UploadFuture {}

impl Future for UploadFuture {
    type Output = Result<MultipartUploadPart>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_unpin(cx)
    }
}

#[derive(Clone)]
struct WriteTask {
    part_number: usize,
    bs: oio::ChunkedBytes,
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this WriteTask.
unsafe impl Send for WriteTask {}
/// # Safety
///
/// We will only take `&mut Self` reference for WriteTask.
unsafe impl Sync for WriteTask {}

/// MultipartUploadWriter will implements [`Write`] based on multipart
/// uploads.
pub struct MultipartUploadWriter<W: MultipartUploadWrite> {
    state: State,
    w: Arc<W>,

    upload_id: Option<Arc<String>>,
    parts: Vec<MultipartUploadPart>,
    processing_tasks: VecDeque<WriteTask>,
    pending_tasks: VecDeque<WriteTask>,
    futures: ConcurrentFutures<UploadFuture>,
    part_number: usize,
}

enum State {
    Idle,
    Init(BoxedFuture<Result<String>>),
    Busy,
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
            processing_tasks: VecDeque::new(),
            pending_tasks: VecDeque::new(),
            futures: ConcurrentFutures::new(1.max(concurrent)),
            part_number: 0,
        }
    }

    /// Increases part number and return the previous part number.
    fn inc_part_number(&mut self) -> usize {
        let part_number = self.part_number;
        self.part_number += 1;
        part_number
    }

    fn process_write_task(&mut self, upload_id: Arc<String>, task: WriteTask) {
        let size = task.bs.len();
        let part_number = task.part_number;
        let bs = task.bs.clone();
        let w = self.w.clone();

        self.futures.push(UploadFuture(Box::pin(async move {
            w.write_part(
                &upload_id,
                part_number,
                size as u64,
                AsyncBody::ChunkedBytes(bs),
            )
            .await
        })));
        self.processing_tasks.push_back(task);
    }

    fn add_write_task(&mut self, bs: &dyn oio::WriteBuf) -> usize {
        let size = bs.remaining();
        let bs = oio::ChunkedBytes::from_vec(bs.vectored_bytes(size));
        let part_number = self.inc_part_number();
        self.pending_tasks.push_back(WriteTask { bs, part_number });
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
                                let task = self
                                    .pending_tasks
                                    .pop_front()
                                    .expect("pending task must exist");
                                self.process_write_task(upload_id, task);

                                let size = self.add_write_task(bs);
                                return Poll::Ready(Ok(size));
                            } else {
                                self.state = State::Busy;
                            }
                        }
                        None => {
                            // Fill cache with the first write.
                            if self.pending_tasks.is_empty() {
                                let size = self.add_write_task(bs);
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
                State::Busy => {
                    if let Some(part) = ready!(self.futures.poll_next_unpin(cx)) {
                        // Safety: must exist.
                        let task = self.processing_tasks.pop_front().unwrap();
                        match part {
                            Ok(part) => self.parts.push(part),
                            Err(err) => {
                                self.pending_tasks.push_front(task);
                                return Poll::Ready(Err(err));
                            }
                        }
                    }
                    self.state = State::Idle;
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
                            if self.futures.is_empty() && self.pending_tasks.is_empty() {
                                let upload_id = upload_id.clone();
                                let parts = self.parts.clone();
                                self.state = State::Close(Box::pin(async move {
                                    w.complete_part(&upload_id, &parts).await
                                }));
                            } else {
                                let rem = min(self.futures.remaining(), self.pending_tasks.len());
                                for _ in 0..rem {
                                    if let Some(task) = self.pending_tasks.pop_front() {
                                        let upload_id = upload_id.clone();
                                        self.process_write_task(upload_id, task);
                                    }
                                }
                                self.state = State::Busy;
                            }
                        }
                        None => match self.pending_tasks.pop_front() {
                            Some(task) => {
                                let w = self.w.clone();
                                let bs = task.bs.clone();
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

                    return Poll::Ready(Ok(()));
                }
                State::Init(_) => unreachable!(
                    "MultipartUploadWriter must not go into State::Init during poll_close"
                ),
                State::Busy => {
                    while let Some(part) = ready!(self.futures.poll_next_unpin(cx)) {
                        // Safety: must exist.
                        let task = self.processing_tasks.pop_front().unwrap();
                        match part {
                            Ok(part) => self.parts.push(part),
                            Err(err) => {
                                self.pending_tasks.push_front(task);
                                return Poll::Ready(Err(err));
                            }
                        }
                    }
                    self.state = State::Idle;
                }
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
                State::Busy => unreachable!(
                    "MultipartUploadWriter must not go into State::Busy during poll_abort"
                ),
                State::Close(_) => unreachable!(
                    "MultipartUploadWriter must not go into State::Close during poll_abort"
                ),
            }
        }
    }
}
