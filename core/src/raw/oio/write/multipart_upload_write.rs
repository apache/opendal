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
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use crate::raw::*;
use crate::*;

/// MultipartUploadWrite is used to implement [`Write`] based on multipart
/// uploads. By implementing MultipartUploadWrite, services don't need to
/// care about the details of buffering and uploading parts.
///
/// The layout after adopting [`MultipartUploadWrite`]:
///
/// - Services impl `MultipartUploadWrite`
/// - `MultipartUploadWriter` impl `Write`
/// - Expose `MultipartUploadWriter` as `Accessor::Writer`
#[async_trait]
pub trait MultipartUploadWrite: Send + Sync + Unpin {
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
pub struct MultipartUploadPart {
    /// The number of the part, starting from 0.
    pub part_number: usize,
    /// The etag of the part.
    pub etag: String,
}

/// MultipartUploadWriter will implements [`Write`] based on multipart
/// uploads.
pub struct MultipartUploadWriter<W: MultipartUploadWrite> {
    state: State<W>,

    upload_id: Option<Arc<String>>,
    parts: Vec<MultipartUploadPart>,
}

enum State<W> {
    Idle(Option<W>),
    Init(BoxFuture<'static, (W, Result<String>)>),
    Write(BoxFuture<'static, (W, Result<(usize, MultipartUploadPart)>)>),
    Close(BoxFuture<'static, (W, Result<()>)>),
    Abort(BoxFuture<'static, (W, Result<()>)>),
}

/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl<S: MultipartUploadWrite> Sync for State<S> {}

impl<W: MultipartUploadWrite> MultipartUploadWriter<W> {
    /// Create a new MultipartUploadWriter.
    pub fn new(inner: W) -> Self {
        Self {
            state: State::Idle(Some(inner)),

            upload_id: None,
            parts: Vec::new(),
        }
    }
}

#[async_trait]
impl<W> oio::Write for MultipartUploadWriter<W>
where
    W: MultipartUploadWrite,
{
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().expect("writer must be valid");
                    match self.upload_id.as_ref() {
                        Some(upload_id) => {
                            let size = bs.remaining();
                            let bs = bs.copy_to_bytes(size);
                            let upload_id = upload_id.clone();
                            let part_number = self.parts.len();

                            self.state = State::Write(Box::pin(async move {
                                let part = w
                                    .write_part(
                                        &upload_id,
                                        part_number,
                                        size as u64,
                                        AsyncBody::Bytes(bs),
                                    )
                                    .await?;

                                (w, Ok((size, part)))
                            }));
                        }
                        None => {
                            self.state = State::Init(Box::pin(async move {
                                let upload_id = w.initiate_part().await;
                                (w, upload_id)
                            }));
                        }
                    }
                }
                State::Init(fut) => {
                    let (w, upload_id) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));
                    self.upload_id = Some(Arc::new(upload_id?));
                }
                State::Write(fut) => {
                    let (w, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));

                    let (written, part) = res?;
                    self.parts.push(part);
                    return Poll::Ready(Ok(written));
                }
                State::Close(_) => {
                    unreachable!(
                        "MultipartUploadWriter must not go into State:Close during poll_write"
                    )
                }
                State::Abort(_) => {
                    unreachable!(
                        "MultipartUploadWriter must not go into State:Abort during poll_write"
                    )
                }
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().expect("writer must be valid");
                    match &self.upload_id {
                        Some(upload_id) => {
                            let parts = self.parts.clone();
                            self.state = State::Close(Box::pin(async move {
                                let res = w.complete_part(&upload_id, &self.parts).await;
                                (w, res)
                            }));
                        }
                        None => return Poll::Ready(Ok(())),
                    }
                }
                State::Close(fut) => {
                    let (w, res) = futures::ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));
                    return Poll::Ready(res);
                }
                State::Init(_) => unreachable!(
                    "MultipartUploadWriter must not go into State:Init during poll_close"
                ),
                State::Write(_) => unreachable!(
                    "MultipartUploadWriter must not go into State:Write during poll_close"
                ),
                State::Abort(_) => unreachable!(
                    "MultipartUploadWriter must not go into State:Abort during poll_close"
                ),
            }
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().expect("writer must be valid");
                    match &self.upload_id {
                        Some(upload_id) => {
                            self.state = State::Close(Box::pin(async move {
                                let res = w.abort_part(&upload_id).await;
                                (w, res)
                            }));
                        }
                        None => return Poll::Ready(Ok(())),
                    }
                }
                State::Abort(fut) => {
                    let (w, res) = futures::ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));
                    return Poll::Ready(res);
                }
                State::Init(_) => unreachable!(
                    "MultipartUploadWriter must not go into State:Init during poll_abort"
                ),
                State::Write(_) => unreachable!(
                    "MultipartUploadWriter must not go into State:Write during poll_abort"
                ),
                State::Close(_) => unreachable!(
                    "MultipartUploadWriter must not go into State:Close during poll_abort"
                ),
            }
        }
    }
}
