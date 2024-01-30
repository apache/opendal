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

use crate::raw::*;
use crate::*;

/// AppendWrite is used to implement [`Write`] based on append
/// object. By implementing AppendWrite, services don't need to
/// care about the details of buffering and uploading parts.
///
/// The layout after adopting [`AppendWrite`]:
///
/// - Services impl `AppendWrite`
/// - `AppendWriter` impl `Write`
/// - Expose `AppendWriter` as `Accessor::Writer`
///
/// ## Requirements
///
/// Services that implement `AppendWrite` must fulfill the following requirements:
///
/// - Must be a http service that could accept `AsyncBody`.
/// - Provide a way to get the current offset of the append object.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait AppendWrite: Send + Sync + Unpin + 'static {
    /// Get the current offset of the append object.
    ///
    /// Returns `0` if the object is not exist.
    async fn offset(&self) -> Result<u64>;

    /// Append the data to the end of this object.
    async fn append(&self, offset: u64, size: u64, body: AsyncBody) -> Result<()>;
}

/// AppendWriter will implements [`Write`] based on append object.
///
/// ## TODO
///
/// - Allow users to switch to un-buffered mode if users write 16MiB every time.
pub struct AppendWriter<W: AppendWrite> {
    state: State<W>,

    offset: Option<u64>,
}

enum State<W> {
    Idle(Option<W>),
    Offset(BoxedFuture<(W, Result<u64>)>),
    Append(BoxedFuture<(W, Result<usize>)>),
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this state.
unsafe impl<S: AppendWrite> Send for State<S> {}

/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl<S: AppendWrite> Sync for State<S> {}

impl<W: AppendWrite> AppendWriter<W> {
    /// Create a new AppendWriter.
    pub fn new(inner: W) -> Self {
        Self {
            state: State::Idle(Some(inner)),
            offset: None,
        }
    }
}

impl<W> oio::Write for AppendWriter<W>
where
    W: AppendWrite,
{
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().expect("writer must be valid");
                    match self.offset {
                        Some(offset) => {
                            let size = bs.remaining();
                            let bs = bs.bytes(size);

                            self.state = State::Append(Box::pin(async move {
                                let res = w.append(offset, size as u64, AsyncBody::Bytes(bs)).await;

                                (w, res.map(|_| size))
                            }));
                        }
                        None => {
                            self.state = State::Offset(Box::pin(async move {
                                let offset = w.offset().await;

                                (w, offset)
                            }));
                        }
                    }
                }
                State::Offset(fut) => {
                    let (w, offset) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));
                    self.offset = Some(offset?);
                }
                State::Append(fut) => {
                    let (w, size) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));

                    let size = size?;
                    // Update offset after succeed.
                    self.offset = self.offset.map(|offset| offset + size as u64);
                    return Poll::Ready(Ok(size));
                }
            }
        }
    }

    fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_abort(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}
