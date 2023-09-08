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
use std::task::{ready, Context, Poll};

use crate::raw::*;
use crate::*;

/// AppendObjectWrite is used to implement [`Write`] based on append
/// object. By implementing AppendObjectWrite, services don't need to
/// care about the details of buffering and uploading parts.
///
/// The layout after adopting [`AppendObjectWrite`]:
///
/// - Services impl `AppendObjectWrite`
/// - `AppendObjectWriter` impl `Write`
/// - Expose `AppendObjectWriter` as `Accessor::Writer`
#[async_trait]
pub trait AppendObjectWrite: Send + Sync + Unpin {
    /// Get the current offset of the append object.
    ///
    /// Returns `0` if the object is not exist.
    async fn offset(&self) -> Result<u64>;

    /// Append the data to the end of this object.
    async fn append(&self, offset: u64, size: u64, body: AsyncBody) -> Result<()>;
}

/// AppendObjectWriter will implements [`Write`] based on append object.
///
/// ## TODO
///
/// - Allow users to switch to un-buffered mode if users write 16MiB every time.
pub struct AppendObjectWriter<W: AppendObjectWrite> {
    state: State<W>,

    offset: Option<u64>,
}

enum State<W> {
    Idle(Option<W>),
    Offset(BoxFuture<'static, (W, Result<u64>)>),
    Append(BoxFuture<'static, (W, Result<usize>)>),
}

impl<W: AppendObjectWrite> AppendObjectWriter<W> {
    /// Create a new AppendObjectWriter.
    pub fn new(inner: W) -> Self {
        Self {
            state: State::Idle(Some(inner)),
            offset: None,
        }
    }

    async fn offset(&mut self) -> Result<u64> {
        if let Some(offset) = self.offset {
            return Ok(offset);
        }

        let offset = self.inner.offset().await?;
        self.offset = Some(offset);

        Ok(offset)
    }
}

#[async_trait]
impl<W> oio::Write for AppendObjectWriter<W>
where
    W: AppendObjectWrite,
{
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().expect("writer must be valid");
                    match self.offset {
                        Some(offset) => {
                            let size = bs.remaining();
                            let bs = bs.copy_to_bytes(size);

                            self.state = State::Append(Box::pin(async move {
                                w.append(offset, size as u64, AsyncBody::Bytes(bs)).await?;

                                (w, Ok(size))
                            }));
                        }
                        None => {
                            self.state = State::Offset(Box::pin(async move {
                                let offset = w.offset().await?;

                                (w, Ok(offset))
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
                    let (w, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));

                    let size = res?;
                    return Poll::Ready(Ok(size));
                }
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}
