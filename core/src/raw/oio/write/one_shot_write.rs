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

/// OneShotWrite is used to implement [`Write`] based on one shot operation.
/// By implementing OneShotWrite, services don't need to care about the details.
///
/// For example, S3 `PUT Object` and fs `write_all`.
///
/// The layout after adopting [`OneShotWrite`]:
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait OneShotWrite: Send + Sync + Unpin + 'static {
    /// write_once write all data at once.
    ///
    /// Implementations should make sure that the data is written correctly at once.
    async fn write_once(&self, bs: &dyn oio::WriteBuf) -> Result<()>;
}

/// OneShotWrite is used to implement [`Write`] based on one shot.
pub struct OneShotWriter<W: OneShotWrite> {
    state: State<W>,
    buffer: Option<oio::ChunkedBytes>,
}

enum State<W> {
    Idle(Option<W>),
    Write(BoxedStaticFuture<(W, Result<()>)>),
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this state.
unsafe impl<S: OneShotWrite> Send for State<S> {}

/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl<S: OneShotWrite> Sync for State<S> {}

impl<W: OneShotWrite> OneShotWriter<W> {
    /// Create a new one shot writer.
    pub fn new(inner: W) -> Self {
        Self {
            state: State::Idle(Some(inner)),
            buffer: None,
        }
    }
}

impl<W: OneShotWrite> oio::Write for OneShotWriter<W> {
    fn poll_write(&mut self, _: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        match &mut self.state {
            State::Idle(_) => match &self.buffer {
                Some(_) => Poll::Ready(Err(Error::new(
                    ErrorKind::Unsupported,
                    "OneShotWriter doesn't support multiple write",
                ))),
                None => {
                    let size = bs.remaining();
                    let bs = bs.vectored_bytes(size);
                    self.buffer = Some(oio::ChunkedBytes::from_vec(bs));
                    Poll::Ready(Ok(size))
                }
            },
            State::Write(_) => {
                unreachable!("OneShotWriter must not go into State::Write during poll_write")
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().expect("writer must be valid");

                    match self.buffer.clone() {
                        Some(bs) => {
                            let fut = Box::pin(async move {
                                let res = w.write_once(&bs).await;

                                (w, res)
                            });
                            self.state = State::Write(fut);
                        }
                        None => {
                            let fut = Box::pin(async move {
                                let res = w.write_once(&"".as_bytes()).await;

                                (w, res)
                            });
                            self.state = State::Write(fut);
                        }
                    };
                }
                State::Write(fut) => {
                    let (w, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));
                    return Poll::Ready(res);
                }
            }
        }
    }

    fn poll_abort(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        self.buffer = None;
        Poll::Ready(Ok(()))
    }
}
