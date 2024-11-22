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

use crate::raw::oio::Delete;
use crate::raw::*;
use crate::*;
use futures::Sink;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

pub struct Deleter {
    state: State,

    max_size: usize,
    cur_size: usize,
}

enum State {
    Idle(Option<oio::Deleter>),
    Flush(BoxedStaticFuture<(oio::Deleter, Result<usize>)>),
}

/// # Safety
///
/// Lister will only be accessed by `&mut Self`
unsafe impl Sync for Deleter {}

impl Deleter {
    /// Create a new lister.
    pub(crate) async fn create(acc: Accessor) -> Result<Self> {
        let max_size = acc.info().full_capability().delete_max_size.unwrap_or(1);
        let (_, deleter) = acc.delete().await?;

        Ok(Self {
            state: State::Idle(Some(deleter)),
            max_size,
            cur_size: 0,
        })
    }

    fn poll_ready_inner(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(deleter) => {
                    if self.cur_size < self.max_size {
                        return Poll::Ready(Ok(()));
                    } else {
                        let mut deleter = deleter
                            .take()
                            .expect("deleter must be valid while calling poll_ready");
                        let fut = async move {
                            let res = deleter.flush().await;
                            (deleter, res)
                        };
                        self.state = State::Flush(BoxedStaticFuture::new(fut));
                    }
                }
                State::Flush(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    return Poll::Ready(res.map(|flushed| self.cur_size -= flushed));
                }
            }
        }
    }

    fn poll_flush_inner(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                // We allow users to call flush even if the max size is not reached.
                State::Idle(deleter) => {
                    let mut deleter = deleter
                        .take()
                        .expect("deleter must be valid while calling poll_ready");
                    let fut = async move {
                        let res = deleter.flush().await;
                        (deleter, res)
                    };
                    self.state = State::Flush(BoxedStaticFuture::new(fut));
                }
                State::Flush(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    return Poll::Ready(res.map(|flushed| self.cur_size -= flushed));
                }
            }
        }
    }

    fn poll_close_inner(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(deleter) => {
                    let mut deleter = deleter
                        .take()
                        .expect("deleter must be valid while calling poll_ready");
                    let fut = async move {
                        let res = deleter.flush().await;
                        (deleter, res)
                    };
                    self.state = State::Flush(BoxedStaticFuture::new(fut));
                }
                State::Flush(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    self.cur_size -= res?;
                    // Keep looping if there are still pending delete operations.
                    if self.cur_size == 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}

impl Sink<String> for Deleter {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_ready_inner(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: String) -> Result<()> {
        match &mut self.state {
            State::Idle(deleter) if self.cur_size < self.max_size => {
                let mut deleter = deleter
                    .take()
                    .expect("deleter must be valid while calling start_send");
                let res = deleter.delete(&item, OpDelete::default());
                self.state = State::Idle(Some(deleter));
                self.cur_size += 1;
                res
            }
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                "Deleter is not ready to start_send, please poll_ready first",
            )),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush_inner(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_close_inner(cx)
    }
}

impl Sink<(String, OpDelete)> for Deleter {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_ready_inner(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: (String, OpDelete)) -> Result<()> {
        match &mut self.state {
            State::Idle(deleter) if self.cur_size < self.max_size => {
                let mut deleter = deleter
                    .take()
                    .expect("deleter must be valid while calling start_send");
                let res = deleter.delete(&item.0, item.1);
                self.state = State::Idle(Some(deleter));
                self.cur_size += 1;
                res
            }
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                "Deleter is not ready to start_send, please poll_ready first",
            )),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush_inner(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_close_inner(cx)
    }
}

impl Sink<Entry> for Deleter {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_ready_inner(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Entry) -> Result<()> {
        match &mut self.state {
            State::Idle(deleter) if self.cur_size < self.max_size => {
                let mut deleter = deleter
                    .take()
                    .expect("deleter must be valid while calling start_send");

                let mut args = OpDelete::default();
                if let Some(version) = item.metadata().version() {
                    args = args.with_version(version);
                }

                let res = deleter.delete(item.name(), args);
                self.state = State::Idle(Some(deleter));
                self.cur_size += 1;
                res
            }
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                "Deleter is not ready to start_send, please poll_ready first",
            )),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush_inner(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_close_inner(cx)
    }
}
