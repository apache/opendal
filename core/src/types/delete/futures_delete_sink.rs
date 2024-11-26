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

use crate::raw::*;
use crate::*;
use futures::Sink;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

pub struct FuturesDeleteSink {
    state: State,
}

enum State {
    Idle(Option<Deleter>),
    Delete(BoxedStaticFuture<(Deleter, Result<()>)>),
    Flush(BoxedStaticFuture<(Deleter, Result<usize>)>),
    Close(BoxedStaticFuture<(Deleter, Result<()>)>),
}

impl FuturesDeleteSink {
    #[inline]
    pub(super) fn new(deleter: Deleter) -> Self {
        Self {
            state: State::Idle(Some(deleter)),
        }
    }
}

impl<T: IntoDeleteInput> Sink<T> for FuturesDeleteSink {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            return match &mut self.state {
                State::Idle(_) => Poll::Ready(Ok(())),
                State::Delete(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    Poll::Ready(res.map(|_| ()))
                }
                State::Flush(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    Poll::Ready(res.map(|_| ()))
                }
                State::Close(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    Poll::Ready(res.map(|_| ()))
                }
            };
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<()> {
        match &mut self.state {
            State::Idle(deleter) => {
                let mut deleter = deleter.take().ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "FuturesDeleteSink has been closed or errored",
                    )
                })?;
                let input = item.into_delete_input();
                let fut = async move {
                    let res = deleter.delete(input).await;
                    (deleter, res)
                };
                self.state = State::Delete(Box::pin(fut));
                Ok(())
            }
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                "FuturesDeleteSink is not ready to send, please poll_ready first",
            )),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(deleter) => {
                    let mut deleter = deleter.take().ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "FuturesDeleteSink has been closed or errored",
                        )
                    })?;
                    let fut = async move {
                        let res = deleter.flush().await;
                        (deleter, res)
                    };
                    self.state = State::Flush(Box::pin(fut));
                    return Poll::Ready(Ok(()));
                }
                State::Delete(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    continue;
                }
                State::Flush(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    let _ = res?;
                    return Poll::Ready(Ok(()));
                }
                State::Close(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    res?;
                    continue;
                }
            };
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(deleter) => {
                    let mut deleter = deleter.take().ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "FuturesDeleteSink has been closed or errored",
                        )
                    })?;
                    let fut = async move {
                        let res = deleter.close().await;
                        (deleter, res)
                    };
                    self.state = State::Close(Box::pin(fut));
                    return Poll::Ready(Ok(()));
                }
                State::Delete(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    res?;
                    continue;
                }
                State::Flush(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    res?;
                    continue;
                }
                State::Close(fut) => {
                    let (deleter, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(deleter));
                    return Poll::Ready(res);
                }
            };
        }
    }
}
