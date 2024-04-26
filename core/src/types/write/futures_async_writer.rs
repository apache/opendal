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

use std::io;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use futures::AsyncWrite;

use crate::raw::oio::Write;
use crate::raw::*;
use crate::*;

/// FuturesIoAsyncWriter is the adapter of [`AsyncWrite`] for [`Writer`].
///
/// Users can use this adapter in cases where they need to use [`AsyncWrite`] related trait.
///
/// FuturesIoAsyncWriter also implements [`Unpin`], [`Send`] and [`Sync`]
pub struct FuturesAsyncWriter {
    state: State,
    buf: oio::FlexBuf,
}

enum State {
    Idle(Option<oio::Writer>),
    Writing(BoxedStaticFuture<(oio::Writer, Result<usize>)>),
    Closing(BoxedStaticFuture<(oio::Writer, Result<()>)>),
}

/// # Safety
///
/// FuturesReader only exposes `&mut self` to the outside world, so it's safe to be `Sync`.
unsafe impl Sync for State {}

impl FuturesAsyncWriter {
    /// NOTE: don't allow users to create directly.
    #[inline]
    pub(crate) fn new(w: oio::Writer) -> Self {
        FuturesAsyncWriter {
            state: State::Idle(Some(w)),
            buf: oio::FlexBuf::new(256 * 1024),
        }
    }
}

impl AsyncWrite for FuturesAsyncWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                State::Idle(w) => {
                    let n = this.buf.put(buf);
                    if n > 0 {
                        return Poll::Ready(Ok(n));
                    }

                    let bs = this.buf.get().expect("frozen buffer must be valid");
                    let mut w = w.take().expect("writer must be valid");
                    let fut = async move {
                        let res = w.write(Buffer::from(bs)).await;
                        (w, res)
                    };
                    this.state = State::Writing(Box::pin(fut));
                }
                State::Writing(ref mut fut) => {
                    let (w, res) = ready!(fut.as_mut().poll(cx));
                    this.state = State::Idle(Some(w));
                    match res {
                        Ok(n) => {
                            this.buf.advance(n);
                        }
                        Err(err) => return Poll::Ready(Err(format_std_io_error(err))),
                    };
                }
                _ => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "another io operation is in progress",
                    )))
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                State::Idle(w) => {
                    // Make sure buf has been frozen.
                    this.buf.freeze();
                    let Some(bs) = this.buf.get() else {
                        return Poll::Ready(Ok(()));
                    };

                    let mut w = w.take().expect("writer must be valid");
                    let fut = async move {
                        let res = w.write(Buffer::from(bs)).await;
                        (w, res)
                    };
                    this.state = State::Writing(Box::pin(fut));
                }
                State::Writing(ref mut fut) => {
                    let (w, res) = ready!(fut.as_mut().poll(cx));
                    this.state = State::Idle(Some(w));
                    match res {
                        Ok(n) => {
                            this.buf.advance(n);
                        }
                        Err(err) => return Poll::Ready(Err(format_std_io_error(err))),
                    };
                }
                _ => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "another io operation is in progress",
                    )))
                }
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.buf.get().is_some() {
            return self.poll_flush(cx);
        }

        let this = self.get_mut();

        loop {
            match &mut this.state {
                State::Idle(w) => {
                    let mut w = w.take().expect("writer must be valid");
                    let fut = async move {
                        let res = w.close().await;
                        (w, res)
                    };
                    this.state = State::Closing(Box::pin(fut));
                }
                State::Closing(ref mut fut) => {
                    let (w, res) = ready!(fut.as_mut().poll(cx));
                    this.state = State::Idle(Some(w));
                    return Poll::Ready(res.map_err(format_std_io_error));
                }
                _ => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "another io operation is in progress",
                    )))
                }
            }
        }
    }
}
