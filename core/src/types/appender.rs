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

use std::fmt::Display;
use std::io;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::AsyncWrite;
use futures::FutureExt;

use crate::raw::oio::Append;
use crate::raw::*;
use crate::*;

/// Appender is designed to append data into given path in an asynchronous
/// manner.
///
/// ## Notes
///
/// Please make sure either `close`` has been called before
/// dropping the appender otherwise the data could be lost.
///
/// ## Notes
///
/// Appender always append data into the end of the file, so it's not
/// possible to overwrite existing data.
///
/// Appender did not know the size of the data to be written, so it will
/// always be `unsized`.
pub struct Appender {
    state: State,
}

/// # Safety
///
/// Appender will only be accessed by `&mut Self`
unsafe impl Sync for Appender {}

impl Appender {
    /// Create a new appender.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) async fn create(acc: FusedAccessor, path: &str, op: OpAppend) -> Result<Self> {
        let (_, a) = acc.append(path, op).await?;

        Ok(Appender {
            state: State::Idle(Some(a)),
        })
    }

    /// Write into inner appender.
    pub async fn append(&mut self, bs: impl Into<Bytes>) -> Result<()> {
        if let State::Idle(Some(a)) = &mut self.state {
            a.append(bs.into()).await
        } else {
            unreachable!(
                "appender state invalid while write, expect Idle, actual {}",
                self.state
            );
        }
    }

    /// Close the appender and make sure all data have been committed.
    ///
    /// ## Notes
    ///
    /// Close should only be called when the appender is not closed,
    /// otherwise an unexpected error could be returned.
    pub async fn close(&mut self) -> Result<()> {
        if let State::Idle(Some(a)) = &mut self.state {
            a.close().await
        } else {
            unreachable!(
                "appender state invalid while close, expect Idle, actual {}",
                self.state
            );
        }
    }
}

#[allow(dead_code)]
enum State {
    Idle(Option<oio::Appender>),
    Write(BoxFuture<'static, Result<(usize, oio::Appender)>>),
    Close(BoxFuture<'static, Result<oio::Appender>>),
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Idle(_) => write!(f, "Idle"),
            State::Write(_) => write!(f, "Write"),
            State::Close(_) => write!(f, "Close"),
        }
    }
}

impl AsyncWrite for Appender {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(a) => {
                    let mut a = a
                        .take()
                        .expect("invalid state of appender: poll_write with State::Idle");
                    let bs = Bytes::from(buf.to_vec());
                    let size = bs.len();
                    let ut = async move {
                        a.append(bs).await?;
                        Ok((size, a))
                    };
                    self.state = State::Write(Box::pin(ut));
                }
                State::Write(fut) => match ready!(fut.poll_unpin(cx)) {
                    Ok((size, a)) => {
                        self.state = State::Idle(Some(a));
                        return Poll::Ready(Ok(size));
                    }
                    Err(err) => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
                },

                State::Close(_) => {
                    unreachable!("invalid state of appender: poll_write with State::Close")
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(a) => {
                    let mut a = a
                        .take()
                        .expect("invalid state of appender: Idle state with empty append");
                    let fut = async move {
                        a.close().await?;
                        Ok(a)
                    };
                    self.state = State::Close(Box::pin(fut));
                }
                State::Write(_) => {
                    unreachable!("invalid state of appender: poll_close with State::Write")
                }
                State::Close(fut) => match ready!(fut.poll_unpin(cx)) {
                    Ok(a) => {
                        self.state = State::Idle(Some(a));
                        return Poll::Ready(Ok(()));
                    }
                    Err(err) => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
                },
            }
        }
    }
}

impl tokio::io::AsyncWrite for Appender {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(a) => {
                    let mut a = a
                        .take()
                        .expect("invalid state of appender: Idle state with empty append");
                    let bs = Bytes::from(buf.to_vec());
                    let size = bs.len();
                    let fut = async move {
                        a.append(bs).await?;
                        Ok((size, a))
                    };
                    self.state = State::Write(Box::pin(fut));
                }
                State::Write(fut) => match ready!(fut.poll_unpin(cx)) {
                    Ok((size, a)) => {
                        self.state = State::Idle(Some(a));
                        return Poll::Ready(Ok(size));
                    }
                    Err(err) => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
                },
                State::Close(_) => {
                    unreachable!("invalid state of appender: poll_write with State::Close")
                }
            };
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(a) => {
                    let mut a = a
                        .take()
                        .expect("invalid state of appender: Idle state with empty append");
                    let fut = async move {
                        a.close().await?;
                        Ok(a)
                    };
                    self.state = State::Close(Box::pin(fut));
                }
                State::Write(_) => {
                    unreachable!("invalid state of appender: poll_close with State::Write")
                }
                State::Close(fut) => match ready!(fut.poll_unpin(cx)) {
                    Ok(a) => {
                        self.state = State::Idle(Some(a));
                        return Poll::Ready(Ok(()));
                    }
                    Err(err) => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
                },
            }
        }
    }
}
