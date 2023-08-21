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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use pin_project::pin_project;

use crate::*;

/// Streamer is a type erased [`Stream`].
pub type Streamer = Box<dyn Stream>;

/// Stream is the trait that OpenDAL accepts for sinking data.
///
/// It's nearly the same with [`futures::Stream`], but it satisfied
/// `Unpin` + `Send` + `Sync`. And the item is `Result<Bytes>`.
pub trait Stream: Unpin + Send + Sync {
    /// Poll next item `Result<Bytes>` from the stream.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>>;

    /// Reset this stream to the beginning.
    fn poll_reset(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>>;
}

impl Stream for () {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        unimplemented!("poll_next is required to be implemented for oio::Stream")
    }

    fn poll_reset(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let _ = cx;

        unimplemented!("poll_reset is required to be implemented for oio::Stream")
    }
}

/// `Box<dyn Stream>` won't implement `Stream` automatically.
/// To make Streamer work as expected, we must add this impl.
impl<T: Stream + ?Sized> Stream for Box<T> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        (**self).poll_next(cx)
    }

    fn poll_reset(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        (**self).poll_reset(cx)
    }
}

impl<T: Stream + ?Sized> Stream for Arc<std::sync::Mutex<T>> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.try_lock() {
            Ok(mut this) => this.poll_next(cx),
            Err(_) => Poll::Ready(Some(Err(Error::new(
                ErrorKind::Unexpected,
                "the stream is expected to have only one consumer, but it's not",
            )))),
        }
    }

    fn poll_reset(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.try_lock() {
            Ok(mut this) => this.poll_reset(cx),
            Err(_) => Poll::Ready(Err(Error::new(
                ErrorKind::Unexpected,
                "the stream is expected to have only one consumer, but it's not",
            ))),
        }
    }
}

impl<T: Stream + ?Sized> Stream for Arc<tokio::sync::Mutex<T>> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.try_lock() {
            Ok(mut this) => this.poll_next(cx),
            Err(_) => Poll::Ready(Some(Err(Error::new(
                ErrorKind::Unexpected,
                "the stream is expected to have only one consumer, but it's not",
            )))),
        }
    }

    fn poll_reset(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.try_lock() {
            Ok(mut this) => this.poll_reset(cx),
            Err(_) => Poll::Ready(Err(Error::new(
                ErrorKind::Unexpected,
                "the stream is expected to have only one consumer, but it's not",
            ))),
        }
    }
}

impl futures::Stream for dyn Stream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this: &mut dyn Stream = &mut *self;

        this.poll_next(cx)
    }
}

/// Impl StreamExt for all T: Stream
impl<T: Stream> StreamExt for T {}

/// Extension of [`Stream`] to make it easier for use.
pub trait StreamExt: Stream {
    /// Build a future for `poll_next`.
    fn next(&mut self) -> NextFuture<'_, Self> {
        NextFuture { inner: self }
    }

    /// Build a future for `poll_reset`.
    fn reset(&mut self) -> ResetFuture<'_, Self> {
        ResetFuture { inner: self }
    }
}

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct NextFuture<'a, T: Stream + Unpin + ?Sized> {
    inner: &'a mut T,
}

impl<T> Future for NextFuture<'_, T>
where
    T: Stream + Unpin + ?Sized,
{
    type Output = Option<Result<Bytes>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let this = self.project();
        Pin::new(this.inner).poll_next(cx)
    }
}

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct ResetFuture<'a, T: Stream + Unpin + ?Sized> {
    inner: &'a mut T,
}

impl<T> Future for ResetFuture<'_, T>
where
    T: Stream + Unpin + ?Sized,
{
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.project();
        Pin::new(this.inner).poll_reset(cx)
    }
}
