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
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::BytesMut;
use bytes::{Buf, Bytes};

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
}

impl Stream for () {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        unimplemented!("poll_next is required to be implemented for oio::Stream")
    }
}

impl Stream for Bytes {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.has_remaining() {
            Poll::Ready(Some(Ok(self.copy_to_bytes(self.remaining()))))
        } else {
            Poll::Ready(None)
        }
    }
}

/// `Box<dyn Stream>` won't implement `Stream` automatically.
/// To make Streamer work as expected, we must add this impl.
impl<T: Stream + ?Sized> Stream for Box<T> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        (**self).poll_next(cx)
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

    /// Chain this stream with another stream.
    fn chain<S>(self, other: S) -> Chain<Self, S>
    where
        Self: Sized,
        S: Stream,
    {
        Chain {
            first: Some(self),
            second: other,
        }
    }

    /// Collect all items from this stream into a single bytes.
    fn collect(self) -> Collect<Self>
    where
        Self: Sized,
    {
        Collect {
            stream: self,
            buf: BytesMut::new(),
        }
    }
}

pub struct NextFuture<'a, T: Stream + Unpin + ?Sized> {
    inner: &'a mut T,
}

impl<T> Future for NextFuture<'_, T>
where
    T: Stream + Unpin + ?Sized,
{
    type Output = Option<Result<Bytes>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        self.inner.poll_next(cx)
    }
}

/// Stream for the [`chain`](StreamExt::chain) method.
#[must_use = "streams do nothing unless polled"]
pub struct Chain<S1: Stream, S2: Stream> {
    first: Option<S1>,
    second: S2,
}

impl<S1: Stream, S2: Stream> Stream for Chain<S1, S2> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if let Some(first) = self.first.as_mut() {
            if let Some(item) = ready!(first.poll_next(cx)) {
                return Poll::Ready(Some(item));
            }

            self.first = None;
        }
        self.second.poll_next(cx)
    }
}

/// Stream for the [`collect`](StreamExt::collect) method.
#[must_use = "streams do nothing unless polled"]
pub struct Collect<S> {
    stream: S,
    buf: BytesMut,
}

impl<S> Future for Collect<S>
where
    S: Stream,
{
    type Output = Result<Bytes>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut();
        loop {
            match ready!(this.stream.poll_next(cx)) {
                Some(Ok(bs)) => this.buf.extend(bs),
                Some(Err(err)) => return Poll::Ready(Err(err)),
                None => return Poll::Ready(Ok(self.buf.split().freeze())),
            }
        }
    }
}
