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

//! Futures provides the futures generated by [`Operator`]
//!
//! By using futures, users can add more options for operation.

use bytes::Bytes;

use std::mem;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::Future;
use futures::FutureExt;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// OperatorFuture is the future generated by [`Operator`].
///
/// The future will consume all the input to generate a result.
pub(crate) enum OperatorFuture<T, F> {
    /// Idle state, waiting for the future to be polled
    Idle(
        /// The accessor to the underlying object storage
        FusedAccessor,
        /// The path of string
        String,
        /// The input args
        T,
        /// The function which will move all the args and return a static future
        fn(FusedAccessor, String, T) -> BoxFuture<'static, Result<F>>,
    ),
    /// Polling state, waiting for the future to be ready
    Poll(BoxFuture<'static, Result<F>>),
    /// Empty state, the future has been polled and completed or
    /// something is broken during state switch.
    Empty,
}

impl<T, F> OperatorFuture<T, F> {
    pub fn new(
        inner: FusedAccessor,
        path: String,
        args: T,
        f: fn(FusedAccessor, String, T) -> BoxFuture<'static, Result<F>>,
    ) -> Self {
        OperatorFuture::Idle(inner, path, args, f)
    }

    fn map_args(self, f: impl FnOnce(T) -> T) -> Self {
        match self {
            OperatorFuture::Idle(inner, path, args, func) => {
                OperatorFuture::Idle(inner, path, f(args), func)
            }
            _ => unreachable!("future has been polled and should not be changed again"),
        }
    }
}

impl<T, F> Future for OperatorFuture<T, F>
where
    T: Unpin,
    F: Unpin,
{
    type Output = Result<F>;

    /// We will move the self state out by replace a `Empty` into.
    ///
    /// - If the future is `Idle`, we will move all args out to build
    ///   a new future, and update self state to `Poll`.
    /// - If the future is `Poll`, we will poll the inner future
    ///   - If future is `Ready`, we will return it directly with
    ///     self state is `Empty`
    ///   - If future is `Pending`, we will set self state to `Poll`
    ///     and wait for next poll
    ///
    /// In general, `Empty` state should not be polled.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        *self = match mem::replace(self.as_mut().get_mut(), OperatorFuture::Empty) {
            OperatorFuture::Idle(inner, path, args, f) => {
                OperatorFuture::Poll(f(inner, path, args))
            }
            OperatorFuture::Poll(mut fut) => match fut.as_mut().poll(cx) {
                Poll::Pending => OperatorFuture::Poll(fut),
                Poll::Ready(v) => return Poll::Ready(v),
            },
            OperatorFuture::Empty => {
                panic!("future polled after completion");
            }
        };
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Future that generated by [`Operator::stat_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FutureStat(pub(crate) OperatorFuture<OpStat, Metadata>);

impl FutureStat {
    /// Set the If-Match for this operation.
    pub fn if_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_if_match(v));
        self
    }

    /// Set the If-None-Match for this operation.
    pub fn if_none_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_if_none_match(v));
        self
    }
}

impl Future for FutureStat {
    type Output = Result<Metadata>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operator::append_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FutureAppend(pub(crate) OperatorFuture<(OpAppend, Bytes), ()>);

impl FutureAppend {
    /// Set the content type of option
    pub fn content_type(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_content_type(v), bs));
        self
    }

    /// Set the content disposition of option
    pub fn content_disposition(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_content_disposition(v), bs));
        self
    }

    /// Set the cache control of option
    pub fn cache_control(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_cache_control(v), bs));
        self
    }
}

impl Future for FutureAppend {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operation.appender_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FutureAppender(pub(crate) OperatorFuture<OpAppend, Appender>);

impl FutureAppender {
    /// Set the content type for this operation.
    pub fn content_type(mut self, content_type: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_content_type(content_type));
        self
    }

    /// Set the content disposition for this operation.
    pub fn content_disposition(mut self, content_disposition: &str) -> Self {
        self.0 = self
            .0
            .map_args(|args| args.with_content_disposition(content_disposition));
        self
    }

    /// Set the cache control for this operation.
    pub fn cache_control(mut self, cache_control: &str) -> Self {
        self.0 = self
            .0
            .map_args(|args| args.with_cache_control(cache_control));
        self
    }
}

impl Future for FutureAppender {
    type Output = Result<Appender>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operator::presign_read_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FuturePresignRead(pub(crate) OperatorFuture<(OpRead, Duration), PresignedRequest>);

impl FuturePresignRead {
    /// Create a new OpRead with range.
    pub fn range(mut self, v: BytesRange) -> Self {
        self.0 = self.0.map_args(|(args, dur)| (args.with_range(v), dur));
        self
    }

    /// Sets the content-disposition header that should be send back by the remote read operation.
    pub fn override_content_disposition(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, dur)| (args.with_override_content_disposition(v), dur));
        self
    }

    /// Sets the cache-control header that should be send back by the remote read operation.
    pub fn override_cache_control(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, dur)| (args.with_override_cache_control(v), dur));
        self
    }

    /// Set the If-Match of the option
    pub fn if_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|(args, dur)| (args.with_if_match(v), dur));
        self
    }

    /// Set the If-None-Match of the option
    pub fn if_none_match(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, dur)| (args.with_if_none_match(v), dur));
        self
    }
}

impl Future for FuturePresignRead {
    type Output = Result<PresignedRequest>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operator::presign_read_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FuturePresignWrite(pub(crate) OperatorFuture<(OpWrite, Duration), PresignedRequest>);

impl FuturePresignWrite {
    /// Set the content length of op.
    ///
    /// If the content length is not set, the content length will be
    /// calculated automatically by buffering part of data.
    ///
    pub fn content_length(mut self, v: u64) -> Self {
        self.0 = self
            .0
            .map_args(|(args, dur)| (args.with_content_length(v), dur));
        self
    }

    /// Set the content type of option
    pub fn content_type(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, dur)| (args.with_content_type(v), dur));
        self
    }

    /// Set the content disposition of option
    pub fn content_disposition(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, dur)| (args.with_content_disposition(v), dur));
        self
    }

    /// Set the content type of option
    pub fn cache_control(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, dur)| (args.with_cache_control(v), dur));
        self
    }
}

impl Future for FuturePresignWrite {
    type Output = Result<PresignedRequest>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operator::read_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FutureRead(pub(crate) OperatorFuture<OpRead, Vec<u8>>);

impl FutureRead {
    /// Set the range header for this operation.
    pub fn range(mut self, range: impl RangeBounds<u64>) -> Self {
        self.0 = self.0.map_args(|args| args.with_range(range.into()));
        self
    }

    /// Sets the content-disposition header that should be send back by the remote read operation.
    pub fn override_content_disposition(mut self, content_disposition: &str) -> Self {
        self.0 = self
            .0
            .map_args(|args| args.with_override_content_disposition(content_disposition));
        self
    }

    /// Sets the cache-control header that should be send back by the remote read operation.
    pub fn override_cache_control(mut self, cache_control: &str) -> Self {
        self.0 = self
            .0
            .map_args(|args| args.with_override_cache_control(cache_control));
        self
    }

    /// Set the If-Match for this operation.
    pub fn if_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_if_match(v));
        self
    }

    /// Set the If-None-Match for this operation.
    pub fn if_none_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_if_none_match(v));
        self
    }
}

impl Future for FutureRead {
    type Output = Result<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operator::reader_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FutureReader(pub(crate) OperatorFuture<OpRead, Reader>);

impl FutureReader {
    /// Set the range header for this operation.
    pub fn range(mut self, range: impl RangeBounds<u64>) -> Self {
        self.0 = self.0.map_args(|args| args.with_range(range.into()));
        self
    }

    /// Sets the content-disposition header that should be send back by the remote read operation.
    pub fn override_content_disposition(mut self, content_disposition: &str) -> Self {
        self.0 = self
            .0
            .map_args(|args| args.with_override_content_disposition(content_disposition));
        self
    }

    /// Sets the cache-control header that should be send back by the remote read operation.
    pub fn override_cache_control(mut self, cache_control: &str) -> Self {
        self.0 = self
            .0
            .map_args(|args| args.with_override_cache_control(cache_control));
        self
    }

    /// Set the If-Match for this operation.
    pub fn if_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_if_match(v));
        self
    }

    /// Set the If-None-Match for this operation.
    pub fn if_none_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_if_none_match(v));
        self
    }
}

impl Future for FutureReader {
    type Output = Result<Reader>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operator::write_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FutureWrite(pub(crate) OperatorFuture<(OpWrite, Bytes), ()>);

impl FutureWrite {
    /// Set the content length of op.
    ///
    /// If the content length is not set, the content length will be
    /// calculated automatically by buffering part of data.
    ///
    pub fn content_length(mut self, v: u64) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_content_length(v), bs));
        self
    }

    /// Set the content type of option
    pub fn content_type(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_content_type(v), bs));
        self
    }

    /// Set the content disposition of option
    pub fn content_disposition(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_content_disposition(v), bs));
        self
    }

    /// Set the content type of option
    pub fn cache_control(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_cache_control(v), bs));
        self
    }
}

impl Future for FutureWrite {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operator::writer_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FutureWriter(pub(crate) OperatorFuture<OpWrite, Writer>);

impl FutureWriter {
    /// Set the content length of op.
    ///
    /// If the content length is not set, the content length will be
    /// calculated automatically by buffering part of data.
    ///
    pub fn content_length(mut self, v: u64) -> Self {
        self.0 = self.0.map_args(|args| args.with_content_length(v));
        self
    }

    /// Set the content type of option
    pub fn content_type(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_content_type(v));
        self
    }

    /// Set the content disposition of option
    pub fn content_disposition(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_content_disposition(v));
        self
    }

    /// Set the content type of option
    pub fn cache_control(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_cache_control(v));
        self
    }
}

impl Future for FutureWriter {
    type Output = Result<Writer>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Future that generated by [`Operator::list_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FutureList(pub(crate) OperatorFuture<OpList, Lister>);

impl FutureList {
    /// Change the limit of this list operation.
    pub fn limit(mut self, v: usize) -> Self {
        self.0 = self.0.map_args(|args| args.with_limit(v));
        self
    }

    /// Change the start_after of this list operation.
    pub fn start_after(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_start_after(v));
        self
    }

    /// Change the delimiter. The default delimiter is "/"
    pub fn delimiter(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_delimiter(v));
        self
    }
}

impl Future for FutureList {
    type Output = Result<Lister>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}
