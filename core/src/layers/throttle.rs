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

use std::io::SeekFrom;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Duration;
use governor::clock::{Clock, DefaultClock, QuantaInstant};
use governor::middleware::{RateLimitingMiddleware, StateInformationMiddleware, StateSnapshot};
use governor::state::{InMemoryState, NotKeyed};
use governor::{NegativeMultiDecision, NotUntil, Quota, RateLimiter, RatelimitedStream};

use crate::raw::*;
use crate::*;

/// ThrottleLayer can help users to control the max bandwidth that used by OpenDAL.
#[derive(Clone)]
pub struct ThrottleLayer {
    max_burst: u32,
}

impl ThrottleLayer {
    /// Create a new ThrottleLayer will specify quota
    pub fn new(max_burst: u32) -> Self {
        assert!(max_burst > 0);
        Self {
            max_burst
        }
    }
}

impl<A: Accessor> Layer<A> for ThrottleLayer {
    type LayeredAccessor = ThrottleAccessor<A>;

    fn layer(&self, accessor: A) -> Self::Output {
        let rate_limiter = Arc::new(
            RateLimiter::direct(Quota::per_second(NonZeroU32::new(self.max_burst).unwrap()))
                // More info about middleware: https://docs.rs/governor/latest/governor/middleware/index.html
                .with_middleware::<StateInformationMiddleware>(),
        );
        ThrottleAccessor {
            inner: accessor,
            rate_limiter,
        }
    }
}

type SharedRateLimiter =
    Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, StateInformationMiddleware>>;

#[derive(Debug, Clone)]
pub struct ThrottleAccessor<A: Accessor> {
    inner: A,
    rate_limiter: SharedRateLimiter,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for ThrottleAccessor<A> {
    type Inner = A;
    type Reader = ThrottleWrapper<A::Reader>;
    type BlockingReader = ThrottleWrapper<A::BlockingReader>;
    type Writer = ThrottleWrapper<A::Writer>;
    type BlockingWriter = ThrottleWrapper<A::BlockingWriter>;
    type Appender = ThrottleWrapper<A::Appender>;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ThrottleWrapper::new(r, limiter)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ThrottleWrapper::new(w, limiter)))
    }

    async fn append(&self, path: &str, args: OpAppend) -> Result<(RpAppend, Self::Appender)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .append(path, args)
            .await
            .map(|(rp, a)| (rp, ThrottleWrapper::new(a, limiter)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner.list(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, ThrottleWrapper::new(r, limiter)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| (rp, ThrottleWrapper::new(w, limiter)))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }
}

pub struct ThrottleWrapper<R> {
    inner: R,
    limiter: SharedRateLimiter,
}

impl<R> ThrottleWrapper<R> {
    pub fn new(inner: R, rate_limiter: SharedRateLimiter) -> Self {
        Self {
            inner,
            limiter: rate_limiter,
        }
    }
}

impl<R: oio::Read> oio::Read for ThrottleWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        // TODO: How can we handle buffer reads with a limiter?
        self.inner.poll_read(cx, buf)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for ThrottleWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // TODO: How can we handle buffer reads with a limiter?
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        self.inner.next()
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for ThrottleWrapper<R> {
    async fn write(&mut self, mut bs: Bytes) -> Result<()> {
        // Note: for self.inner.write(bs), it's possible that the given bs length is less than the total content length.
        // Users will call write multiple times to write the whole data.

        let mut buf_length = NonZeroU32::new(bs.len() as u32).unwrap();

        while !bs.is_empty() {
            match self.limiter.check_n(buf_length) {
                Ok(_) => self.inner.write(bs.split_to(buf_length as usize)).await,
                Err(negative) => match negative {
                    // the query is valid but the Decider can not accommodate them.
                    NegativeMultiDecision::BatchNonConforming(_, not_until) => {
                        // We can either change buf_length or we can wait until the rate limiter has enough capacity to accommodate the request.
                        let wait_time = not_until
                            .wait_time_from(DefaultClock::default().now())
                            .as_secs();
                        tokio::time::sleep(Duration::from_secs(wait_time)).await;
                    }
                    // the query was invalid as the rate limit parameters can "never" accommodate the number of cells queried for.
                    NegativeMultiDecision::InsufficientCapacity(_) => {
                        // Allow a single cell through the rate limiter.
                        // If the rate limit is reached, check returns information about the earliest time that a cell might be allowed through again.
                        buf_length: NonZeroU32 = match self.limiter.check() {
                            Ok(snapshot) => {
                                /// If this state snapshot is based on a negative rate limiting
                                /// outcome, this method returns 0. (So unwrap() is safe here...right?)
                                NonZeroU32::new(snapshot.remaining_burst_capacity()).unwrap()
                            }
                            Err(negative) => {
                                // not a single cell can be allowed through the rate limiter for now
                                let wait_time =
                                    negative.wait_time_from(DefaultClock::default().now());
                                tokio::time::sleep(Duration::from_secs(wait_time)).await;
                            }
                        }
                    }
                },
            }
        }
        return Ok(());
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for ThrottleWrapper<R> {
    fn write(&mut self, mut bs: Bytes) -> Result<()> {
        // Note: for self.inner.write(bs), it's possible that the given bs length is less than the total content length.
        // Users will call write multiple times to write the whole data.

        let mut buf_length = NonZeroU32::new(bs.len() as u32).unwrap();

        while !bs.is_empty() {
            match self.limiter.check_n(buf_length) {
                Ok(_) => self.inner.write(bs.split_to(buf_length as usize)),
                Err(negative) => match negative {
                    // the query is valid but the Decider can not accommodate them.
                    NegativeMultiDecision::BatchNonConforming(_, not_until) => {
                        // We can either change buf_length or we can wait until the rate limiter has enough capacity to accommodate the request.
                        let wait_time = not_until
                            .wait_time_from(DefaultClock::default().now())
                            .as_secs();
                        std::thread::sleep(Duration::from_secs(wait_time));
                    }
                    // the query was invalid as the rate limit parameters can "never" accommodate the number of cells queried for.
                    NegativeMultiDecision::InsufficientCapacity(_) => {
                        // Allow a single cell through the rate limiter.
                        // If the rate limit is reached, check returns information about the earliest time that a cell might be allowed through again.
                        buf_length: NonZeroU32 = match self.limiter.check() {
                            Ok(snapshot) => {
                                /// If this state snapshot is based on a negative rate limiting
                                /// outcome, this method returns 0. (So unwrap() is safe here...right?)
                                NonZeroU32::new(snapshot.remaining_burst_capacity()).unwrap()
                            }
                            Err(negative) => {
                                // not a single cell can be allowed through the rate limiter for now
                                let wait_time =
                                    negative.wait_time_from(DefaultClock::default().now());
                                std::thread::sleep(Duration::from_secs(wait_time));
                            }
                        }
                    }
                },
            }
        }
        return Ok(());
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }
}

#[async_trait]
impl<R: oio::Append> oio::Append for ThrottleWrapper<R> {
    async fn append(&mut self, mut bs: Bytes) -> Result<()> {
        // Note: for self.inner.write(bs), it's possible that the given bs length is less than the total content length.
        // Users will call write multiple times to write the whole data.

        let mut buf_length = NonZeroU32::new(bs.len() as u32).unwrap();

        while !bs.is_empty() {
            match self.limiter.check_n(buf_length) {
                Ok(_) => self.inner.append(bs.split_to(buf_length as usize)),
                Err(negative) => match negative {
                    // the query is valid but the Decider can not accommodate them.
                    NegativeMultiDecision::BatchNonConforming(_, not_until) => {
                        // We can either change buf_length or we can wait until the rate limiter has enough capacity to accommodate the request.
                        let wait_time = not_until
                            .wait_time_from(DefaultClock::default().now())
                            .as_secs();
                        tokio::time::sleep(Duration::from_secs(wait_time)).await;
                    }
                    // the query was invalid as the rate limit parameters can "never" accommodate the number of cells queried for.
                    NegativeMultiDecision::InsufficientCapacity(_) => {
                        // Allow a single cell through the rate limiter.
                        // If the rate limit is reached, check returns information about the earliest time that a cell might be allowed through again.
                        buf_length: NonZeroU32 = match self.limiter.check() {
                            Ok(snapshot) => {
                                /// If this state snapshot is based on a negative rate limiting
                                /// outcome, this method returns 0. (So unwrap() is safe here...right?)
                                NonZeroU32::new(snapshot.remaining_burst_capacity()).unwrap()
                            }
                            Err(negative) => {
                                // not a single cell can be allowed through the rate limiter for now
                                let wait_time =
                                    negative.wait_time_from(DefaultClock::default().now());
                                tokio::time::sleep(Duration::from_secs(wait_time)).await;
                            }
                        }
                    }
                },
            }
        }
        return Ok(());
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}
