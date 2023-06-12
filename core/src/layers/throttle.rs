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
use governor::middleware::{
    NoOpMiddleware, RateLimitingMiddleware, StateInformationMiddleware, StateSnapshot,
};
use governor::state::{InMemoryState, NotKeyed};
use governor::{NegativeMultiDecision, NotUntil, Quota, RateLimiter, RatelimitedStream};

use crate::raw::*;
use crate::*;

/// Add a bandwidth rate limiter to the underlying services.
///
/// # Throttle
///
/// There are several algorithms when it come to rate limiting techniques.
/// This throttle layer uses Generic Cell Rate Algorithm (GCRA) provided by
/// [Governor](https://docs.rs/governor/latest/governor/index.html).
/// By setting the `replenish_byte_rate` and `max_burst_byte`, we can control the byte flow rate of underlying services.
///
/// # Note
///
/// When setting the ThrottleLayer, always consider the largest possible operation size as the burst size,
/// as **the burst size should be larger than any possible byte length to allow it to pass through**.
///
/// Read more about [Quota](https://docs.rs/governor/latest/governor/struct.Quota.html#examples)
///
/// # Examples
///
/// This example limits bandwidth to 10 KiB/s and burst size to 10 MiB.
/// ```
/// use anyhow::Result;
/// use opendal::layers::ThrottleLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(ThrottleLayer::new(10 * 1024, 10000 * 1024))
///     .finish();
/// ```
#[derive(Clone)]
pub struct ThrottleLayer {
    replenish_byte_rate: u32,
    max_burst_byte: u32,
}

impl ThrottleLayer {
    /// Quota takes replenish_byte_rate and max_burst_byte as NonZeroU32 types, so setting them to <= 0 will trigger a panic.
    /// NonZeroU32 can store an integer from 1 to 4,294,967,295 (2^32 - 1).
    pub fn new(replenish_byte_rate: u32, max_burst_byte: u32) -> Self {
        assert!(replenish_byte_rate > 0);
        assert!(max_burst_byte > 0);
        Self {
            replenish_byte_rate,
            max_burst_byte,
        }
    }
}

impl<A: Accessor> Layer<A> for ThrottleLayer {
    type LayeredAccessor = ThrottleAccessor<A>;

    fn layer(&self, accessor: A) -> Self::Output {
        let rate_limiter = Arc::new(RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(self.replenish_byte_rate).unwrap())
                .allow_burst(NonZeroU32::new(self.max_burst_byte).unwrap()),
        ));
        ThrottleAccessor {
            inner: accessor,
            rate_limiter,
        }
    }
}

/// Share an atomic RateLimiter instance across all threads in one operator.
/// If want to add more observability in the future, replace the default NoOpMiddleware with other middleware types.
/// Read more about [Middleware](https://docs.rs/governor/latest/governor/middleware/index.html)
type SharedRateLimiter = Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>;

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
        let mut buf_length = NonZeroU32::new(bs.len() as u32).unwrap();

        match self.limiter.check_n(buf_length) {
            Ok(_) => self.inner.write(bs.split_to(buf_length as usize)).await?,
            Err(negative) => match negative {
                // the query is valid but the Decider can not accommodate them.
                NegativeMultiDecision::BatchNonConforming(_, not_until) => {
                    let wait_time = not_until.wait_time_from(DefaultClock::default().now());
                    // TODO: Should lock the limiter and wait for the wait_time, or should let other small requests go first?
                    tokio::time::sleep(wait_time).await;
                }
                // the query was invalid as the rate limit parameters can "never" accommodate the number of cells queried for.
                NegativeMultiDecision::InsufficientCapacity(_) => Err(Error::new(
                    ErrorKind::RateLimited,
                    "InsufficientCapacity duo to max_burst_byte smaller than the request size",
                )),
            },
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
        let mut buf_length = NonZeroU32::new(bs.len() as u32).unwrap();

        while !bs.is_empty() {
            match self.limiter.check_n(buf_length) {
                Ok(_) => self.inner.write(bs.split_to(buf_length as usize)),
                Err(negative) => match negative {
                    // the query is valid but the Decider can not accommodate them.
                    NegativeMultiDecision::BatchNonConforming(_, not_until) => {
                        let wait_time = not_until.wait_time_from(DefaultClock::default().now());
                        // TODO: Should lock the limiter and wait for the wait_time, or should let other small requests go first?
                        tokio::time::sleep(wait_time)
                    }
                    // the query was invalid as the rate limit parameters can "never" accommodate the number of cells queried for.
                    NegativeMultiDecision::InsufficientCapacity(_) => Err(Error::new(
                        ErrorKind::RateLimited,
                        "InsufficientCapacity duo to max_burst_byte smaller than the request size",
                    )),
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
        let mut buf_length = NonZeroU32::new(bs.len() as u32).unwrap();

        match self.limiter.check_n(buf_length) {
            Ok(_) => self.inner.append(bs.split_to(buf_length as usize)).await?,
            Err(negative) => match negative {
                // the query is valid but the Decider can not accommodate them.
                NegativeMultiDecision::BatchNonConforming(_, not_until) => {
                    let wait_time = not_until.wait_time_from(DefaultClock::default().now());
                    // TODO: Should lock the limiter and wait for the wait_time, or should let other small requests go first?
                    tokio::time::sleep(wait_time).await;
                }
                // the query was invalid as the rate limit parameters can "never" accommodate the number of cells queried for.
                NegativeMultiDecision::InsufficientCapacity(_) => Err(Error::new(
                    ErrorKind::RateLimited,
                    "InsufficientCapacity duo to max_burst_byte smaller than the request size",
                )),
            },
        }
        return Ok(());
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}
