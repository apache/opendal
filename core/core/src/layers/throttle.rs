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

use std::num::NonZeroU32;
use std::sync::Arc;

use governor::Quota;
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::InMemoryState;
use governor::state::NotKeyed;

use crate::raw::*;
use crate::*;

/// Add a bandwidth rate limiter to the underlying services.
///
/// # Throttle
///
/// There are several algorithms when it come to rate limiting techniques.
/// This throttle layer uses Generic Cell Rate Algorithm (GCRA) provided by
/// [Governor](https://docs.rs/governor/latest/governor/index.html).
/// By setting the `bandwidth` and `burst`, we can control the byte flow rate of underlying services.
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
///
/// ```no_run
/// # use opendal_core::layers::ThrottleLayer;
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(ThrottleLayer::new(10 * 1024, 10000 * 1024))
///     .finish();
/// Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ThrottleLayer {
    bandwidth: NonZeroU32,
    burst: NonZeroU32,
}

impl ThrottleLayer {
    /// Create a new `ThrottleLayer` with given bandwidth and burst.
    ///
    /// - bandwidth: the maximum number of bytes allowed to pass through per second.
    /// - burst: the maximum number of bytes allowed to pass through at once.
    pub fn new(bandwidth: u32, burst: u32) -> Self {
        assert!(bandwidth > 0);
        assert!(burst > 0);
        Self {
            bandwidth: NonZeroU32::new(bandwidth).unwrap(),
            burst: NonZeroU32::new(burst).unwrap(),
        }
    }
}

impl<A: Access> Layer<A> for ThrottleLayer {
    type LayeredAccess = ThrottleAccessor<A>;

    fn layer(&self, accessor: A) -> Self::LayeredAccess {
        let rate_limiter = Arc::new(RateLimiter::direct(
            Quota::per_second(self.bandwidth).allow_burst(self.burst),
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
pub struct ThrottleAccessor<A: Access> {
    inner: A,
    rate_limiter: SharedRateLimiter,
}

impl<A: Access> LayeredAccess for ThrottleAccessor<A> {
    type Inner = A;
    type Reader = ThrottleWrapper<A::Reader>;
    type Writer = ThrottleWrapper<A::Writer>;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

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

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
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
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
    }
}

impl<R: oio::Write> oio::Write for ThrottleWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let len = bs.len();
        if len == 0 {
            return self.inner.write(bs).await;
        }

        if len > u32::MAX as usize {
            return Err(Error::new(
                ErrorKind::RateLimited,
                "request size exceeds throttle quota capacity",
            ));
        }

        let buf_length =
            NonZeroU32::new(len as u32).expect("len is non-zero so NonZeroU32 must exist");

        self.limiter.until_n_ready(buf_length).await.map_err(|_| {
            Error::new(
                ErrorKind::RateLimited,
                "burst size is smaller than the request size",
            )
        })?;

        self.inner.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }
}
