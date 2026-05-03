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

//! Throttle layer implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;

use governor::Quota;
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use opendal_core::raw::*;
use opendal_core::*;

/// ThrottleRateLimiter abstracts a rate-limit primitive used by
/// [`ThrottleLayer`].
pub trait ThrottleRateLimiter: Send + Sync + Clone + Unpin + 'static {
    /// Block until `n` units of capacity are available.
    ///
    /// Returns an error when the request can never be satisfied, for
    /// example when `n` exceeds the limiter's burst/capacity.
    fn until_n_ready(&self, n: NonZeroU32) -> impl Future<Output = Result<()>> + MaybeSend;
}

/// Share an atomic RateLimiter instance across all threads in one operator.
/// If want to add more observability in the future, replace the default NoOpMiddleware with other middleware types.
/// Read more about [Middleware](https://docs.rs/governor/latest/governor/middleware/index.html)
pub type SharedRateLimiter =
    Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>;

impl ThrottleRateLimiter for SharedRateLimiter {
    async fn until_n_ready(&self, n: NonZeroU32) -> Result<()> {
        self.as_ref().until_n_ready(n).await.map_err(|_| {
            Error::new(
                ErrorKind::RateLimited,
                "burst size is smaller than the request size",
            )
        })
    }
}

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
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_throttle::ThrottleLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(ThrottleLayer::new(10 * 1024, 10000 * 1024))
///     .finish();
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ThrottleLayer<L: ThrottleRateLimiter = SharedRateLimiter> {
    rate_limiter: L,
}

impl ThrottleLayer<SharedRateLimiter> {
    /// Create a new `ThrottleLayer` with given bandwidth and burst.
    ///
    /// - bandwidth: the maximum number of bytes allowed to pass through per second.
    /// - burst: the maximum number of bytes allowed to pass through at once.
    pub fn new(bandwidth: u32, burst: u32) -> Self {
        assert!(bandwidth > 0);
        assert!(burst > 0);
        let bandwidth = NonZeroU32::new(bandwidth).unwrap();
        let burst = NonZeroU32::new(burst).unwrap();
        let rate_limiter = Arc::new(RateLimiter::direct(
            Quota::per_second(bandwidth).allow_burst(burst),
        ));
        Self { rate_limiter }
    }
}

impl<L: ThrottleRateLimiter> ThrottleLayer<L> {
    /// Create a layer with any [`ThrottleRateLimiter`] implementation.
    ///
    /// ```
    /// # use std::num::NonZeroU32;
    /// # use std::sync::Arc;
    /// # use governor::Quota;
    /// # use governor::RateLimiter;
    /// # use opendal_layer_throttle::SharedRateLimiter;
    /// # use opendal_layer_throttle::ThrottleLayer;
    /// let limiter: SharedRateLimiter = Arc::new(RateLimiter::direct(
    ///     Quota::per_second(NonZeroU32::new(1024).unwrap())
    ///         .allow_burst(NonZeroU32::new(1024 * 1024).unwrap()),
    /// ));
    /// let _layer = ThrottleLayer::with_limiter(limiter);
    /// ```
    pub fn with_limiter(rate_limiter: L) -> Self {
        Self { rate_limiter }
    }
}

impl<A: Access, L: ThrottleRateLimiter> Layer<A> for ThrottleLayer<L> {
    type LayeredAccess = ThrottleAccessor<A, L>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        ThrottleAccessor {
            inner,
            rate_limiter: self.rate_limiter.clone(),
        }
    }
}

#[doc(hidden)]
pub struct ThrottleAccessor<A: Access, L: ThrottleRateLimiter> {
    inner: A,
    rate_limiter: L,
}

impl<A: Access, L: ThrottleRateLimiter> std::fmt::Debug for ThrottleAccessor<A, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThrottleAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access, L: ThrottleRateLimiter> LayeredAccess for ThrottleAccessor<A, L> {
    type Inner = A;
    type Reader = ThrottleWrapper<A::Reader, L>;
    type Writer = ThrottleWrapper<A::Writer, L>;
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

#[doc(hidden)]
pub struct ThrottleWrapper<R, L> {
    inner: R,
    limiter: L,
}

impl<R, L> ThrottleWrapper<R, L> {
    fn new(inner: R, limiter: L) -> Self {
        Self { inner, limiter }
    }
}

impl<R: oio::Read, L: ThrottleRateLimiter> oio::Read for ThrottleWrapper<R, L> {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
    }
}

impl<R: oio::Write, L: ThrottleRateLimiter> oio::Write for ThrottleWrapper<R, L> {
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

        self.limiter.until_n_ready(buf_length).await?;

        self.inner.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }
}
