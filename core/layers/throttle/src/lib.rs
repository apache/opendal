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

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let rate_limiter = Arc::new(RateLimiter::direct(
            Quota::per_second(self.bandwidth).allow_burst(self.burst),
        ));
        ThrottleAccessor {
            inner,
            rate_limiter,
        }
    }
}

/// Share an atomic RateLimiter instance across all threads in one operator.
/// If want to add more observability in the future, replace the default NoOpMiddleware with other middleware types.
/// Read more about [Middleware](https://docs.rs/governor/latest/governor/middleware/index.html)
type SharedRateLimiter = Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>;

#[doc(hidden)]
#[derive(Debug)]
pub struct ThrottleAccessor<A: Access> {
    inner: A,
    rate_limiter: SharedRateLimiter,
}

impl<A: Access> LayeredAccess for ThrottleAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = ThrottleWrapper<A::Writer>;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        if let Some(size) = args.range().size() {
            wait_for_quota(&self.rate_limiter, size).await?;
        }

        self.inner.read(path, args).await
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
pub struct ThrottleWrapper<R> {
    inner: R,
    limiter: SharedRateLimiter,
}

impl<R> ThrottleWrapper<R> {
    fn new(inner: R, rate_limiter: SharedRateLimiter) -> Self {
        Self {
            inner,
            limiter: rate_limiter,
        }
    }
}

async fn wait_for_quota(limiter: &SharedRateLimiter, len: u64) -> Result<()> {
    if len == 0 {
        return Ok(());
    }

    if len > u32::MAX as u64 {
        return Err(Error::new(
            ErrorKind::RateLimited,
            "request size exceeds throttle quota capacity",
        ));
    }

    let buf_length = NonZeroU32::new(len as u32).expect("len is non-zero so NonZeroU32 must exist");

    limiter.until_n_ready(buf_length).await.map_err(|_| {
        Error::new(
            ErrorKind::RateLimited,
            "burst size is smaller than the request size",
        )
    })
}

impl<R: oio::Write> oio::Write for ThrottleWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        wait_for_quota(&self.limiter, bs.len() as u64).await?;
        self.inner.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct SingleRead {
        data: Option<Buffer>,
    }

    impl oio::Read for SingleRead {
        async fn read(&mut self) -> Result<Buffer> {
            Ok(self.data.take().unwrap_or_default())
        }
    }

    #[derive(Debug)]
    struct MockService {
        read_count: Arc<AtomicUsize>,
    }

    impl Access for MockService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;

        fn info(&self) -> Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(Capability {
                read: true,
                ..Default::default()
            });

            info.into()
        }

        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
            self.read_count.fetch_add(1, Ordering::SeqCst);
            Ok((RpRead::new(), Box::new(SingleRead { data: None })))
        }
    }

    fn new_throttle_accessor(
        bandwidth: u32,
        burst: u32,
    ) -> (ThrottleAccessor<MockService>, Arc<AtomicUsize>) {
        let read_count = Arc::new(AtomicUsize::new(0));
        let accessor = ThrottleLayer::new(bandwidth, burst).layer(MockService {
            read_count: read_count.clone(),
        });

        (accessor, read_count)
    }

    #[tokio::test]
    async fn bounded_read_allows_request_within_burst() {
        let (accessor, read_count) = new_throttle_accessor(2, 2);

        LayeredAccess::read(
            &accessor,
            "path",
            OpRead::new().with_range(BytesRange::new(0, Some(2))),
        )
        .await
        .expect("bounded read within burst should pass throttle");

        assert_eq!(read_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bounded_read_rejects_before_sending_request() {
        let (accessor, read_count) = new_throttle_accessor(1, 1);

        let err = match LayeredAccess::read(
            &accessor,
            "path",
            OpRead::new().with_range(BytesRange::new(0, Some(2))),
        )
        .await
        {
            Ok(_) => panic!("bounded read larger than burst should fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert_eq!(read_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unbounded_read_is_forwarded_without_precheck() {
        let (accessor, read_count) = new_throttle_accessor(1, 1);

        LayeredAccess::read(&accessor, "path", OpRead::new())
            .await
            .expect("unbounded read size is unknown before request");

        assert_eq!(read_count.load(Ordering::SeqCst), 1);
    }
}
