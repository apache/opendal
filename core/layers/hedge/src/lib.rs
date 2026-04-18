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

//! Hedged request layer implementation for Apache OpenDAL.
//!
//! This layer reduces tail latency by issuing hedged requests for slow operations.
//! After a configurable delay, a new request is fired every delay interval. The
//! first completed response is used and all others are dropped.
//!
//! Hedged request layer doesn't provide max attempts count or timeout, so users should
//! use together with timeout layer to avoid infinite hedging.
//!
//! Reference: <https://research.google/pubs/the-tail-at-scale/>

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use opendal_core::raw::*;
use opendal_core::*;

/// Emitted before each hedged request.
#[derive(Debug, Clone, Copy)]
pub struct HedgeEvent {
    /// The operation being hedged.
    pub operation: Operation,
    /// Hedge attempt number (1 = first hedge, 2 = second, etc.).
    pub attempt: usize,
}

/// Called each time a hedged request is fired. Must be quick and non-blocking.
pub trait HedgeInterceptor: Send + Sync + 'static {
    /// Called before each hedged request is sent.
    fn intercept(&self, event: &HedgeEvent);
}

impl<F> HedgeInterceptor for F
where
    F: Fn(&HedgeEvent) + Send + Sync + 'static,
{
    fn intercept(&self, event: &HedgeEvent) {
        self(event);
    }
}

pub struct DefaultHedgeInterceptor;

impl HedgeInterceptor for DefaultHedgeInterceptor {
    fn intercept(&self, event: &HedgeEvent) {
        log::warn!(
            target: "opendal::layers::hedge",
            "hedge #{} fired for {}",
            event.attempt, event.operation
        );
    }
}

/// Per-operation delay configuration.
#[derive(Clone, Copy)]
enum OpDelay {
    Default,
    Custom(Duration),
    Disabled,
}

impl OpDelay {
    fn resolve(self, default: Duration) -> Option<Duration> {
        match self {
            OpDelay::Default => Some(default),
            OpDelay::Custom(d) => Some(d),
            OpDelay::Disabled => None,
        }
    }
}

/// Hedged request layer for read-only operations.
///
/// # Examples
///
/// ```no_run
/// # use std::time::Duration;
/// #
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_hedge::HedgeLayer;
/// # use opendal_layer_timeout::TimeoutLayer;
/// #
/// # fn main() -> Result<()> {
/// let op = Operator::new(services::Memory::default())?
///     .layer(
///         HedgeLayer::new()
///             .with_delay(Duration::from_millis(200))
///             .with_stat_delay(Duration::from_millis(50))
///             .without_read_hedge(),
///     )
///     .layer(TimeoutLayer::default().with_timeout(Duration::from_secs(5)))
///     .finish();
/// # Ok(())
/// # }
/// ```
pub struct HedgeLayer<I: HedgeInterceptor = DefaultHedgeInterceptor> {
    delay: Duration,
    stat_delay: OpDelay,
    read_delay: OpDelay,
    interceptor: Arc<I>,
}

impl<I: HedgeInterceptor> Clone for HedgeLayer<I> {
    fn clone(&self) -> Self {
        Self {
            delay: self.delay,
            stat_delay: self.stat_delay,
            read_delay: self.read_delay,
            interceptor: self.interceptor.clone(),
        }
    }
}

impl Default for HedgeLayer {
    fn default() -> Self {
        Self {
            delay: Duration::from_secs(5),
            stat_delay: OpDelay::Default,
            read_delay: OpDelay::Default,
            interceptor: Arc::new(DefaultHedgeInterceptor),
        }
    }
}

impl HedgeLayer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<I: HedgeInterceptor> HedgeLayer<I> {
    /// Set the default hedge delay. Per-operation delays override this.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Override the hedge delay for `stat`.
    pub fn with_stat_delay(mut self, delay: Duration) -> Self {
        self.stat_delay = OpDelay::Custom(delay);
        self
    }

    /// Disable hedging for `stat`.
    pub fn without_stat_hedge(mut self) -> Self {
        self.stat_delay = OpDelay::Disabled;
        self
    }

    /// Override the hedge delay for `read` (both initiation and per-chunk streaming).
    pub fn with_read_delay(mut self, delay: Duration) -> Self {
        self.read_delay = OpDelay::Custom(delay);
        self
    }

    /// Disable hedging for `read`.
    pub fn without_read_hedge(mut self) -> Self {
        self.read_delay = OpDelay::Disabled;
        self
    }

    /// Set a custom interceptor, called before each hedged request.
    pub fn with_interceptor<NI: HedgeInterceptor>(self, interceptor: NI) -> HedgeLayer<NI> {
        HedgeLayer {
            delay: self.delay,
            stat_delay: self.stat_delay,
            read_delay: self.read_delay,
            interceptor: Arc::new(interceptor),
        }
    }
}

impl<A: Access, I: HedgeInterceptor> Layer<A> for HedgeLayer<I> {
    type LayeredAccess = HedgeAccessor<A, I>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        HedgeAccessor {
            inner: Arc::new(inner),
            stat_delay: self.stat_delay.resolve(self.delay),
            read_delay: self.read_delay.resolve(self.delay),
            interceptor: self.interceptor.clone(),
        }
    }
}

#[doc(hidden)]
pub struct HedgeAccessor<A: Access, I: HedgeInterceptor> {
    inner: Arc<A>,
    stat_delay: Option<Duration>,
    read_delay: Option<Duration>,
    interceptor: Arc<I>,
}

impl<A: Access, I: HedgeInterceptor> Debug for HedgeAccessor<A, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HedgeAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

async fn hedged_op<T, F, Fut>(
    delay: Duration,
    op: Operation,
    interceptor: &impl HedgeInterceptor,
    mut make_fut: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>> + Send,
{
    let mut futs: FuturesUnordered<Pin<Box<dyn Future<Output = Result<T>> + Send>>> =
        FuturesUnordered::new();
    futs.push(Box::pin(make_fut()));
    let mut ticker = tokio::time::interval(delay);
    ticker.tick().await;

    let mut attempt = 0usize;
    loop {
        tokio::select! {
            biased;

            Some(result) = futs.next() => return result,
            _ = ticker.tick() => {
                attempt += 1;
                interceptor.intercept(&HedgeEvent { operation: op, attempt });
                futs.push(Box::pin(make_fut()));
            }
        }
    }
}

impl<A: Access, I: HedgeInterceptor> LayeredAccess for HedgeAccessor<A, I> {
    type Inner = A;
    type Reader = HedgeReader<A, A::Reader, I>;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let Some(delay) = self.stat_delay else {
            return self.inner.stat(path, args).await;
        };
        let inner = self.inner.clone();
        hedged_op(delay, Operation::Stat, &*self.interceptor, || {
            let inner = inner.clone();
            let args = args.clone();
            async move { inner.stat(path, args).await }
        })
        .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let Some(delay) = self.read_delay else {
            return self
                .inner
                .read(path, args.clone())
                .await
                .map(|(rp, reader)| {
                    (
                        rp,
                        HedgeReader::new(
                            self.inner.clone(),
                            path.to_string(),
                            args,
                            reader,
                            None,
                            self.interceptor.clone(),
                        ),
                    )
                });
        };
        let inner = self.inner.clone();
        hedged_op(delay, Operation::Read, &*self.interceptor, || {
            let inner = inner.clone();
            let args = args.clone();
            async move { inner.read(path, args).await }
        })
        .await
        .map(|(rp, reader)| {
            (
                rp,
                HedgeReader::new(
                    self.inner.clone(),
                    path.to_string(),
                    args,
                    reader,
                    Some(delay),
                    self.interceptor.clone(),
                ),
            )
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }
}

#[doc(hidden)]
pub struct HedgeReader<A, R, I: HedgeInterceptor> {
    inner: Arc<A>,
    reader: Option<R>,
    path: String,
    args: OpRead,
    delay: Option<Duration>,
    interceptor: Arc<I>,
}

impl<A, R, I: HedgeInterceptor> HedgeReader<A, R, I> {
    fn new(
        inner: Arc<A>,
        path: String,
        args: OpRead,
        reader: R,
        delay: Option<Duration>,
        interceptor: Arc<I>,
    ) -> Self {
        Self {
            inner,
            reader: Some(reader),
            path,
            args,
            delay,
            interceptor,
        }
    }
}

impl<A: Access, I: HedgeInterceptor> oio::Read for HedgeReader<A, A::Reader, I> {
    async fn read(&mut self) -> Result<Buffer> {
        let Some(delay) = self.delay else {
            let reader = self.reader.as_mut().expect("reader must be present");
            let buf = reader.read().await?;
            self.args.range_mut().advance(buf.len() as u64);
            return Ok(buf);
        };

        let mut current = self.reader.take();
        let inner = self.inner.clone();
        let path = self.path.clone();
        let args = self.args.clone();

        let (reader, buf) = hedged_op(delay, Operation::Read, &*self.interceptor, || {
            let existing = current.take();
            let inner = inner.clone();
            let path = path.clone();
            let args = args.clone();
            async move {
                let mut reader = match existing {
                    Some(r) => r,
                    None => inner.read(&path, args).await?.1,
                };
                let buf = reader.read().await?;
                Ok((reader, buf))
            }
        })
        .await?;

        self.reader = Some(reader);
        self.args.range_mut().advance(buf.len() as u64);
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::time::sleep;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct RecordingInterceptor {
        events: Arc<Mutex<Vec<HedgeEvent>>>,
    }

    impl HedgeInterceptor for RecordingInterceptor {
        fn intercept(&self, event: &HedgeEvent) {
            self.events.lock().unwrap().push(*event);
        }
    }

    impl RecordingInterceptor {
        fn attempt_count(&self) -> usize {
            self.events.lock().unwrap().len()
        }

        fn assert_operations(&self, expected_op: Operation) {
            let events = self.events.lock().unwrap();
            for (i, event) in events.iter().enumerate() {
                assert_eq!(event.operation, expected_op);
                assert_eq!(event.attempt, i + 1);
            }
        }
    }

    fn new_layer(delay: Duration) -> (HedgeLayer<RecordingInterceptor>, RecordingInterceptor) {
        let interceptor = RecordingInterceptor::default();
        let layer = HedgeLayer::new()
            .with_delay(delay)
            .with_interceptor(interceptor.clone());
        (layer, interceptor)
    }

    #[derive(Debug, Clone)]
    struct MockService {
        stat_latency: Duration,
        read_init_latency: Duration,
        chunk_delay: Duration,
    }

    impl Default for MockService {
        fn default() -> Self {
            Self {
                stat_latency: Duration::ZERO,
                read_init_latency: Duration::ZERO,
                chunk_delay: Duration::ZERO,
            }
        }
    }

    impl Access for MockService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;

        fn info(&self) -> Arc<AccessorInfo> {
            let am = AccessorInfo::default();
            am.set_native_capability(Capability {
                read: true,
                stat: true,
                ..Default::default()
            });
            am.into()
        }

        async fn stat(&self, _: &str, _: OpStat) -> Result<RpStat> {
            sleep(self.stat_latency).await;
            Ok(RpStat::new(
                Metadata::new(EntryMode::FILE).with_content_length(13),
            ))
        }

        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
            sleep(self.read_init_latency).await;
            let chunk_delay = self.chunk_delay;
            Ok((
                RpRead::new(),
                Box::new(MockReader {
                    data: Buffer::from(bytes::Bytes::from("Hello, World!")),
                    chunk_delay,
                    done: false,
                }),
            ))
        }
    }

    #[derive(Debug)]
    struct MockReader {
        data: Buffer,
        chunk_delay: Duration,
        done: bool,
    }

    impl oio::Read for MockReader {
        async fn read(&mut self) -> Result<Buffer> {
            if self.done {
                return Ok(Buffer::new());
            }
            sleep(self.chunk_delay).await;
            self.done = true;
            Ok(std::mem::take(&mut self.data))
        }
    }

    #[tokio::test]
    async fn test_hedge_stat_slow_primary() {
        let (layer, interceptor) = new_layer(Duration::from_millis(50));
        let srv = MockService {
            stat_latency: Duration::from_millis(500),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let meta = op.stat("test").await.unwrap();
        assert_eq!(meta.content_length(), 13);

        assert!(interceptor.attempt_count() > 0);
        interceptor.assert_operations(Operation::Stat);
    }

    #[tokio::test]
    async fn test_hedge_stat_fast_primary() {
        let (layer, interceptor) = new_layer(Duration::from_millis(200));
        let srv = MockService {
            stat_latency: Duration::from_millis(1),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let meta = op.stat("test").await.unwrap();
        assert_eq!(meta.content_length(), 13);

        assert_eq!(interceptor.attempt_count(), 0);
    }

    #[tokio::test]
    async fn test_hedge_read_initiation_slow() {
        let (layer, interceptor) = new_layer(Duration::from_millis(50));
        let srv = MockService {
            read_init_latency: Duration::from_millis(500),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let buf = op.read("test").await.unwrap();
        assert_eq!(buf.to_vec(), b"Hello, World!");

        assert!(interceptor.attempt_count() > 0);
        interceptor.assert_operations(Operation::Read);
    }

    #[tokio::test]
    async fn test_hedge_read_streaming_slow() {
        let interceptor = RecordingInterceptor::default();
        let layer = HedgeLayer::new()
            .with_read_delay(Duration::from_millis(50))
            .with_interceptor(interceptor.clone());
        let srv = MockService {
            chunk_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let buf = op.read("test").await.unwrap();
        assert_eq!(buf.to_vec(), b"Hello, World!");

        assert!(interceptor.attempt_count() > 0);
        interceptor.assert_operations(Operation::Read);
    }

    #[tokio::test]
    async fn test_hedge_read_initiation_fast() {
        let (layer, interceptor) = new_layer(Duration::from_millis(200));
        let srv = MockService {
            read_init_latency: Duration::from_millis(1),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let buf = op.read("test").await.unwrap();
        assert_eq!(buf.to_vec(), b"Hello, World!");

        assert_eq!(interceptor.attempt_count(), 0);
    }

    #[tokio::test]
    async fn test_hedge_read_streaming_fast() {
        let interceptor = RecordingInterceptor::default();
        let layer = HedgeLayer::new()
            .with_read_delay(Duration::from_millis(200))
            .with_interceptor(interceptor.clone());
        let srv = MockService {
            chunk_delay: Duration::from_millis(1),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let buf = op.read("test").await.unwrap();
        assert_eq!(buf.to_vec(), b"Hello, World!");

        assert_eq!(interceptor.attempt_count(), 0);
    }

    #[tokio::test]
    async fn test_hedge_multiple_hedges_fire() {
        let (layer, interceptor) = new_layer(Duration::from_millis(50));
        let srv = MockService {
            stat_latency: Duration::from_millis(500),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let meta = op.stat("test").await.unwrap();
        assert_eq!(meta.content_length(), 13);

        assert!(interceptor.attempt_count() > 1);
        interceptor.assert_operations(Operation::Stat);
    }

    #[tokio::test]
    async fn test_hedge_returns_error_immediately() {
        #[derive(Debug, Clone)]
        struct FailService;

        impl Access for FailService {
            type Reader = oio::Reader;
            type Writer = oio::Writer;
            type Lister = oio::Lister;
            type Deleter = oio::Deleter;

            fn info(&self) -> Arc<AccessorInfo> {
                let am = AccessorInfo::default();
                am.set_native_capability(Capability {
                    stat: true,
                    ..Default::default()
                });
                am.into()
            }

            async fn stat(&self, _: &str, _: OpStat) -> Result<RpStat> {
                Err(Error::new(ErrorKind::NotFound, "not found"))
            }
        }

        let (layer, interceptor) = new_layer(Duration::from_millis(50));
        let op = Operator::from_inner(Arc::new(FailService)).layer(layer);

        let err = op.stat("test").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);

        assert_eq!(interceptor.attempt_count(), 0);
    }

    #[tokio::test]
    async fn test_hedge_read_stream_error() {
        #[derive(Debug, Clone)]
        struct StreamFailService;

        impl Access for StreamFailService {
            type Reader = oio::Reader;
            type Writer = oio::Writer;
            type Lister = oio::Lister;
            type Deleter = oio::Deleter;

            fn info(&self) -> Arc<AccessorInfo> {
                let am = AccessorInfo::default();
                am.set_native_capability(Capability {
                    read: true,
                    ..Default::default()
                });
                am.into()
            }

            async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
                Ok((RpRead::new(), Box::new(FailReader)))
            }
        }

        #[derive(Debug)]
        struct FailReader;

        impl oio::Read for FailReader {
            async fn read(&mut self) -> Result<Buffer> {
                Err(Error::new(ErrorKind::Unexpected, "stream read failed"))
            }
        }

        let (layer, interceptor) = new_layer(Duration::from_millis(50));
        let op = Operator::from_inner(Arc::new(StreamFailService)).layer(layer);

        let err = op.read("test").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        assert_eq!(interceptor.attempt_count(), 0);
    }

    #[tokio::test]
    async fn test_hedge_stat_disabled() {
        let interceptor = RecordingInterceptor::default();
        let layer = HedgeLayer::new()
            .with_delay(Duration::from_millis(50))
            .without_stat_hedge()
            .with_interceptor(interceptor.clone());
        let srv = MockService {
            stat_latency: Duration::from_millis(500),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let meta = op.stat("test").await.unwrap();
        assert_eq!(meta.content_length(), 13);

        assert_eq!(interceptor.attempt_count(), 0);
    }

    #[tokio::test]
    async fn test_hedge_read_disabled() {
        let interceptor = RecordingInterceptor::default();
        let layer = HedgeLayer::new()
            .with_delay(Duration::from_millis(50))
            .without_read_hedge()
            .with_interceptor(interceptor.clone());
        let srv = MockService {
            read_init_latency: Duration::from_millis(500),
            chunk_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let op = Operator::from_inner(Arc::new(srv)).layer(layer);

        let buf = op.read("test").await.unwrap();
        assert_eq!(buf.to_vec(), b"Hello, World!");

        assert_eq!(interceptor.attempt_count(), 0);
    }
}
