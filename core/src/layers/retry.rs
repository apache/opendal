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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use backon::BackoffBuilder;
use backon::BlockingRetryable;
use backon::ExponentialBackoff;
use backon::ExponentialBuilder;
use backon::Retryable;
use bytes::Bytes;
use futures::ready;
use futures::FutureExt;
use log::warn;

use crate::ops::*;
use crate::raw::oio::PageOperation;
use crate::raw::oio::ReadOperation;
use crate::raw::oio::WriteOperation;
use crate::raw::*;
use crate::*;

/// Add retry for temporary failed operations.
///
/// # Notes
///
/// This layer will retry failed operations when [`Error::is_temporary`]
/// returns true. If operation still failed, this layer will set error to
/// `Persistent` which means error has been retried.
///
/// `write` and `blocking_write` don't support retry so far, visit [this issue](https://github.com/apache/incubator-opendal/issues/1223) for more details.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::RetryLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(RetryLayer::new())
///     .finish();
/// ```
#[derive(Default, Clone)]
pub struct RetryLayer(ExponentialBuilder);

impl RetryLayer {
    /// Create a new retry layer.
    /// # Examples
    ///
    /// ```
    /// use anyhow::Result;
    /// use opendal::layers::RetryLayer;
    /// use opendal::services;
    /// use opendal::Operator;
    /// use opendal::Scheme;
    ///
    /// let _ = Operator::new(services::Memory::default())
    ///     .expect("must init")
    ///     .layer(RetryLayer::new());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set jitter of current backoff.
    ///
    /// If jitter is enabled, ExponentialBackoff will add a random jitter in `[0, min_delay)
    /// to current delay.
    pub fn with_jitter(mut self) -> Self {
        self.0 = self.0.with_jitter();
        self
    }

    /// Set factor of current backoff.
    ///
    /// # Panics
    ///
    /// This function will panic if input factor smaller than `1.0`.
    pub fn with_factor(mut self, factor: f32) -> Self {
        self.0 = self.0.with_factor(factor);
        self
    }

    /// Set min_delay of current backoff.
    pub fn with_min_delay(mut self, min_delay: Duration) -> Self {
        self.0 = self.0.with_min_delay(min_delay);
        self
    }

    /// Set max_delay of current backoff.
    ///
    /// Delay will not increasing if current delay is larger than max_delay.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.0 = self.0.with_max_delay(max_delay);
        self
    }

    /// Set max_times of current backoff.
    ///
    /// Backoff will return `None` if max times is reaching.
    pub fn with_max_times(mut self, max_times: usize) -> Self {
        self.0 = self.0.with_max_times(max_times);
        self
    }
}

impl<A: Accessor> Layer<A> for RetryLayer {
    type LayeredAccessor = RetryAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        RetryAccessor {
            inner,
            builder: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RetryAccessor<A: Accessor> {
    inner: A,
    builder: ExponentialBuilder,
}

impl<A: Accessor> Debug for RetryAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for RetryAccessor<A> {
    type Inner = A;
    type Reader = RetryWrapper<A::Reader>;
    type BlockingReader = RetryWrapper<A::BlockingReader>;
    type Writer = RetryWrapper<A::Writer>;
    type BlockingWriter = RetryWrapper<A::BlockingWriter>;
    type Pager = RetryWrapper<A::Pager>;
    type BlockingPager = RetryWrapper<A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        { || self.inner.create(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Create, dur.as_secs_f64(), err)
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        { || self.inner.read(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Read, dur.as_secs_f64(), err)
            })
            .map(|v| {
                v.map(|(rp, r)| (rp, RetryWrapper::new(r, path, self.builder.clone())))
                    .map_err(|e| e.set_persistent())
            })
            .await
    }

    /// Return `Interrupted` Error even after retry.
    ///
    /// Allowing users to retry the write request from upper logic.
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        { || self.inner.write(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Write, dur.as_secs_f64(), err)
            })
            .map(|v| {
                v.map(|(rp, r)| (rp, RetryWrapper::new(r, path, self.builder.clone())))
                    .map_err(|e| e.set_persistent())
            })
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        { || self.inner.stat(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Stat, dur.as_secs_f64(), err)
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        { || self.inner.delete(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Delete, dur.as_secs_f64(), err)
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        { || self.inner.list(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::List, dur.as_secs_f64(), err)
            })
            .map(|v| {
                v.map(|(l, p)| {
                    let pager = RetryWrapper::new(p, path, self.builder.clone());
                    (l, pager)
                })
                .map_err(|e| e.set_persistent())
            })
            .await
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        { || self.inner.scan(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Scan, dur.as_secs_f64(), err)
            })
            .map(|v| {
                v.map(|(l, p)| {
                    let pager = RetryWrapper::new(p, path, self.builder.clone());
                    (l, pager)
                })
                .map_err(|e| e.set_persistent())
            })
            .await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        { || self.inner.batch(args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::Batch, dur.as_secs_f64(), err)
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        { || self.inner.blocking_create(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::BlockingCreate, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        { || self.inner.blocking_read(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::BlockingRead, dur.as_secs_f64(), err)
            })
            .call()
            .map(|(rp, r)| (rp, RetryWrapper::new(r, path, self.builder.clone())))
            .map_err(|e| e.set_persistent())
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        { || self.inner.blocking_write(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::BlockingWrite, dur.as_secs_f64(), err)
            })
            .call()
            .map(|(rp, r)| (rp, RetryWrapper::new(r, path, self.builder.clone())))
            .map_err(|e| e.set_persistent())
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        { || self.inner.blocking_stat(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::BlockingStat, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        { || self.inner.blocking_delete(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::BlockingDelete, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        { || self.inner.blocking_list(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::BlockingList, dur.as_secs_f64(), err)
            })
            .call()
            .map(|(rp, p)| {
                let p = RetryWrapper::new(p, path, self.builder.clone());
                (rp, p)
            })
            .map_err(|e| e.set_persistent())
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        { || self.inner.blocking_scan(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                warn!(
                    target: "opendal::service",
                    "operation={} -> retry after {}s: error={:?}",
                    Operation::BlockingScan, dur.as_secs_f64(), err)
            })
            .call()
            .map(|(rp, p)| {
                let p = RetryWrapper::new(p, path, self.builder.clone());
                (rp, p)
            })
            .map_err(|e| e.set_persistent())
    }
}

pub struct RetryWrapper<R> {
    inner: R,
    path: String,
    builder: ExponentialBuilder,
    current_backoff: Option<ExponentialBackoff>,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<R> RetryWrapper<R> {
    fn new(inner: R, path: &str, backoff: ExponentialBuilder) -> Self {
        Self {
            inner,
            path: path.to_string(),
            builder: backoff,
            current_backoff: None,
            sleep: None,
        }
    }
}

impl<R: oio::Read> oio::Read for RetryWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        if let Some(sleep) = self.sleep.as_mut() {
            ready!(sleep.poll_unpin(cx));
            self.sleep = None;
        }

        match ready!(self.inner.poll_read(cx, buf)) {
            Ok(v) => {
                self.current_backoff = None;
                Poll::Ready(Ok(v))
            }
            Err(err) if !err.is_temporary() => {
                self.current_backoff = None;
                Poll::Ready(Err(err))
            }
            Err(err) => {
                let backoff = match self.current_backoff.as_mut() {
                    Some(backoff) => backoff,
                    None => {
                        self.current_backoff = Some(self.builder.build());
                        self.current_backoff.as_mut().unwrap()
                    }
                };

                match backoff.next() {
                    None => {
                        self.current_backoff = None;
                        Poll::Ready(Err(err))
                    }
                    Some(dur) => {
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            ReadOperation::Read, self.path, dur.as_secs_f64(), err);
                        self.sleep = Some(Box::pin(tokio::time::sleep(dur)));
                        self.poll_read(cx, buf)
                    }
                }
            }
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        if let Some(sleep) = self.sleep.as_mut() {
            ready!(sleep.poll_unpin(cx));
            self.sleep = None;
        }

        match ready!(self.inner.poll_seek(cx, pos)) {
            Ok(v) => {
                self.current_backoff = None;
                Poll::Ready(Ok(v))
            }
            Err(err) if !err.is_temporary() => {
                self.current_backoff = None;
                Poll::Ready(Err(err))
            }
            Err(err) => {
                let backoff = match self.current_backoff.as_mut() {
                    Some(backoff) => backoff,
                    None => {
                        self.current_backoff = Some(self.builder.build());
                        self.current_backoff.as_mut().unwrap()
                    }
                };

                match backoff.next() {
                    None => {
                        self.current_backoff = None;
                        Poll::Ready(Err(err))
                    }
                    Some(dur) => {
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                             ReadOperation::Seek, self.path, dur.as_secs_f64(), err);
                        self.sleep = Some(Box::pin(tokio::time::sleep(dur)));
                        self.poll_seek(cx, pos)
                    }
                }
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if let Some(sleep) = self.sleep.as_mut() {
            ready!(sleep.poll_unpin(cx));
            self.sleep = None;
        }

        match ready!(self.inner.poll_next(cx)) {
            None => {
                self.current_backoff = None;
                Poll::Ready(None)
            }
            Some(Ok(v)) => {
                self.current_backoff = None;
                Poll::Ready(Some(Ok(v)))
            }
            Some(Err(err)) if !err.is_temporary() => {
                self.current_backoff = None;
                Poll::Ready(Some(Err(err)))
            }
            Some(Err(err)) => {
                let backoff = match self.current_backoff.as_mut() {
                    Some(backoff) => backoff,
                    None => {
                        self.current_backoff = Some(self.builder.build());
                        self.current_backoff.as_mut().unwrap()
                    }
                };

                match backoff.next() {
                    None => {
                        self.current_backoff = None;
                        Poll::Ready(Some(Err(err)))
                    }
                    Some(dur) => {
                        warn!(
                            target: "opendal::service",
                            "operation={} path={} -> retry after {}s: error={:?}",
                            ReadOperation::Next, self.path, dur.as_secs_f64(), err);
                        self.sleep = Some(Box::pin(tokio::time::sleep(dur)));
                        self.poll_next(cx)
                    }
                }
            }
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for RetryWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        { || self.inner.read(buf) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(move |err, dur| {
                warn!(
                target: "opendal::service",
                "operation={} -> pager retry after {}s: error={:?}",
                ReadOperation::BlockingRead, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        { || self.inner.seek(pos) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(move |err, dur| {
                warn!(
                target: "opendal::service",
                "operation={} -> pager retry after {}s: error={:?}",
                ReadOperation::BlockingSeek, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        { || self.inner.next().transpose() }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(move |err, dur| {
                warn!(
                target: "opendal::service",
                "operation={} -> pager retry after {}s: error={:?}",
                ReadOperation::BlockingNext, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
            .transpose()
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for RetryWrapper<R> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let mut backoff = self.builder.build();

        loop {
            match self.inner.write(bs.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) if !e.is_temporary() => return Err(e),
                Err(e) => match backoff.next() {
                    None => return Err(e),
                    Some(dur) => {
                        warn!(target: "opendal::service",
                              "operation={} path={} -> pager retry after {}s: error={:?}",
                              WriteOperation::Write, self.path, dur.as_secs_f64(), e);
                        tokio::time::sleep(dur).await;
                        continue;
                    }
                },
            }
        }
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        let mut backoff = self.builder.build();

        loop {
            match self.inner.append(bs.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) if !e.is_temporary() => return Err(e),
                Err(e) => match backoff.next() {
                    None => return Err(e),
                    Some(dur) => {
                        warn!(target: "opendal::service",
                              "operation={} path={} -> pager retry after {}s: error={:?}",
                              WriteOperation::Append, self.path, dur.as_secs_f64(), e);
                        tokio::time::sleep(dur).await;
                        continue;
                    }
                },
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        let mut backoff = self.builder.build();

        loop {
            match self.inner.close().await {
                Ok(v) => return Ok(v),
                Err(e) if !e.is_temporary() => return Err(e),
                Err(e) => match backoff.next() {
                    None => return Err(e),
                    Some(dur) => {
                        warn!(target: "opendal::service",
                              "operation={} path={} -> pager retry after {}s: error={:?}",
                              WriteOperation::Close, self.path, dur.as_secs_f64(), e);
                        tokio::time::sleep(dur).await;
                        continue;
                    }
                },
            }
        }
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for RetryWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        { || self.inner.write(bs.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(move |err, dur| {
                warn!(
                target: "opendal::service",
                "operation={} -> pager retry after {}s: error={:?}",
                WriteOperation::BlockingWrite, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn append(&mut self, bs: Bytes) -> Result<()> {
        { || self.inner.append(bs.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(move |err, dur| {
                warn!(
                target: "opendal::service",
                "operation={} -> pager retry after {}s: error={:?}",
                WriteOperation::BlockingAppend, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn close(&mut self) -> Result<()> {
        { || self.inner.close() }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(move |err, dur| {
                warn!(
                target: "opendal::service",
                "operation={} -> pager retry after {}s: error={:?}",
               WriteOperation::BlockingClose, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }
}

#[async_trait]
impl<P: oio::Page> oio::Page for RetryWrapper<P> {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let mut backoff = self.builder.build();

        loop {
            match self.inner.next().await {
                Ok(v) => return Ok(v),
                Err(e) if !e.is_temporary() => return Err(e),
                Err(e) => match backoff.next() {
                    None => return Err(e),
                    Some(dur) => {
                        warn!(target: "opendal::service",
                              "operation={} path={} -> pager retry after {}s: error={:?}",
                              PageOperation::Next, self.path, dur.as_secs_f64(), e);
                        tokio::time::sleep(dur).await;
                        continue;
                    }
                },
            }
        }
    }
}

impl<P: oio::BlockingPage> oio::BlockingPage for RetryWrapper<P> {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        { || self.inner.next() }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(move |err, dur| {
                warn!(
                target: "opendal::service",
                "operation={} -> pager retry after {}s: error={:?}",
                PageOperation::BlockingNext, dur.as_secs_f64(), err)
            })
            .call()
            .map_err(|e| e.set_persistent())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::AsyncReadExt;
    use futures::TryStreamExt;

    use super::*;

    #[derive(Default, Clone)]
    struct MockBuilder {
        attempt: Arc<Mutex<usize>>,
    }

    impl Builder for MockBuilder {
        const SCHEME: Scheme = Scheme::Custom("mock");
        type Accessor = MockService;

        fn from_map(_: HashMap<String, String>) -> Self {
            Self::default()
        }

        fn build(&mut self) -> Result<Self::Accessor> {
            Ok(MockService {
                attempt: self.attempt.clone(),
            })
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockService {
        attempt: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl Accessor for MockService {
        type Reader = MockReader;
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Pager = MockPager;
        type BlockingPager = ();

        fn info(&self) -> AccessorInfo {
            let mut am = AccessorInfo::default();
            am.set_capabilities(AccessorCapability::List);
            am.set_hints(AccessorHint::ReadStreamable);

            am
        }

        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
            Ok((
                RpRead::new(13),
                MockReader {
                    attempt: self.attempt.clone(),
                    pos: 0,
                },
            ))
        }

        async fn list(&self, _: &str, _: OpList) -> Result<(RpList, Self::Pager)> {
            let pager = MockPager::default();
            Ok((RpList::default(), pager))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReader {
        attempt: Arc<Mutex<usize>>,
        pos: u64,
    }

    impl oio::Read for MockReader {
        fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            Poll::Ready(match *attempt {
                1 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                2 => {
                    buf[..7].copy_from_slice("Hello, ".as_bytes());
                    self.pos += 7;
                    Ok(7)
                }
                3 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                4 => {
                    buf[..6].copy_from_slice("World!".as_bytes());
                    self.pos += 6;
                    Ok(6)
                }
                5 => Ok(0),
                _ => unreachable!(),
            })
        }

        fn poll_seek(&mut self, _: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
            self.pos = match pos {
                io::SeekFrom::Current(n) => (self.pos as i64 + n) as u64,
                io::SeekFrom::Start(n) => n,
                io::SeekFrom::End(n) => (13 + n) as u64,
            };

            Poll::Ready(Ok(self.pos))
        }

        fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
            let mut bs = vec![0; 1];
            match ready!(self.poll_read(cx, &mut bs)) {
                Ok(v) if v == 0 => Poll::Ready(None),
                Ok(v) => Poll::Ready(Some(Ok(Bytes::from(bs[..v].to_vec())))),
                Err(err) => Poll::Ready(Some(Err(err))),
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockPager {
        attempt: usize,
    }
    #[async_trait]
    impl oio::Page for MockPager {
        async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
            self.attempt += 1;
            match self.attempt {
                1 => Err(Error::new(
                    ErrorKind::RateLimited,
                    "retryable rate limited error from pager",
                )
                .set_temporary()),
                2 => {
                    let entries = vec![
                        oio::Entry::new("hello", Metadata::new(EntryMode::FILE)),
                        oio::Entry::new("world", Metadata::new(EntryMode::FILE)),
                    ];
                    Ok(Some(entries))
                }
                3 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable internal server error")
                        .set_temporary(),
                ),
                4 => {
                    let entries = vec![
                        oio::Entry::new("2023/", Metadata::new(EntryMode::DIR)),
                        oio::Entry::new("0208/", Metadata::new(EntryMode::DIR)),
                    ];
                    Ok(Some(entries))
                }
                5 => Ok(None),
                _ => {
                    unreachable!()
                }
            }
        }
    }

    #[tokio::test]
    async fn test_retry_read() {
        let _ = env_logger::try_init();

        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())
            .unwrap()
            .layer(RetryLayer::new())
            .finish();

        let mut r = op.reader("retryable_error").await.unwrap();
        let mut content = Vec::new();
        let size = r
            .read_to_end(&mut content)
            .await
            .expect("read must succeed");
        assert_eq!(size, 13);
        assert_eq!(content, "Hello, World!".as_bytes());
        // The error is retryable, we should request it 1 + 10 times.
        assert_eq!(*builder.attempt.lock().unwrap(), 5);
    }

    #[tokio::test]
    async fn test_retry_list() {
        let _ = env_logger::try_init();

        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())
            .unwrap()
            .layer(RetryLayer::new())
            .finish();

        let expected = vec!["hello", "world", "2023/", "0208/"];

        let mut lister = op
            .list("retryable_error/")
            .await
            .expect("service must support list");
        let mut actual = Vec::new();
        while let Some(obj) = lister.try_next().await.expect("must success") {
            actual.push(obj.name().to_owned());
        }

        assert_eq!(actual, expected);
    }
}
