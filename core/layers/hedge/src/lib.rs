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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{Either, select};
use futures::stream::{FuturesUnordered, StreamExt};
use opendal_core::raw::*;
use opendal_core::*;

/// Issues duplicate attempts for slow `stat` and read requests.
///
/// By default the layer hedges both operations after five seconds, with at most
/// one additional attempt per request. Use [`Self::with_max_hedges`] to change
/// the limit or set it to zero to disable hedging and its timers. Use
/// [`Self::without_stat_hedge`] or [`Self::without_read_hedge`] to disable hedging
/// for a single operation. Enabled operations use the context executor for
/// timers and fall back to Tokio if the executor does not provide one.
///
/// A success or an error for which [`Error::is_temporary`] is false ends the
/// race immediately. A temporary error stops new attempts and lets any attempts
/// already in flight finish. If they all fail temporarily, the layer returns the
/// last observed error. If the primary fails before a hedge is in flight, the
/// layer returns that error immediately.
///
/// Each read races the range I/O, including opening a stream. The caller
/// receives the winning response and stream together, and consumes that stream
/// without hedging its chunks. The layer drops pending attempts when it returns
/// or when the caller cancels the operation. Dropping a client-side future
/// may leave the service processing an already submitted request.
///
/// Add this layer before the timeout and retry layers. The timeout then covers
/// the whole race, and the outer retry layer can retry a temporary error from it.
///
/// ```no_run
/// use std::time::Duration;
///
/// use opendal_core::{Operator, Result, services};
/// use opendal_layer_hedge::HedgeLayer;
/// use opendal_layer_retry::RetryLayer;
/// use opendal_layer_timeout::TimeoutLayer;
///
/// fn build_operator() -> Result<Operator> {
///     Ok(Operator::new(services::Memory::default())?
///         .layer(HedgeLayer::new().with_delay(Duration::from_millis(500)))
///         .layer(TimeoutLayer::new())
///         .layer(RetryLayer::new()))
/// }
/// ```
#[derive(Clone, Debug)]
pub struct HedgeLayer {
    delay: Duration,
    stat_delay: Option<Duration>,
    read_delay: Option<Duration>,
    max_hedges: usize,
    hedge_stat: bool,
    hedge_read: bool,
}

impl Default for HedgeLayer {
    fn default() -> Self {
        Self {
            delay: Duration::from_secs(5),
            stat_delay: None,
            read_delay: None,
            max_hedges: 1,
            hedge_stat: true,
            hedge_read: true,
        }
    }
}

impl HedgeLayer {
    /// Create a new [`HedgeLayer`] with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the fallback delay between attempts for both operations.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Override the delay between `stat` attempts.
    pub fn with_stat_delay(mut self, delay: Duration) -> Self {
        self.stat_delay = Some(delay);
        self
    }

    /// Override the delay between attempts to open or read a byte range.
    pub fn with_read_delay(mut self, delay: Duration) -> Self {
        self.read_delay = Some(delay);
        self
    }

    /// Set the maximum number of additional attempts per request.
    ///
    /// The default is one additional attempt. Zero disables hedging and its
    /// timers. Each additional attempt waits a full delay after the previous
    /// launch, even if the runtime was stalled.
    pub fn with_max_hedges(mut self, max_hedges: usize) -> Self {
        self.max_hedges = max_hedges;
        self
    }

    /// Disable hedging for `stat`.
    pub fn without_stat_hedge(mut self) -> Self {
        self.hedge_stat = false;
        self
    }

    /// Disable hedging for both stream opens and bounded range reads.
    pub fn without_read_hedge(mut self) -> Self {
        self.hedge_read = false;
        self
    }
}

impl Layer for HedgeLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        if self.max_hedges == 0 || (!self.hedge_stat && !self.hedge_read) {
            return inner;
        }

        Arc::new(HedgeService {
            inner,
            stat_delay: self
                .hedge_stat
                .then(|| self.stat_delay.unwrap_or(self.delay)),
            read_delay: self
                .hedge_read
                .then(|| self.read_delay.unwrap_or(self.delay)),
            max_hedges: self.max_hedges,
        })
    }
}

#[derive(Debug)]
struct HedgeService {
    inner: Servicer,
    stat_delay: Option<Duration>,
    read_delay: Option<Duration>,
    max_hedges: usize,
}

impl Service for HedgeService {
    type Reader = HedgeReader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        match self.stat_delay {
            Some(delay) => {
                hedge(ctx.executor().inner(), delay, self.max_hedges, || {
                    self.inner.stat(ctx, path, args.clone())
                })
                .await
            }
            None => self.inner.stat(ctx, path, args).await,
        }
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(HedgeReader {
            inner: self.inner.read(ctx, path, args)?,
            executor: ctx.executor().clone(),
            delay: self.read_delay,
            max_hedges: self.max_hedges,
        })
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

struct HedgeReader {
    inner: oio::Reader,
    executor: Executor,
    delay: Option<Duration>,
    max_hedges: usize,
}

impl oio::Read for HedgeReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        match self.delay {
            Some(delay) => {
                hedge(self.executor.inner(), delay, self.max_hedges, || {
                    self.inner.open(range)
                })
                .await
            }
            None => self.inner.open(range).await,
        }
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        match self.delay {
            Some(delay) => {
                hedge(self.executor.inner(), delay, self.max_hedges, || {
                    self.inner.read(range)
                })
                .await
            }
            None => self.inner.read(range).await,
        }
    }
}

async fn hedge<T, F: Future<Output = Result<T>>>(
    executor: &dyn Execute,
    delay: Duration,
    mut remaining_hedges: usize,
    attempt: impl Fn() -> F,
) -> Result<T> {
    let mut attempts = FuturesUnordered::new();
    attempts.push(attempt());

    loop {
        let result = if remaining_hedges == 0 {
            attempts.next().await
        } else {
            // Poll attempts before creating or polling the timer so that a
            // ready response takes precedence. Each launch gets a fresh delay.
            let timer = pin!(async {
                executor
                    .sleep(delay)
                    .unwrap_or_else(|| Box::pin(tokio::time::sleep(delay)))
                    .await
            });
            match select(attempts.next(), timer).await {
                Either::Left((result, _)) => result,
                Either::Right(_) => {
                    attempts.push(attempt());
                    remaining_hedges -= 1;
                    continue;
                }
            }
        };

        let result = result.expect("at least one attempt is in flight");
        if matches!(&result, Err(err) if err.is_temporary()) && !attempts.is_empty() {
            remaining_hedges = 0;
        } else {
            return result;
        }
    }
}

#[cfg(test)]
mod tests;
