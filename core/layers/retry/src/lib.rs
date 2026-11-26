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
use std::fmt::Debug;
use std::future::Future;
use std::panic::RefUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use backon::BackoffBuilder;
use backon::BlockingSleeper as _;
use backon::DefaultBlockingSleeper;
use backon::DefaultSleeper;
use backon::ExponentialBackoff;
use backon::ExponentialBuilder;
use backon::Sleeper as _;
use opendal_core::raw::*;
use opendal_core::*;

/// Register the `retry` layer with an operator registry.
///
/// `jitter` accepts `true`, `false`, `on`, and `off`, ignoring ASCII case.
/// `factor` must be finite and at least `1`. `min_delay` and `max_delay` must
/// be positive durations such as `500ms` or `PT1S`. `max_times` is the maximum
/// number of retries after the first attempt and accepts `0`.
/// Omitted options keep the defaults from [`RetryLayer::new`].
pub fn register_retry_layer(registry: &OperatorRegistry) {
    registry.register_layer("retry", |options| {
        let mut layer = RetryLayer::new();

        if let Some(value) = options.get("jitter") {
            match value.to_ascii_lowercase().as_str() {
                "true" | "on" => layer = layer.with_jitter(),
                "false" | "off" => {}
                _ => {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        "jitter must be true, false, on, or off",
                    ));
                }
            }
        }
        if let Some(value) = options.get("factor") {
            let factor = match value.parse::<f32>() {
                Ok(factor) if factor.is_finite() && factor >= 1.0 => factor,
                _ => {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        "factor must be finite and at least 1",
                    ));
                }
            };
            layer = layer.with_factor(factor);
        }
        if let Some(value) = options.get("min_delay") {
            layer = layer.with_min_delay(parse_delay("min_delay", value)?);
        }
        if let Some(value) = options.get("max_delay") {
            layer = layer.with_max_delay(parse_delay("max_delay", value)?);
        }
        if let Some(value) = options.get("max_times") {
            let max_times = value.parse::<usize>().map_err(|_| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "max_times must be a nonnegative integer",
                )
            })?;
            layer = layer.with_max_times(max_times);
        }
        Ok(layer)
    });
}

fn parse_delay(option: &'static str, value: &str) -> Result<Duration> {
    match signed_to_duration(value) {
        Ok(duration) if !duration.is_zero() => Ok(duration),
        _ => Err(
            Error::new(ErrorKind::ConfigInvalid, "expected a positive duration")
                .with_context("option", option),
        ),
    }
}

/// `RetryLayer` retries temporarily failed operations.
///
/// # Notes
///
/// This layer retries an operation when [`Error::is_temporary`] returns `true`.
/// If the operation still fails, the layer marks the error as `Persistent` to
/// indicate that retries did not resolve it.
///
/// When reopening a read stream after a temporary error, `RetryLayer` checks
/// that the reopened object identity matches the previously observed metadata.
/// If the object's identity changes or can no longer be verified (for example,
/// a strong validator disappears, or a mid-stream resume only has a weak ETag
/// without a matching `version`), `RetryLayer` returns a non-temporary
/// [`ErrorKind::Conflict`] instead of splicing bytes across different
/// versions.
///
/// # Composing with `TimeoutLayer` and cancellation
///
/// Layer order controls which deadline `TimeoutLayer` enforces when composed with `RetryLayer`:
///
/// - Adding `TimeoutLayer` before `RetryLayer` applies the timeout to each individual attempt so
///   `RetryLayer` can retry timed-out attempts.
/// - Adding `TimeoutLayer` after `RetryLayer` bounds total time across all retries of a single
///   call.
///
/// Dropping an in-flight retry future keeps the underlying handle usable instead of poisoning
/// `RetryLayer`, and callers still follow each handle's recovery contract:
///
/// - `Writer`: dropping an in-flight `Writer::write` or `Writer::close` future keeps the handle
///   usable for cleanup, but a streaming or append body may have already persisted part of that
///   call. Call `Writer::abort` and start a new writer instead of continuing to write or close,
///   which can duplicate or corrupt data.
/// - `Deleter`, `Copier`, and `Lister`: these handles can continue their normal recovery (for
///   example, retrying `Deleter::close` to flush buffered deletes, calling `Copier::abort`, or
///   continuing to poll a `Lister`).
/// - Readers with [`RetryBudget`]: reopen a read stream after cancelled or exhausted recovery.
///
/// ```no_run
/// # use std::time::Duration;
/// #
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_retry::RetryLayer;
/// # use opendal_layer_timeout::TimeoutLayer;
/// #
/// # fn main() -> Result<()> {
/// let op = Operator::new(services::Memory::default())?
///     // Time out each retry attempt so RetryLayer can retry it.
///     .layer(TimeoutLayer::default().with_io_timeout(Duration::from_secs(10)))
///     .layer(RetryLayer::default())
///     // Optional outer deadline across all retries of one call.
///     .layer(TimeoutLayer::default().with_io_timeout(Duration::from_secs(30)));
/// # Ok(())
/// # }
/// ```
///
/// # Examples
///
/// ```no_run
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_retry::RetryLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(RetryLayer::default());
/// # Ok(())
/// # }
/// ```
///
/// ## Customize retry interceptor
///
/// RetryLayer accepts [`RetryInterceptor`] to allow users to customize
/// their own retry interceptor logic.
///
/// ```no_run
/// # use std::time::Duration;
/// #
/// # use opendal_core::services;
/// # use opendal_core::Error;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_retry::RetryEvent;
/// # use opendal_layer_retry::RetryInterceptor;
/// # use opendal_layer_retry::RetryLayer;
/// #
/// struct MyRetryInterceptor;
///
/// impl RetryInterceptor for MyRetryInterceptor {
///     fn intercept(&self, event: RetryEvent<'_>) {
///         // do something with event.op, event.err, event.retry_after, event.attempt
///     }
/// }
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(RetryLayer::default().with_notify(MyRetryInterceptor));
/// # Ok(())
/// # }
/// ```
pub struct RetryLayer<I: RetryInterceptor = DefaultRetryInterceptor> {
    policy: RetryPolicy<I>,
}

impl<I: RetryInterceptor> Debug for RetryLayer<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryLayer")
            .field("builder", &self.policy.builder)
            .finish_non_exhaustive()
    }
}

impl<I: RetryInterceptor> Clone for RetryLayer<I> {
    fn clone(&self) -> Self {
        Self {
            policy: self.policy.clone(),
        }
    }
}

impl Default for RetryLayer {
    fn default() -> Self {
        Self {
            policy: RetryPolicy {
                builder: ExponentialBuilder::default(),
                notify: Arc::new(DefaultRetryInterceptor),
                budget: None,
            },
        }
    }
}

impl RetryLayer {
    /// Create a new [`RetryLayer`].
    pub fn new() -> RetryLayer {
        Self::default()
    }
}

impl<I: RetryInterceptor> RetryLayer<I> {
    /// Set the retry interceptor as new notify.
    ///
    /// ```no_run
    /// use opendal_core::services;
    /// use opendal_core::Operator;
    /// use opendal_layer_retry::RetryLayer;
    ///
    /// fn notify(_event: opendal_layer_retry::RetryEvent<'_>) {}
    ///
    /// let _ = Operator::new(services::Memory::default())
    ///     .expect("must init")
    ///     .layer(RetryLayer::default().with_notify(notify));
    /// ```
    pub fn with_notify<NI: RetryInterceptor>(self, notify: NI) -> RetryLayer<NI> {
        RetryLayer {
            policy: RetryPolicy {
                builder: self.policy.builder,
                notify: Arc::new(notify),
                budget: self.policy.budget,
            },
        }
    }

    /// Set an optional budget that can limit retries across operations.
    ///
    /// By default, retries are limited by the backoff settings alone. The first
    /// attempt is always free. The layer consults the budget only after a
    /// temporary failure when the backoff allows another attempt. If the budget
    /// denies the retry, the layer returns the failure as a persistent error.
    /// A stream whose recovery fails or is cancelled must be reopened by the
    /// caller before reading further.
    ///
    /// Clones of this layer and the operators and handles built from it share
    /// the same budget.
    pub fn with_budget<B: RetryBudget>(mut self, budget: B) -> Self {
        self.policy.budget = Some(Arc::new(budget));
        self
    }

    /// Set jitter of current backoff.
    ///
    /// If jitter is enabled, ExponentialBackoff will add a random jitter in `[0, min_delay)
    /// to current delay.
    pub fn with_jitter(mut self) -> Self {
        self.policy.builder = self.policy.builder.with_jitter();
        self
    }

    /// Set factor of current backoff.
    ///
    /// # Panics
    ///
    /// This function will panic if input factor smaller than `1.0`.
    pub fn with_factor(mut self, factor: f32) -> Self {
        self.policy.builder = self.policy.builder.with_factor(factor);
        self
    }

    /// Set min_delay of current backoff.
    pub fn with_min_delay(mut self, min_delay: Duration) -> Self {
        self.policy.builder = self.policy.builder.with_min_delay(min_delay);
        self
    }

    /// Set max_delay of current backoff.
    ///
    /// Delay will not increase if current delay is larger than max_delay.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.policy.builder = self.policy.builder.with_max_delay(max_delay);
        self
    }

    /// Set max_times of current backoff.
    ///
    /// Backoff will return `None` if max times is reaching.
    pub fn with_max_times(mut self, max_times: usize) -> Self {
        self.policy.builder = self.policy.builder.with_max_times(max_times);
        self
    }
}

impl<I: RetryInterceptor> Layer for RetryLayer<I> {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(RetryService {
            inner,
            policy: self.policy.clone(),
        })
    }
}

/// Controls which retries [`RetryLayer`] may issue after temporary failures.
///
/// A budget can limit the retries shared by multiple operators to avoid
/// amplifying load on a degraded backend. Methods can be called concurrently
/// and must be quick and non-blocking. The operation identifies which balance
/// to update when the implementation tracks separate budgets per operation.
pub trait RetryBudget: Send + Sync + RefUnwindSafe + Debug + 'static {
    /// Record a successful operation.
    ///
    /// `create_dir`, `stat`, `rename`, `restore`, and `presign` deposit once on
    /// success. Each reader deposits once when its first range completes, even
    /// when it reads multiple ranges concurrently or resumes an interrupted
    /// stream. A streaming range completes at EOF or when it yields the final
    /// byte of a bounded range. Writers, copiers, and deleters deposit once on
    /// successful close. Listers deposit once when they first reach the end.
    fn deposit(&self, op: Operation);

    /// Atomically reserve one retry. Return `false` without changing the
    /// balance if the retry cannot be reserved.
    fn try_reserve(&self, op: Operation) -> bool;

    /// Return one reservation for a retry that was cancelled before it started.
    ///
    /// The layer calls this after a successful reservation if it drops the
    /// retry before invoking the inner operation, for example during the backoff
    /// sleep. A retry that starts consumes its reservation even if it fails.
    fn release(&self, op: Operation);
}

#[must_use]
struct RetryBudgetPermit<'a> {
    budget: Option<&'a (dyn RetryBudget + RefUnwindSafe)>,
    op: Operation,
}

impl RetryBudgetPermit<'_> {
    fn commit(mut self) {
        self.budget = None;
    }
}

impl Drop for RetryBudgetPermit<'_> {
    fn drop(&mut self) {
        if let Some(budget) = self.budget {
            budget.release(self.op);
        }
    }
}

struct RetryPolicy<I> {
    builder: ExponentialBuilder,
    notify: Arc<I>,
    budget: Option<Arc<dyn RetryBudget + RefUnwindSafe>>,
}

impl<I> Clone for RetryPolicy<I> {
    fn clone(&self) -> Self {
        Self {
            builder: self.builder,
            notify: self.notify.clone(),
            budget: self.budget.clone(),
        }
    }
}

struct RetryLoop<'a, I> {
    policy: &'a RetryPolicy<I>,
    op: Operation,
    backoff: ExponentialBackoff,
    attempt: u32,
}

impl<'a, I: RetryInterceptor> RetryLoop<'a, I> {
    fn new(policy: &'a RetryPolicy<I>, op: Operation) -> Self {
        Self {
            policy,
            op,
            backoff: policy.builder.build(),
            attempt: 0,
        }
    }

    async fn sleep_or_fail(&mut self, err: Error) -> Result<()> {
        let Some((delay, permit)) =
            self.policy
                .schedule_retry(self.op, &err, &mut self.backoff, &mut self.attempt)
        else {
            return Err(err.set_persistent());
        };
        drop(err);
        DefaultSleeper::default().sleep(delay).await;
        permit.commit();
        Ok(())
    }
}

impl<I: RetryInterceptor> RetryPolicy<I> {
    fn retry_loop(&self, op: Operation) -> RetryLoop<'_, I> {
        RetryLoop::new(self, op)
    }

    fn schedule_retry(
        &self,
        op: Operation,
        err: &Error,
        backoff: &mut ExponentialBackoff,
        attempt: &mut u32,
    ) -> Option<(Duration, RetryBudgetPermit<'_>)> {
        if !err.is_temporary() {
            return None;
        }
        let retry_after = backoff.next()?;
        let budget = self.budget.as_deref();
        if budget.is_some_and(|budget| !budget.try_reserve(op)) {
            return None;
        }
        let permit = RetryBudgetPermit { budget, op };
        *attempt += 1;
        self.notify.intercept(RetryEvent {
            op,
            err,
            retry_after,
            attempt: *attempt,
        });
        Some((retry_after, permit))
    }

    async fn retry<T, F>(&self, op: Operation, mut call: impl FnMut() -> F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let mut retry = self.retry_loop(op);
        loop {
            match call().await {
                Ok(value) => return Ok(value),
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }

    fn retry_blocking<T>(&self, op: Operation, mut call: impl FnMut() -> Result<T>) -> Result<T> {
        let mut backoff = self.builder.build();
        let mut attempt = 0;
        loop {
            let err = match call() {
                Ok(value) => return Ok(value),
                Err(err) => err,
            };
            let Some((delay, permit)) = self.schedule_retry(op, &err, &mut backoff, &mut attempt)
            else {
                return Err(err.set_persistent());
            };
            DefaultBlockingSleeper::default().sleep(delay);
            permit.commit();
        }
    }

    fn deposit(&self, op: Operation) {
        if let Some(budget) = &self.budget {
            budget.deposit(op);
        }
    }
}

/// Context passed to [`RetryInterceptor`] before each retry sleep.
#[non_exhaustive]
#[derive(Debug)]
pub struct RetryEvent<'a> {
    /// The operation being retried.
    pub op: Operation,
    /// The error that triggered the retry.
    pub err: &'a Error,
    /// The duration to wait before the next retry attempt.
    pub retry_after: Duration,
    /// 1-based retry attempt number.
    pub attempt: u32,
}

/// RetryInterceptor observes retry attempts before the retry sleep.
pub trait RetryInterceptor: Send + Sync + 'static {
    /// Called before each retry sleep.
    ///
    /// # Notes
    ///
    /// The intercept must be quick and non-blocking. No heavy IO is
    /// allowed. Otherwise, the retry will be blocked.
    fn intercept(&self, event: RetryEvent<'_>);
}

impl<F> RetryInterceptor for F
where
    F: for<'a> Fn(RetryEvent<'a>) + Send + Sync + 'static,
{
    fn intercept(&self, event: RetryEvent<'_>) {
        self(event);
    }
}

/// The DefaultRetryInterceptor logs each retry error at warn level.
pub struct DefaultRetryInterceptor;

impl RetryInterceptor for DefaultRetryInterceptor {
    fn intercept(&self, event: RetryEvent<'_>) {
        log::warn!(
            target: "opendal::layers::retry",
            "will retry {:?} (attempt {}) after {}s because: {:?}",
            event.op, event.attempt, event.retry_after.as_secs_f64(), event.err
        );
    }
}

struct RetryService<I: RetryInterceptor> {
    inner: Servicer,
    policy: RetryPolicy<I>,
}

impl<I: RetryInterceptor> Debug for RetryService<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<I: RetryInterceptor> Service for RetryService<I> {
    type Reader = RetryReader<oio::Reader, I>;
    type Writer = RetryWrapper<oio::Writer, I>;
    type Lister = RetryWrapper<oio::Lister, I>;
    type Deleter = RetryWrapper<oio::Deleter, I>;
    type Copier = RetryWrapper<oio::Copier, I>;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let result = self
            .policy
            .retry(Operation::CreateDir, || {
                self.inner.create_dir(ctx, path, args.clone())
            })
            .await?;
        self.policy.deposit(Operation::CreateDir);
        Ok(result)
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let reader = self
            .policy
            .retry_blocking(Operation::Read, || self.inner.read(ctx, path, args.clone()))?;

        Ok(RetryReader::new(reader, self.policy.clone()))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let writer = self.policy.retry_blocking(Operation::Write, || {
            self.inner.write(ctx, path, args.clone())
        })?;

        Ok(RetryWrapper::new(writer, self.policy.clone()))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let result = self
            .policy
            .retry(Operation::Stat, || self.inner.stat(ctx, path, args.clone()))
            .await?;
        self.policy.deposit(Operation::Stat);
        Ok(result)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let deleter = self
            .policy
            .retry_blocking(Operation::Delete, || self.inner.delete(ctx))?;

        Ok(RetryWrapper::new(deleter, self.policy.clone()))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let copier = self.policy.retry_blocking(Operation::Copy, || {
            self.inner.copy(ctx, from, to, args.clone())
        })?;

        Ok(RetryWrapper::new(copier, self.policy.clone()))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let result = self
            .policy
            .retry(Operation::Rename, || {
                self.inner.rename(ctx, from, to, args.clone())
            })
            .await?;
        self.policy.deposit(Operation::Rename);
        Ok(result)
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        let result = self
            .policy
            .retry(Operation::Restore, || {
                self.inner.restore(ctx, path, args.clone())
            })
            .await?;
        self.policy.deposit(Operation::Restore);
        Ok(result)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let lister = self
            .policy
            .retry_blocking(Operation::List, || self.inner.list(ctx, path, args.clone()))?;

        Ok(RetryWrapper::new(lister, self.policy.clone()))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let result = self
            .policy
            .retry(Operation::Presign, || {
                self.inner.presign(ctx, path, args.clone())
            })
            .await?;
        self.policy.deposit(Operation::Presign);
        Ok(result)
    }
}

struct RetryReader<R, I> {
    inner: Arc<R>,
    policy: RetryPolicy<I>,
    deposited: Option<Arc<AtomicBool>>,
}

impl<R, I> RetryReader<R, I> {
    fn new(inner: R, policy: RetryPolicy<I>) -> Self {
        let deposited = policy
            .budget
            .as_ref()
            .map(|_| Arc::new(AtomicBool::new(false)));
        Self {
            inner: Arc::new(inner),
            policy,
            deposited,
        }
    }
}

impl<R: oio::Read + 'static, I: RetryInterceptor> oio::Read for RetryReader<R, I> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (rp, stream) = self
            .policy
            .retry(Operation::Read, || self.inner.open(range))
            .await?;
        let identity = ReadStreamIdentity::from_rp(&rp);
        let range = resolve_range(range, &rp);
        Ok((
            rp,
            Box::new(RetryReadStream {
                reader: self.inner.clone(),
                stream: Some(stream),
                range,
                read: 0,
                has_progress: false,
                identity,
                terminal_conflict: None,
                policy: self.policy.clone(),
                deposited: self.deposited.clone(),
            }),
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let result = self
            .policy
            .retry(Operation::Read, || self.inner.read(range))
            .await?;
        if self
            .deposited
            .as_ref()
            .is_some_and(|flag| !flag.swap(true, Ordering::Relaxed))
        {
            self.policy.deposit(Operation::Read);
        }
        Ok(result)
    }
}

/// Resolve a range to the bytes that exist in the object when the response
/// reports the total object size.
///
/// A suffix range is relative to the end of the object, and a forward range may
/// extend past it. An unbounded forward range becomes bounded by the object
/// size. Clamping makes advancing the range after progress safe: it can never
/// reopen an empty range at or beyond the end of the object.
fn resolve_range(range: BytesRange, rp: &RpRead) -> BytesRange {
    let Some(metadata) = rp.metadata().filter(|metadata| metadata.is_file()) else {
        return range;
    };
    let content_length = metadata.content_length();
    match range {
        BytesRange::Range { offset, size } => {
            let remaining = content_length.saturating_sub(offset);
            BytesRange::new(
                offset,
                Some(size.map_or(remaining, |size| size.min(remaining))),
            )
        }
        BytesRange::Suffix { size } => BytesRange::new(
            content_length.saturating_sub(size),
            Some(size.min(content_length)),
        ),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadStreamEtag {
    Strong(String),
    Weak(String),
}

impl ReadStreamEtag {
    fn from_raw(etag: &str) -> Self {
        // RFC 9110 section 8.8.3 defines the weak indicator `W/` as case-sensitive.
        if etag.starts_with("W/\"") {
            Self::Weak(etag.to_owned())
        } else {
            Self::Strong(etag.to_owned())
        }
    }

    fn is_strong(&self) -> bool {
        matches!(self, Self::Strong(_))
    }

    fn is_weak(&self) -> bool {
        matches!(self, Self::Weak(_))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadStreamIdentity {
    version: Option<String>,
    etag: Option<ReadStreamEtag>,
    last_modified: Option<Timestamp>,
    content_length: Option<u64>,
}

impl ReadStreamIdentity {
    fn from_rp(rp: &RpRead) -> Option<Self> {
        let metadata = rp.metadata()?;
        let identity = Self {
            version: metadata.version().map(str::to_owned),
            etag: metadata.etag().map(ReadStreamEtag::from_raw),
            last_modified: metadata.last_modified(),
            content_length: metadata.is_file().then_some(metadata.content_length()),
        };
        (identity.version.is_some()
            || identity.etag.is_some()
            || identity.last_modified.is_some()
            || identity.content_length.is_some())
        .then_some(identity)
    }

    fn has_strong_etag(&self) -> bool {
        self.etag.as_ref().is_some_and(ReadStreamEtag::is_strong)
    }

    fn has_weak_etag(&self) -> bool {
        self.etag.as_ref().is_some_and(ReadStreamEtag::is_weak)
    }

    fn has_strong_validator(&self) -> bool {
        self.version.is_some() || self.has_strong_etag()
    }

    /// Return `true` when this identity carries a weak ETag without a strong
    /// `version` validator. Per RFC 9110 sections 8.8.1, 8.8.3, and 13.1.1, a
    /// weak ETag is not a strong validator and does not guarantee byte-for-byte
    /// identity across range requests.
    fn has_unverified_weak_etag(&self) -> bool {
        self.has_weak_etag() && self.version.is_none()
    }

    /// Compare a previously observed identity (`self`) with a reopened stream's
    /// identity (`other`).
    ///
    /// - Every validator present in both responses must match; any mismatch
    ///   (including a weak ETag change even when `version` matches) rejects.
    /// - When `has_progress` is `true` (at least one byte has already been
    ///   yielded), a weak ETag cannot justify byte splicing unless both
    ///   responses carry a matching `version`.
    /// - If `self` had a strong validator (`version` or strong ETag), at least
    ///   one strong validator must remain present and match in `other` so a
    ///   reopen cannot silently downgrade to `content_length` or
    ///   `last_modified` alone.
    /// - Otherwise, at least one validator field must overlap and match.
    fn matches(&self, other: &Self, has_progress: bool) -> bool {
        if matches!((&self.version, &other.version), (Some(a), Some(b)) if a != b)
            || matches!((&self.etag, &other.etag), (Some(a), Some(b)) if a != b)
            || matches!((self.last_modified, other.last_modified), (Some(a), Some(b)) if a != b)
            || matches!((self.content_length, other.content_length), (Some(a), Some(b)) if a != b)
        {
            return false;
        }

        let version_overlap = self.version.is_some() && other.version.is_some();
        let strong_etag_overlap = self.has_strong_etag() && other.has_strong_etag();

        if has_progress && (self.has_weak_etag() || other.has_weak_etag()) && !version_overlap {
            return false;
        }

        if self.has_strong_validator() {
            return version_overlap || strong_etag_overlap;
        }

        let etag_overlap = self.etag.is_some() && other.etag.is_some();
        let last_modified_overlap = self.last_modified.is_some() && other.last_modified.is_some();
        let content_length_overlap =
            self.content_length.is_some() && other.content_length.is_some();

        etag_overlap || last_modified_overlap || content_length_overlap
    }
}

struct RetryReadStream<R, I> {
    reader: Arc<R>,
    stream: Option<Box<dyn oio::ReadStreamDyn>>,
    range: BytesRange,
    read: u64,
    has_progress: bool,
    identity: Option<ReadStreamIdentity>,
    terminal_conflict: Option<&'static str>,
    policy: RetryPolicy<I>,
    deposited: Option<Arc<AtomicBool>>,
}

impl<R: oio::Read, I: RetryInterceptor> oio::ReadStream for RetryReadStream<R, I> {
    async fn read(&mut self) -> Result<Buffer> {
        if let Some(message) = self.terminal_conflict {
            return Err(Error::new(ErrorKind::Conflict, message)
                .with_operation("RetryReadStream::read")
                .set_persistent());
        }
        let mut stream = self.stream.take();
        if stream.is_none() && self.policy.budget.is_some() && self.range.size() != Some(self.read)
        {
            // The previous read ended before recovery. A new call must not
            // reopen the remaining range as a free first attempt.
            return Err(
                Error::new(ErrorKind::Unexpected, "read stream must be reopened").set_persistent(),
            );
        }

        let mut retry = self.policy.retry_loop(Operation::Read);
        let buf = loop {
            let mut active = match stream.take() {
                Some(stream) => stream,
                None => {
                    // A completed read stays at EOF across later calls.
                    if self.range.size() == Some(self.read) {
                        break Buffer::new();
                    }
                    if self.range.is_suffix() && self.read > 0 {
                        // Without the object size, the actual suffix length is
                        // unknown, so advancing it can duplicate bytes that the
                        // caller has already received.
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "cannot resume a suffix range after progress without the object size",
                        )
                        .with_operation("RetryReadStream::read")
                        .set_persistent());
                    }
                    if self.has_progress
                        && self
                            .identity
                            .as_ref()
                            .is_some_and(ReadStreamIdentity::has_unverified_weak_etag)
                    {
                        let message = "cannot resume read stream with weak etag";
                        self.terminal_conflict = Some(message);
                        return Err(Error::new(ErrorKind::Conflict, message)
                            .with_operation("RetryReadStream::read")
                            .set_persistent());
                    }
                    if !self.range.is_suffix() {
                        self.range.advance(self.read);
                        self.read = 0;
                    }
                    match self.reader.open(self.range).await {
                        Ok((rp, reopened)) => {
                            let reopened_identity = ReadStreamIdentity::from_rp(&rp);
                            match (&self.identity, &reopened_identity) {
                                (Some(expected), Some(actual))
                                    if !expected.matches(actual, self.has_progress) =>
                                {
                                    let message = "object changed during read stream retry";
                                    self.terminal_conflict = Some(message);
                                    return Err(Error::new(ErrorKind::Conflict, message)
                                        .with_operation("RetryReadStream::read")
                                        .set_persistent());
                                }
                                (Some(_), None) => {
                                    let message =
                                        "object identity missing during read stream retry";
                                    self.terminal_conflict = Some(message);
                                    return Err(Error::new(ErrorKind::Conflict, message)
                                        .with_operation("RetryReadStream::read")
                                        .set_persistent());
                                }
                                (None, Some(actual))
                                    if self.has_progress && actual.has_unverified_weak_etag() =>
                                {
                                    let message = "cannot resume read stream with weak etag";
                                    self.terminal_conflict = Some(message);
                                    return Err(Error::new(ErrorKind::Conflict, message)
                                        .with_operation("RetryReadStream::read")
                                        .set_persistent());
                                }
                                (None, _) => {
                                    self.identity = reopened_identity;
                                }
                                (Some(_), Some(_)) => {}
                            }
                            self.range = resolve_range(self.range, &rp);
                            reopened
                        }
                        Err(err) => {
                            retry.sleep_or_fail(err).await?;
                            continue;
                        }
                    }
                }
            };

            match active.read().await {
                Ok(buf) => {
                    if !buf.is_empty() {
                        self.read += buf.len() as u64;
                        self.has_progress = true;
                    }
                    self.stream = Some(active);
                    break buf;
                }
                // There are no bytes left to fetch after the resolved range completes.
                Err(err) if err.is_temporary() && self.range.size() == Some(self.read) => {
                    break Buffer::new();
                }
                // An unresolved suffix range cannot be advanced safely after
                // progress. Preserve the original error and stop retrying.
                Err(err) if err.is_temporary() && self.range.is_suffix() && self.read > 0 => {
                    return Err(err.set_persistent());
                }
                Err(err) => {
                    drop(active);
                    retry.sleep_or_fail(err).await?;
                }
            }
        };

        if (buf.is_empty() || self.range.size() == Some(self.read))
            && self
                .deposited
                .as_ref()
                .is_some_and(|flag| !flag.swap(true, Ordering::Relaxed))
        {
            self.policy.deposit(Operation::Read);
        }
        Ok(buf)
    }
}

struct RetryWrapper<R, I> {
    inner: R,
    policy: RetryPolicy<I>,
    deposited: bool,
}

impl<R, I: RetryInterceptor> RetryWrapper<R, I> {
    fn new(inner: R, policy: RetryPolicy<I>) -> Self {
        Self {
            inner,
            policy,
            deposited: false,
        }
    }

    fn deposit_once(&mut self, op: Operation) {
        if !self.deposited {
            self.deposited = true;
            self.policy.deposit(op);
        }
    }
}

impl<R: oio::Write, I: RetryInterceptor> oio::Write for RetryWrapper<R, I> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let mut retry = self.policy.retry_loop(Operation::Write);
        loop {
            match self.inner.write(bs.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        let mut retry = self.policy.retry_loop(Operation::Write);
        loop {
            match self.inner.copy_from(path, args.clone(), range).await {
                Ok(()) => return Ok(()),
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        let mut retry = self.policy.retry_loop(Operation::Write);
        loop {
            match self.inner.abort().await {
                Ok(()) => return Ok(()),
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        let mut retry = self.policy.retry_loop(Operation::Write);
        loop {
            match self.inner.close().await {
                Ok(meta) => {
                    self.deposit_once(Operation::Write);
                    return Ok(meta);
                }
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }
}

impl<P: oio::List, I: RetryInterceptor> oio::List for RetryWrapper<P, I> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let mut retry = self.policy.retry_loop(Operation::List);
        loop {
            match self.inner.next().await {
                Ok(entry) => {
                    if entry.is_none() {
                        self.deposit_once(Operation::List);
                    }
                    return Ok(entry);
                }
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }
}

impl<P: oio::Delete, I: RetryInterceptor> oio::Delete for RetryWrapper<P, I> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let mut retry = self.policy.retry_loop(Operation::Delete);
        loop {
            match self.inner.delete(path, args.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        let mut retry = self.policy.retry_loop(Operation::Delete);
        loop {
            match self.inner.close().await {
                Ok(()) => {
                    self.deposit_once(Operation::Delete);
                    return Ok(());
                }
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }
}

impl<C: oio::Copy, I: RetryInterceptor> oio::Copy for RetryWrapper<C, I> {
    async fn next(&mut self) -> Result<Option<usize>> {
        let mut retry = self.policy.retry_loop(Operation::Copy);
        loop {
            match self.inner.next().await {
                Ok(progress) => return Ok(progress),
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        let mut retry = self.policy.retry_loop(Operation::Copy);
        loop {
            match self.inner.close().await {
                Ok(meta) => {
                    self.deposit_once(Operation::Copy);
                    return Ok(meta);
                }
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        let mut retry = self.policy.retry_loop(Operation::Copy);
        loop {
            match self.inner.abort().await {
                Ok(()) => return Ok(()),
                Err(err) => retry.sleep_or_fail(err).await?,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    mod budget;
    mod cancellation;
    mod identity;
    mod range;

    use std::sync::Mutex;

    use bytes::Bytes;
    use futures::TryStreamExt;
    use futures::stream;
    use logforth::append::Testing;
    use logforth::filter::rustlog::RustLogFilterBuilder;
    use logforth::layout::TextLayout;
    use opendal_layer_logging::LoggingLayer;

    use super::*;

    #[test]
    fn registered_retry_validates_options_without_echoing_values() {
        let registry = OperatorRegistry::get();
        register_retry_layer(registry);
        assert!(registry.load("memory://?layers=retry").is_ok());
        for value in ["TrUe", "On", "FaLsE", "OfF"] {
            assert!(
                registry
                    .load(("memory://?layers=retry", [("layer.retry.jitter", value)]))
                    .is_ok()
            );
        }

        for (option, value) in [
            ("jitter", ""),
            ("jitter", "1"),
            ("factor", ""),
            ("factor", "NaN"),
            ("factor", "1e100"),
            ("factor", "0.9"),
            ("min_delay", ""),
            ("min_delay", "0s"),
            ("min_delay", "invalid-config-marker"),
            ("max_delay", ""),
            ("max_delay", "0s"),
            ("max_delay", "-1ns"),
            ("max_times", ""),
            ("max_times", "-1"),
        ] {
            let uri = format!("memory://?layers=retry&layer.retry.{option}={value}");
            let err = registry.load(uri).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
            assert!(err.to_string().contains(option));
            assert!(!err.to_string().contains("invalid-config-marker"));
            assert!(!format!("{err:?}").contains("invalid-config-marker"));
        }
    }

    #[tokio::test]
    async fn registered_retry_honors_max_times() -> Result<()> {
        struct FailOnce(AtomicBool);

        impl HttpTransport for FailOnce {
            async fn fetch(&self, _: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
                if !self.0.swap(true, Ordering::Relaxed) {
                    return Err(
                        Error::new(ErrorKind::RateLimited, "temporary failure").set_temporary()
                    );
                }
                Ok(http::Response::builder()
                    .header(http::header::CONTENT_LENGTH, "13")
                    .body(HttpBody::new(stream::empty(), Some(0)))
                    .unwrap())
            }
        }

        let registry = OperatorRegistry::get();
        opendal_service_http::register_http_service(registry);
        registry.register_context("retry_test", |ctx, _| {
            Ok(ctx.with_http_transport(HttpTransporter::new(FailOnce(AtomicBool::new(false)))))
        });
        register_retry_layer(registry);

        let op = registry.load(concat!(
            "http://localhost?context.retry_test.enabled=true",
            "&layers=retry&layer.retry.max_times=0",
        ))?;
        let err = op.stat("object").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert!(!err.is_temporary());

        let op = registry.load(concat!(
            "http://localhost?context.retry_test.enabled=true",
            "&layers=retry&layer.retry.max_times=1",
            "&layer.retry.jitter=OfF&layer.retry.factor=1.5",
            "&layer.retry.min_delay=1ms&layer.retry.max_delay=PT0.01S&layer.retry.future_option=",
        ))?;
        assert_eq!(op.stat("object").await?.content_length(), 13);
        Ok(())
    }

    #[derive(Default, Clone)]
    struct MockBuilder {
        attempt: Arc<Mutex<usize>>,
        operation_failures: Option<usize>,
    }

    impl MockBuilder {
        fn with_operation_failures(failures: usize) -> Self {
            Self {
                operation_failures: Some(failures),
                ..Default::default()
            }
        }
    }

    impl Builder for MockBuilder {
        type Config = ();

        fn build(self) -> Result<impl Service> {
            Ok(MockService {
                attempt: self.attempt,
                operation_failures: self.operation_failures,
            })
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockService {
        attempt: Arc<Mutex<usize>>,
        operation_failures: Option<usize>,
    }

    impl MockService {
        fn attempt_operation(&self) -> Result<()> {
            if let Some(failures) = self.operation_failures {
                let mut attempt = self.attempt.lock().unwrap();
                *attempt += 1;
                if *attempt <= failures {
                    return Err(
                        Error::new(ErrorKind::RateLimited, "temporary failure").set_temporary()
                    );
                }
            }
            Ok(())
        }
    }

    /// Reader returned by this backend.
    pub struct MockReader {
        backend: MockService,
    }

    impl MockReader {
        fn new(backend: MockService, _: &str, _: OpRead) -> Self {
            Self { backend }
        }
    }

    impl oio::StreamRead for MockReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            let backend = &self.backend;
            let rp = RpRead::new({
                let metadata = MetadataBuilder::file(13);
                metadata.build()
            });
            let stream = MockReadStream {
                buf: Bytes::from("Hello, World!").into(),
                range,
                attempt: backend.attempt.clone(),
            };

            Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
        }
    }

    impl Service for MockService {
        type Reader = oio::StreamReader<MockReader>;
        type Writer = MockWriter;
        type Lister = MockLister;
        type Deleter = MockDeleter;
        type Copier = MockCopier;
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock")
        }

        fn capability(&self) -> Capability {
            Capability {
                read: true,
                write: true,
                write_can_multi: true,
                delete: true,
                delete_max_size: Some(10),
                stat: true,
                list: true,
                list_with_recursive: true,
                copy: true,
                ..Default::default()
            }
        }

        async fn create_dir(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpCreateDir,
        ) -> Result<RpCreateDir> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            self.attempt_operation()?;
            Ok(RpStat::new({
                let metadata = MetadataBuilder::file(13);
                metadata.build()
            }))
        }

        fn read(&self, _: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            self.attempt_operation()?;
            Ok(oio::StreamReader::new(MockReader::new(
                self.clone(),
                path,
                args,
            )))
        }

        fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
            Ok(MockDeleter {
                size: 0,
                attempt: self.attempt.clone(),
            })
        }

        fn write(&self, _ctx: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
            Ok(MockWriter {})
        }

        fn list(&self, _ctx: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            let lister = MockLister::default();
            Ok(lister)
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
            Ok(MockCopier {
                attempt: self.attempt.clone(),
            })
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReadStream {
        buf: Buffer,
        range: BytesRange,
        attempt: Arc<Mutex<usize>>,
    }

    impl oio::ReadStream for MockReadStream {
        async fn read(&mut self) -> Result<Buffer> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            match *attempt {
                1 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                2 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                // Should read out all data.
                3 => Ok(self.buf.slice(self.range.to_range_as_usize())),
                4 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                // Should be empty.
                5 => Ok(self.buf.slice(self.range.to_range_as_usize())),
                _ => unreachable!(),
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockWriter {}

    impl oio::Write for MockWriter {
        async fn write(&mut self, _: Buffer) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Err(Error::new(ErrorKind::Unexpected, "always close failed").set_temporary())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockLister {
        attempt: usize,
    }

    impl oio::List for MockLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            self.attempt += 1;
            match self.attempt {
                1 => Err(Error::new(
                    ErrorKind::RateLimited,
                    "retryable rate limited error from lister",
                )
                .set_temporary()),
                2 => Ok(Some(oio::Entry::new(
                    "hello",
                    MetadataBuilder::file(0).build(),
                ))),
                3 => Ok(Some(oio::Entry::new(
                    "world",
                    MetadataBuilder::file(0).build(),
                ))),
                4 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable internal server error")
                        .set_temporary(),
                ),
                5 => Ok(Some(oio::Entry::new(
                    "2023/",
                    MetadataBuilder::dir().build(),
                ))),
                6 => Ok(Some(oio::Entry::new(
                    "0208/",
                    MetadataBuilder::dir().build(),
                ))),
                7 => Ok(None),
                _ => {
                    unreachable!()
                }
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockDeleter {
        size: usize,
        attempt: Arc<Mutex<usize>>,
    }

    impl oio::Delete for MockDeleter {
        async fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
            self.size += 1;
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            match *attempt {
                1 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from deleter")
                        .set_temporary(),
                ),
                2..=4 => {
                    self.size = self.size.saturating_sub(1);
                    Err(
                        Error::new(ErrorKind::Unexpected, "retryable_error from deleter")
                            .set_temporary(),
                    )
                }
                5 => {
                    self.size = self.size.saturating_sub(1);
                    if self.size == 0 {
                        Ok(())
                    } else {
                        Err(
                            Error::new(ErrorKind::Unexpected, "retryable_error from deleter")
                                .set_temporary(),
                        )
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockCopier {
        attempt: Arc<Mutex<usize>>,
    }

    impl oio::Copy for MockCopier {
        async fn next(&mut self) -> Result<Option<usize>> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            match *attempt {
                1 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from copier")
                        .set_temporary(),
                ),
                2 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from copier")
                        .set_temporary(),
                ),
                3 => Ok(Some(8)),
                4 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from copier")
                        .set_temporary(),
                ),
                5 => Ok(Some(5)),
                6 => Ok(None),
                _ => unreachable!(),
            }
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn setup() {
        let _ = logforth::starter_log::builder()
            .dispatch(|d| {
                d.filter(RustLogFilterBuilder::from_default_env().build())
                    .append(Testing::default().with_layout(TextLayout::default()))
            })
            .try_apply();
    }

    #[tokio::test]
    async fn test_retry_read() -> Result<()> {
        setup();

        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default());

        let r = op.reader("retryable_error").await?;
        let mut content = Vec::new();
        let size = r
            .read_into(&mut content, ..)
            .await
            .expect("read must succeed");
        assert_eq!(size, 13);
        assert_eq!(content, "Hello, World!".as_bytes());
        // Two retries, then the full read. The temporary error at EOF ends the
        // stream without another open.
        assert_eq!(*builder.attempt.lock().unwrap(), 4);
        Ok(())
    }

    /// A stream that breaks after its last byte is at EOF, so the layer must end
    /// the read instead of reopening the empty range left over. This applies to
    /// bounded ranges and to unbounded ranges resolved against the object size.
    #[tokio::test]
    async fn test_retry_read_bounded_range_does_not_reopen_at_eof() -> Result<()> {
        setup();

        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default());

        let r = op.reader("retryable_error").await?;
        let mut content = Vec::new();
        let size = r
            .read_into(&mut content, 0..13)
            .await
            .expect("read must succeed");
        assert_eq!(size, 13);
        assert_eq!(content, "Hello, World!".as_bytes());
        // Two retries, one full read, then the error at EOF. No reopen, so no
        // fifth attempt.
        assert_eq!(*builder.attempt.lock().unwrap(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_write_fail_on_close() -> Result<()> {
        setup();

        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())?
            .layer(
                RetryLayer::default()
                    .with_min_delay(Duration::from_millis(1))
                    .with_max_delay(Duration::from_millis(1))
                    .with_jitter(),
            )
            .layer(LoggingLayer::default());

        let mut w = op.writer("test_write").await?;
        w.write("aaa").await?;
        w.write("bbb").await?;
        match w.close().await {
            Ok(_) => (),
            Err(_) => {
                w.abort().await?;
            }
        };
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_list() -> Result<()> {
        setup();

        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())?.layer(RetryLayer::default());

        let expected = vec!["hello", "world", "2023/", "0208/"];

        let mut lister = op
            .lister("retryable_error/")
            .await
            .expect("service must support list");
        let mut actual = Vec::new();
        while let Some(obj) = lister.try_next().await.expect("must success") {
            actual.push(obj.name().to_owned());
        }

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_event_attempt_and_op() -> Result<()> {
        setup();

        #[derive(Default, Clone)]
        struct Recorder {
            events: Arc<Mutex<Vec<(Operation, u32)>>>,
        }

        impl RetryInterceptor for Recorder {
            fn intercept(&self, event: RetryEvent<'_>) {
                self.events.lock().unwrap().push((event.op, event.attempt));
            }
        }

        let recorder = Recorder::default();
        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())?.layer(
            RetryLayer::default()
                .with_min_delay(Duration::from_millis(1))
                .with_max_delay(Duration::from_millis(1))
                .with_notify(recorder.clone()),
        );

        let r = op.reader("retryable_error").await?;
        let mut content = Vec::new();
        let _ = r.read_into(&mut content, ..).await?;

        let events = recorder.events.lock().unwrap().clone();
        assert_eq!(events, vec![(Operation::Read, 1), (Operation::Read, 2)]);
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_read_stream_error_enters_retry_budget() -> Result<()> {
        setup();

        #[derive(Default, Clone)]
        struct Recorder {
            events: Arc<Mutex<Vec<(Operation, u32)>>>,
        }

        impl RetryInterceptor for Recorder {
            fn intercept(&self, event: RetryEvent<'_>) {
                self.events.lock().unwrap().push((event.op, event.attempt));
            }
        }

        let recorder = Recorder::default();
        let backend = MockService::default();
        let reader = RetryReader::new(
            oio::StreamReader::new(MockReader::new(
                backend.clone(),
                "retryable_error",
                OpRead::default(),
            )),
            RetryLayer::new()
                .with_min_delay(Duration::from_millis(1))
                .with_max_delay(Duration::from_millis(1))
                .with_notify(recorder.clone())
                .policy,
        );

        let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
        let buf = oio::ReadStream::read_all(&mut stream).await?;

        assert_eq!(buf.to_bytes(), Bytes::from_static(b"Hello, World!"));
        assert_eq!(*backend.attempt.lock().unwrap(), 4);

        let events = recorder.events.lock().unwrap().clone();
        assert_eq!(events, vec![(Operation::Read, 1), (Operation::Read, 2)]);
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_batch() -> Result<()> {
        setup();

        let builder = MockBuilder::default();
        // set to a lower delay to make it run faster
        let op = Operator::new(builder.clone())?.layer(
            RetryLayer::default()
                .with_min_delay(Duration::from_secs_f32(0.1))
                .with_max_times(5),
        );

        let paths = vec!["hello", "world", "test", "batch"];
        op.delete_stream(stream::iter(paths)).await?;
        assert_eq!(*builder.attempt.lock().unwrap(), 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_copy() -> Result<()> {
        setup();

        #[derive(Default, Clone)]
        struct Recorder {
            events: Arc<Mutex<Vec<(Operation, u32)>>>,
        }

        impl RetryInterceptor for Recorder {
            fn intercept(&self, event: RetryEvent<'_>) {
                self.events.lock().unwrap().push((event.op, event.attempt));
            }
        }

        let recorder = Recorder::default();
        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())?.layer(
            RetryLayer::default()
                .with_min_delay(Duration::from_millis(1))
                .with_max_delay(Duration::from_millis(1))
                .with_notify(recorder.clone()),
        );

        op.copy("from", "to").await.expect("copy must succeed");

        // The MockCopier returns errors on attempts 1, 2, 4 and progress
        // on attempts 3, 5; finishing on attempt 6. The retry layer must
        // retry `Copier::next` to drive the operation to completion.
        assert_eq!(*builder.attempt.lock().unwrap(), 6);

        let events = recorder.events.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                (Operation::Copy, 1),
                (Operation::Copy, 2),
                (Operation::Copy, 1),
            ],
        );
        Ok(())
    }
}
