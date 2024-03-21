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
use std::future::Future;

use std::io;

use std::sync::Arc;

use std::time::Duration;

use async_trait::async_trait;
use backon::BlockingRetryable;

use backon::ExponentialBuilder;
use backon::Retryable;
use bytes::Bytes;
use futures::FutureExt;
use log::warn;

use crate::raw::oio::ReadOperation;
use crate::raw::oio::WriteOperation;
use crate::raw::oio::{Buffer, ListOperation};
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
/// `write` and `blocking_write` don't support retry so far, visit [this issue](https://github.com/apache/opendal/issues/1223) for more details.
///
/// # Examples
///
/// ```no_run
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
///
/// ## Customize retry interceptor
///
/// RetryLayer accepts [`RetryInterceptor`] to allow users to customize
/// their own retry interceptor logic.
///
/// ```no_run
/// use std::time::Duration;
///
/// use anyhow::Result;
/// use opendal::layers::RetryInterceptor;
/// use opendal::layers::RetryLayer;
/// use opendal::services;
/// use opendal::Error;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// struct MyRetryInterceptor;
///
/// impl RetryInterceptor for MyRetryInterceptor {
///     fn intercept(&self, err: &Error, dur: Duration, ctx: &[(&str, &str)]) {
///         // do something
///     }
/// }
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(RetryLayer::new().with_notify(MyRetryInterceptor))
///     .finish();
/// ```
pub struct RetryLayer<I = DefaultRetryInterceptor> {
    builder: ExponentialBuilder,
    notify: Arc<I>,
}

impl<I> Clone for RetryLayer<I> {
    fn clone(&self) -> Self {
        Self {
            builder: self.builder.clone(),
            notify: self.notify.clone(),
        }
    }
}

impl Default for RetryLayer {
    fn default() -> Self {
        Self {
            builder: ExponentialBuilder::default(),
            notify: Arc::new(DefaultRetryInterceptor),
        }
    }
}

impl RetryLayer {
    /// Create a new retry layer.
    /// # Examples
    ///
    /// ```no_run
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

    /// Set the retry interceptor as new notify.
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use opendal::layers::RetryInterceptor;
    /// use opendal::layers::RetryLayer;
    /// use opendal::services;
    /// use opendal::Error;
    /// use opendal::Operator;
    /// use opendal::Scheme;
    ///
    /// struct MyRetryInterceptor;
    ///
    /// impl RetryInterceptor for MyRetryInterceptor {
    ///     fn intercept(&self, err: &Error, dur: Duration, ctx: &[(&str, &str)]) {
    ///         // do something
    ///     }
    /// }
    ///
    /// let _ = Operator::new(services::Memory::default())
    ///     .expect("must init")
    ///     .layer(RetryLayer::new().with_notify(MyRetryInterceptor))
    ///     .finish();
    /// ```
    pub fn with_notify<I: RetryInterceptor>(self, notify: I) -> RetryLayer<I> {
        RetryLayer {
            builder: self.builder,
            notify: Arc::new(notify),
        }
    }

    /// Set jitter of current backoff.
    ///
    /// If jitter is enabled, ExponentialBackoff will add a random jitter in `[0, min_delay)
    /// to current delay.
    pub fn with_jitter(mut self) -> Self {
        self.builder = self.builder.with_jitter();
        self
    }

    /// Set factor of current backoff.
    ///
    /// # Panics
    ///
    /// This function will panic if input factor smaller than `1.0`.
    pub fn with_factor(mut self, factor: f32) -> Self {
        self.builder = self.builder.with_factor(factor);
        self
    }

    /// Set min_delay of current backoff.
    pub fn with_min_delay(mut self, min_delay: Duration) -> Self {
        self.builder = self.builder.with_min_delay(min_delay);
        self
    }

    /// Set max_delay of current backoff.
    ///
    /// Delay will not increasing if current delay is larger than max_delay.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.builder = self.builder.with_max_delay(max_delay);
        self
    }

    /// Set max_times of current backoff.
    ///
    /// Backoff will return `None` if max times is reaching.
    pub fn with_max_times(mut self, max_times: usize) -> Self {
        self.builder = self.builder.with_max_times(max_times);
        self
    }
}

impl<A: Accessor, I: RetryInterceptor> Layer<A> for RetryLayer<I> {
    type LayeredAccessor = RetryAccessor<A, I>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        RetryAccessor {
            inner,
            builder: self.builder.clone(),
            notify: self.notify.clone(),
        }
    }
}

/// RetryInterceptor is used to intercept while retry happened.
pub trait RetryInterceptor: Send + Sync + 'static {
    /// Everytime RetryLayer is retrying, this function will be called.
    ///
    /// # Timing
    ///
    /// just before the retry sleep.
    ///
    /// # Inputs
    ///
    /// - err: The error that caused the current retry.
    /// - dur: The duration that will sleep before next retry.
    /// - ctx: The context (`name`, `value`) of current operation, like `operation` and `path`.
    ///
    /// # Notes
    ///
    /// The intercept must be quick and non-blocking. No heavy IO is
    /// allowed. Otherwise the retry will be blocked.
    fn intercept(&self, err: &Error, dur: Duration, ctx: &[(&str, &str)]);
}

/// The DefaultRetryInterceptor will log the retry error in warning level.
pub struct DefaultRetryInterceptor;

impl RetryInterceptor for DefaultRetryInterceptor {
    fn intercept(&self, err: &Error, dur: Duration, ctx: &[(&str, &str)]) {
        let context = ctx
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(" ");

        warn!(
            target: "opendal::service",
            "{} -> retry after {}s: error={}",
            context, dur.as_secs_f64(), err)
    }
}

pub struct RetryAccessor<A: Accessor, I: RetryInterceptor> {
    inner: A,
    builder: ExponentialBuilder,
    notify: Arc<I>,
}

impl<A: Accessor, I: RetryInterceptor> Debug for RetryAccessor<A, I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor, I: RetryInterceptor> LayeredAccessor for RetryAccessor<A, I> {
    type Inner = A;
    type Reader = RetryWrapper<A::Reader, I>;
    type BlockingReader = RetryWrapper<A::BlockingReader, I>;
    type Writer = RetryWrapper<A::Writer, I>;
    type BlockingWriter = RetryWrapper<A::BlockingWriter, I>;
    type Lister = RetryWrapper<A::Lister, I>;
    type BlockingLister = RetryWrapper<A::BlockingLister, I>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        { || self.inner.create_dir(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur: Duration| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::CreateDir.into_static()),
                        ("path", path),
                    ],
                )
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        { || self.inner.read(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[("operation", Operation::Read.into_static()), ("path", path)],
                )
            })
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        RetryWrapper::new(r, self.notify.clone(), path, self.builder.clone()),
                    )
                })
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
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::Write.into_static()),
                        ("path", path),
                    ],
                )
            })
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        RetryWrapper::new(r, self.notify.clone(), path, self.builder.clone()),
                    )
                })
                .map_err(|e| e.set_persistent())
            })
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        { || self.inner.stat(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[("operation", Operation::Stat.into_static()), ("path", path)],
                )
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        { || self.inner.delete(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::Delete.into_static()),
                        ("path", path),
                    ],
                )
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        { || self.inner.copy(from, to, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::Copy.into_static()),
                        ("from", from),
                        ("to", to),
                    ],
                )
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        { || self.inner.rename(from, to, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::Rename.into_static()),
                        ("from", from),
                        ("to", to),
                    ],
                )
            })
            .map(|v| v.map_err(|e| e.set_persistent()))
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        { || self.inner.list(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[("operation", Operation::List.into_static()), ("path", path)],
                )
            })
            .map(|v| {
                v.map(|(l, p)| {
                    let lister =
                        RetryWrapper::new(p, self.notify.clone(), path, self.builder.clone());
                    (l, lister)
                })
                .map_err(|e| e.set_persistent())
            })
            .await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        {
            || async {
                let rp = self.inner.batch(args.clone()).await?;
                let mut nrp = Vec::with_capacity(rp.results().len());
                for (path, result) in rp.into_results() {
                    let result = result?;
                    nrp.push((path, Ok(result)))
                }
                Ok(RpBatch::new(nrp))
            }
        }
        .retry(&self.builder)
        .when(|e: &Error| e.is_temporary())
        .notify(|err, dur| {
            self.notify.intercept(
                err,
                dur,
                &[
                    ("operation", Operation::Batch.into_static()),
                    ("count", &args.operation().len().to_string()),
                ],
            )
        })
        .await
        .map_err(|e| e.set_persistent())
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        { || self.inner.blocking_create_dir(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::BlockingCreateDir.into_static()),
                        ("path", path),
                    ],
                )
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        { || self.inner.blocking_read(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::BlockingRead.into_static()),
                        ("path", path),
                    ],
                )
            })
            .call()
            .map(|(rp, r)| {
                (
                    rp,
                    RetryWrapper::new(r, self.notify.clone(), path, self.builder.clone()),
                )
            })
            .map_err(|e| e.set_persistent())
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        { || self.inner.blocking_write(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::BlockingWrite.into_static()),
                        ("path", path),
                    ],
                )
            })
            .call()
            .map(|(rp, r)| {
                (
                    rp,
                    RetryWrapper::new(r, self.notify.clone(), path, self.builder.clone()),
                )
            })
            .map_err(|e| e.set_persistent())
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        { || self.inner.blocking_stat(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::BlockingStat.into_static()),
                        ("path", path),
                    ],
                )
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        { || self.inner.blocking_delete(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::BlockingDelete.into_static()),
                        ("path", path),
                    ],
                )
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        { || self.inner.blocking_copy(from, to, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::BlockingCopy.into_static()),
                        ("from", from),
                        ("to", to),
                    ],
                )
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        { || self.inner.blocking_rename(from, to, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::BlockingRename.into_static()),
                        ("from", from),
                        ("to", to),
                    ],
                )
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        { || self.inner.blocking_list(path, args.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", Operation::BlockingList.into_static()),
                        ("path", path),
                    ],
                )
            })
            .call()
            .map(|(rp, p)| {
                let p = RetryWrapper::new(p, self.notify.clone(), path, self.builder.clone());
                (rp, p)
            })
            .map_err(|e| e.set_persistent())
    }
}

pub struct RetryWrapper<R, I> {
    inner: Option<R>,
    notify: Arc<I>,

    path: String,
    builder: ExponentialBuilder,
}

impl<R, I> RetryWrapper<R, I> {
    fn new(inner: R, notify: Arc<I>, path: &str, backoff: ExponentialBuilder) -> Self {
        Self {
            inner: Some(inner),
            notify,

            path: path.to_string(),
            builder: backoff,
        }
    }
}

impl<R: oio::Read, I: RetryInterceptor> oio::Read for RetryWrapper<R, I> {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<Buffer> {
        {
            || {
                self.inner
                    .as_ref()
                    .expect("inner must be valid")
                    .read_at(offset, limit)
            }
        }
        .retry(&self.builder)
        .when(|e| e.is_temporary())
        .notify(|err, dur| {
            self.notify.intercept(
                err,
                dur,
                &[
                    ("operation", ReadOperation::Seek.into_static()),
                    ("path", &self.path),
                ],
            )
        })
        .await
        .map_err(|e| e.set_persistent())
    }
}

impl<R: oio::BlockingRead, I: RetryInterceptor> oio::BlockingRead for RetryWrapper<R, I> {
    fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        { || self.inner.as_ref().unwrap().read_at(offset, limit) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", ReadOperation::BlockingRead.into_static()),
                        ("path", &self.path),
                    ],
                );
            })
            .call()
            .map_err(|e| e.set_persistent())
    }
}

impl<R: oio::Write, I: RetryInterceptor> oio::Write for RetryWrapper<R, I> {
    async fn write(&mut self, bs: Bytes) -> Result<usize> {
        use backon::RetryableWithContext;

        let inner = self.inner.take().expect("inner must be valid");

        let ((inner, _), res) = {
            |(mut r, bs): (R, Bytes)| async move {
                let res = r.write(bs.clone()).await;

                ((r, bs), res)
            }
        }
        .retry(&self.builder)
        .when(|e| e.is_temporary())
        .context((inner, bs))
        .notify(|err, dur| {
            self.notify.intercept(
                err,
                dur,
                &[
                    ("operation", WriteOperation::Write.into_static()),
                    ("path", &self.path),
                ],
            )
        })
        .map(|(r, res)| (r, res.map_err(|err| err.set_persistent())))
        .await;

        self.inner = Some(inner);
        res
    }

    async fn abort(&mut self) -> Result<()> {
        use backon::RetryableWithContext;

        let inner = self.inner.take().expect("inner must be valid");

        let (inner, res) = {
            |mut r: R| async move {
                let res = r.abort().await;

                (r, res)
            }
        }
        .retry(&self.builder)
        .when(|e| e.is_temporary())
        .context(inner)
        .notify(|err, dur| {
            self.notify.intercept(
                err,
                dur,
                &[
                    ("operation", WriteOperation::Abort.into_static()),
                    ("path", &self.path),
                ],
            )
        })
        .map(|(r, res)| (r, res.map_err(|err| err.set_persistent())))
        .await;

        self.inner = Some(inner);
        res
    }

    async fn close(&mut self) -> Result<()> {
        use backon::RetryableWithContext;

        let inner = self.inner.take().expect("inner must be valid");

        let (inner, res) = {
            |mut r: R| async move {
                let res = r.close().await;

                (r, res)
            }
        }
        .retry(&self.builder)
        .when(|e| e.is_temporary())
        .context(inner)
        .notify(|err, dur| {
            self.notify.intercept(
                err,
                dur,
                &[
                    ("operation", WriteOperation::Close.into_static()),
                    ("path", &self.path),
                ],
            )
        })
        .map(|(r, res)| (r, res.map_err(|err| err.set_persistent())))
        .await;

        self.inner = Some(inner);
        res
    }
}

impl<R: oio::BlockingWrite, I: RetryInterceptor> oio::BlockingWrite for RetryWrapper<R, I> {
    fn write(&mut self, bs: Bytes) -> Result<usize> {
        { || self.inner.as_mut().unwrap().write(bs.clone()) }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", WriteOperation::BlockingWrite.into_static()),
                        ("path", &self.path),
                    ],
                );
            })
            .call()
            .map_err(|e| e.set_persistent())
    }

    fn close(&mut self) -> Result<()> {
        { || self.inner.as_mut().unwrap().close() }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", WriteOperation::BlockingClose.into_static()),
                        ("path", &self.path),
                    ],
                );
            })
            .call()
            .map_err(|e| e.set_persistent())
    }
}

impl<P: oio::List, I: RetryInterceptor> oio::List for RetryWrapper<P, I> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        use backon::RetryableWithContext;

        let inner = self.inner.take().expect("inner must be valid");

        let (inner, res) = {
            |mut p: P| async move {
                let res = p.next().await;

                (p, res)
            }
        }
        .retry(&self.builder)
        .when(|e| e.is_temporary())
        .context(inner)
        .notify(|err, dur| {
            self.notify.intercept(
                err,
                dur,
                &[
                    ("operation", ListOperation::Next.into_static()),
                    ("path", &self.path),
                ],
            )
        })
        .map(|(r, res)| (r, res.map_err(|err| err.set_persistent())))
        .await;

        self.inner = Some(inner);
        res
    }
}

impl<P: oio::BlockingList, I: RetryInterceptor> oio::BlockingList for RetryWrapper<P, I> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        { || self.inner.as_mut().unwrap().next() }
            .retry(&self.builder)
            .when(|e| e.is_temporary())
            .notify(|err, dur| {
                self.notify.intercept(
                    err,
                    dur,
                    &[
                        ("operation", ListOperation::BlockingNext.into_static()),
                        ("path", &self.path),
                    ],
                );
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

    use async_trait::async_trait;
    use bytes::Bytes;
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

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Accessor for MockService {
        type Reader = MockReader;
        type Writer = ();
        type Lister = MockLister;
        type BlockingReader = ();
        type BlockingWriter = ();
        type BlockingLister = ();

        fn info(&self) -> AccessorInfo {
            let mut am = AccessorInfo::default();
            am.set_native_capability(Capability {
                read: true,
                list: true,
                list_with_recursive: true,
                batch: true,
                ..Default::default()
            });

            am
        }

        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
            Ok((
                RpRead::new(),
                MockReader {
                    attempt: self.attempt.clone(),
                    pos: 0,
                },
            ))
        }

        async fn list(&self, _: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
            let lister = MockLister::default();
            Ok((RpList::default(), lister))
        }

        async fn batch(&self, op: OpBatch) -> Result<RpBatch> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            match *attempt {
                1 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                2 => Ok(RpBatch::new(
                    op.into_operation()
                        .into_iter()
                        .map(|(s, _)| {
                            (
                                s,
                                Err(Error::new(
                                    ErrorKind::Unexpected,
                                    "retryable_error from reader",
                                )
                                .set_temporary()),
                            )
                        })
                        .collect(),
                )),
                3 => Ok(RpBatch::new(
                    op.into_operation()
                        .into_iter()
                        .enumerate()
                        .map(|(i, (s, _))| {
                            (
                                s,
                                match i {
                                    0 => Err(Error::new(
                                        ErrorKind::Unexpected,
                                        "retryable_error from reader",
                                    )
                                    .set_temporary()),
                                    _ => Ok(RpDelete {}.into()),
                                },
                            )
                        })
                        .collect(),
                )),
                4 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                5 => Ok(RpBatch::new(
                    op.into_operation()
                        .into_iter()
                        .map(|(s, _)| (s, Ok(RpDelete {}.into())))
                        .collect(),
                )),
                _ => unreachable!(),
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReader {
        attempt: Arc<Mutex<usize>>,
        pos: u64,
    }

    impl oio::Read for MockReader {
        async fn read_at(&self, _: u64, _: usize) -> Result<oio::Buffer> {
            let mut attempt = self.attempt.lock().unwrap();
            *attempt += 1;

            match *attempt {
                1 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                2 => Ok(Bytes::copy_from_slice("Hello, ".as_bytes()).into()),
                3 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable_error from reader")
                        .set_temporary(),
                ),
                4 => Ok(Bytes::copy_from_slice("World!".as_bytes()).into()),
                5 => Ok(Bytes::new().into()),
                _ => unreachable!(),
            }
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
                    Metadata::new(EntryMode::FILE),
                ))),
                3 => Ok(Some(oio::Entry::new(
                    "world",
                    Metadata::new(EntryMode::FILE),
                ))),
                4 => Err(
                    Error::new(ErrorKind::Unexpected, "retryable internal server error")
                        .set_temporary(),
                ),
                5 => Ok(Some(oio::Entry::new(
                    "2023/",
                    Metadata::new(EntryMode::DIR),
                ))),
                6 => Ok(Some(oio::Entry::new(
                    "0208/",
                    Metadata::new(EntryMode::DIR),
                ))),
                7 => Ok(None),
                _ => {
                    unreachable!()
                }
            }
        }
    }

    #[tokio::test]
    async fn test_retry_read() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

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
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let builder = MockBuilder::default();
        let op = Operator::new(builder.clone())
            .unwrap()
            .layer(RetryLayer::new())
            .finish();

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
    }

    #[tokio::test]
    async fn test_retry_batch() {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let builder = MockBuilder::default();
        // set to a lower delay to make it run faster
        let op = Operator::new(builder.clone())
            .unwrap()
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_secs_f32(0.1))
                    .with_max_times(5),
            )
            .finish();

        let paths = vec![
            "hello".into(),
            "world".into(),
            "test".into(),
            "batch".into(),
        ];
        op.remove(paths).await.expect("batch must succeed");
        assert_eq!(*builder.attempt.lock().unwrap(), 5);
    }
}
