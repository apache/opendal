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
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use futures::TryFutureExt;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use crate::raw::Accessor;
use crate::raw::*;
use crate::*;

/// Add [prometheus](https://docs.rs/prometheus) for every operations.
///
/// # Examples
///
/// ```
/// use log::debug;
/// use log::info;
/// use opendal::layers::PrometheusClientLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Result;
///
/// /// Visit [`opendal::services`] for more service related config.
/// /// Visit [`opendal::Operator`] for more operator level APIs.
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Pick a builder and configure it.
///     let builder = services::Memory::default();
///     let mut registry = prometheus_client::registry::Registry::default();
///
///     let op = Operator::new(builder)
///         .expect("must init")
///         .layer(PrometheusClientLayer::with_registry(&mut registry))
///         .finish();
///     debug!("operator: {op:?}");
///
///     // Write data into object test.
///     op.write("test", "Hello, World!").await?;
///     // Read data from object.
///     let bs = op.read("test").await?;
///     info!("content: {}", String::from_utf8_lossy(&bs));
///
///     // Get object metadata.
///     let meta = op.stat("test").await?;
///     info!("meta: {:?}", meta);
///
///     // Export prometheus metrics.
///     let mut buf = String::new();
///     prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
///     println!("## Prometheus Metrics");
///     println!("{}", buf);
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PrometheusClientLayer {
    metrics: PrometheusClientMetrics,
}

impl PrometheusClientLayer {
    /// Create PrometheusClientLayer while registering itself to this registry. Please keep in caution
    /// that do NOT call this method multiple times with a same registry. If you want initialize multiple
    /// [`PrometheusClientLayer`] with a single registry, you should use [`clone`] instead.
    pub fn new(registry: &mut Registry) -> Self {
        let metrics = PrometheusClientMetrics::register(registry);
        Self { metrics }
    }
}

impl<A: Accessor> Layer<A> for PrometheusClientLayer {
    type LayeredAccessor = PrometheusAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.info();
        let scheme = meta.scheme();

        let metrics = Arc::new(self.metrics.clone());
        PrometheusAccessor {
            inner,
            metrics,
            scheme,
        }
    }
}

type OperationLabels = [(&'static str, &'static str); 2];
type ErrorLabels = [(&'static str, &'static str); 3];

/// [`PrometheusClientMetrics`] provide the performance and IO metrics with the `prometheus-client` crate.
#[derive(Debug, Clone)]
struct PrometheusClientMetrics {
    /// Total counter of the specific operation be called.
    requests_total: Family<OperationLabels, Counter>,
    /// Total counter of the errors.
    errors_total: Family<ErrorLabels, Counter>,
    /// Latency of the specific operation be called.
    request_duration_seconds: Family<OperationLabels, Histogram>,
    /// The histogram of bytes
    bytes_histogram: Family<OperationLabels, Histogram>,
    /// The counter of bytes
    bytes_total: Family<OperationLabels, Counter>,
}

impl PrometheusClientMetrics {
    pub fn register(registry: &mut Registry) -> Self {
        let requests_total = Family::default();
        let errors_total = Family::default();
        let bytes_total = Family::default();
        let request_duration_seconds = Family::<OperationLabels, _>::new_with_constructor(|| {
            let buckets = histogram::exponential_buckets(0.01, 2.0, 16);
            Histogram::new(buckets)
        });
        let bytes_histogram = Family::<OperationLabels, _>::new_with_constructor(|| {
            let buckets = histogram::exponential_buckets(1.0, 2.0, 16);
            Histogram::new(buckets)
        });

        registry.register("opendal_requests", "", requests_total.clone());
        registry.register("opendal_errors", "", errors_total.clone());
        registry.register(
            "opendal_request_duration_seconds",
            "",
            request_duration_seconds.clone(),
        );
        registry.register("opendal_bytes_histogram", "", bytes_histogram.clone());
        registry.register("opendal_bytes", "", bytes_total.clone());
        Self {
            requests_total,
            errors_total,
            request_duration_seconds,
            bytes_histogram,
            bytes_total,
        }
    }

    fn increment_errors_total(&self, scheme: Scheme, op: Operation, err: ErrorKind) {
        let labels = [
            ("scheme", scheme.into_static()),
            ("op", op.into_static()),
            ("err", err.into_static()),
        ];
        self.errors_total.get_or_create(&labels).inc();
    }

    fn increment_request_total(&self, scheme: Scheme, op: Operation) {
        let labels = [("scheme", scheme.into_static()), ("op", op.into_static())];
        self.requests_total.get_or_create(&labels).inc();
    }

    fn observe_bytes_total(&self, scheme: Scheme, op: Operation, bytes: usize) {
        let labels = [("scheme", scheme.into_static()), ("op", op.into_static())];
        self.bytes_histogram
            .get_or_create(&labels)
            .observe(bytes as f64);
        self.bytes_total.get_or_create(&labels).inc_by(bytes as u64);
    }

    fn observe_request_duration(&self, scheme: Scheme, op: Operation, duration: Duration) {
        let labels = [("scheme", scheme.into_static()), ("op", op.into_static())];
        self.request_duration_seconds
            .get_or_create(&labels)
            .observe(duration.as_secs_f64());
    }
}

#[derive(Clone)]
pub struct PrometheusAccessor<A: Accessor> {
    inner: A,
    metrics: Arc<PrometheusClientMetrics>,
    scheme: Scheme,
}

impl<A: Accessor> Debug for PrometheusAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for PrometheusAccessor<A> {
    type Inner = A;
    type Reader = PrometheusMetricWrapper<A::Reader>;
    type BlockingReader = PrometheusMetricWrapper<A::BlockingReader>;
    type Writer = PrometheusMetricWrapper<A::Writer>;
    type BlockingWriter = PrometheusMetricWrapper<A::BlockingWriter>;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.metrics
            .increment_request_total(self.scheme, Operation::CreateDir);

        let start_time = Instant::now();
        let create_res = self.inner.create_dir(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::CreateDir,
            start_time.elapsed(),
        );
        create_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::CreateDir, e.kind());
            e
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Read);

        let read_res = self
            .inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        PrometheusMetricWrapper::new(
                            r,
                            Operation::Read,
                            self.metrics.clone(),
                            self.scheme,
                        ),
                    )
                })
            })
            .await;
        read_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Read, e.kind());
            e
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Write);

        let write_res = self
            .inner
            .write(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        PrometheusMetricWrapper::new(
                            r,
                            Operation::Write,
                            self.metrics.clone(),
                            self.scheme,
                        ),
                    )
                })
            })
            .await;

        write_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Write, e.kind());
            e
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Stat);
        let start_time = Instant::now();

        let stat_res = self
            .inner
            .stat(path, args)
            .inspect_err(|e| {
                self.metrics
                    .increment_errors_total(self.scheme, Operation::Stat, e.kind());
            })
            .await;

        self.metrics
            .observe_request_duration(self.scheme, Operation::Stat, start_time.elapsed());
        stat_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Stat, e.kind());
            e
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Delete);
        let start_time = Instant::now();

        let delete_res = self.inner.delete(path, args).await;

        self.metrics
            .observe_request_duration(self.scheme, Operation::Delete, start_time.elapsed());
        delete_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Delete, e.kind());
            e
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::List);
        let start_time = Instant::now();

        let list_res = self.inner.list(path, args).await;

        self.metrics
            .observe_request_duration(self.scheme, Operation::List, start_time.elapsed());
        list_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::List, e.kind());
            e
        })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Batch);
        let start_time = Instant::now();

        let result = self.inner.batch(args).await;

        self.metrics
            .observe_request_duration(self.scheme, Operation::Batch, start_time.elapsed());
        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Batch, e.kind());
            e
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Presign);
        let start_time = Instant::now();

        let result = self.inner.presign(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::Presign,
            start_time.elapsed(),
        );
        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Presign, e.kind());
            e
        })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingCreateDir);
        let start_time = Instant::now();

        let result = self.inner.blocking_create_dir(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingCreateDir,
            start_time.elapsed(),
        );
        result.map_err(|e| {
            self.metrics.increment_errors_total(
                self.scheme,
                Operation::BlockingCreateDir,
                e.kind(),
            );
            e
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingRead);

        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(
                    r,
                    Operation::BlockingRead,
                    self.metrics.clone(),
                    self.scheme,
                ),
            )
        });

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingRead, e.kind());
            e
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingWrite);

        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(
                    r,
                    Operation::BlockingWrite,
                    self.metrics.clone(),
                    self.scheme,
                ),
            )
        });

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingWrite, e.kind());
            e
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingStat);
        let start_time = Instant::now();

        let result = self.inner.blocking_stat(path, args);
        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingStat,
            start_time.elapsed(),
        );

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingStat, e.kind());
            e
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingDelete);
        let start_time = Instant::now();

        let result = self.inner.blocking_delete(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingDelete,
            start_time.elapsed(),
        );
        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingDelete, e.kind());
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingList);
        let start_time = Instant::now();

        let result = self.inner.blocking_list(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingList,
            start_time.elapsed(),
        );
        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingList, e.kind());
            e
        })
    }
}

pub struct PrometheusMetricWrapper<R> {
    inner: R,

    op: Operation,
    metrics: Arc<PrometheusClientMetrics>,
    scheme: Scheme,
    bytes_total: usize,
    start_time: Instant,
}

impl<R> PrometheusMetricWrapper<R> {
    fn new(inner: R, op: Operation, metrics: Arc<PrometheusClientMetrics>, scheme: Scheme) -> Self {
        Self {
            inner,
            op,
            metrics,
            scheme,
            bytes_total: 0,
            start_time: Instant::now(),
        }
    }
}

impl<R: oio::Read> oio::Read for PrometheusMetricWrapper<R> {
    async fn read(&mut self, limit: usize) -> Result<Bytes> {
        match self.inner.read(limit).await {
            Ok(bytes) => {
                self.bytes_total += bytes.len();
                Ok(bytes)
            }
            Err(e) => {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, e.kind());
                Err(e)
            }
        }
    }

    async fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        match self.inner.seek(pos).await {
            Ok(n) => Ok(n),
            Err(e) => {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, e.kind());
                Err(e)
            }
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for PrometheusMetricWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner
            .read(buf)
            .map(|n| {
                self.bytes_total += n;
                n
            })
            .map_err(|e| {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, e.kind());
                e
            })
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos).map_err(|err| {
            self.metrics
                .increment_errors_total(self.scheme, self.op, err.kind());
            err
        })
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        self.inner.next().map(|res| match res {
            Ok(bytes) => {
                self.bytes_total += bytes.len();
                Ok(bytes)
            }
            Err(e) => {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, e.kind());
                Err(e)
            }
        })
    }
}

impl<R: oio::Write> oio::Write for PrometheusMetricWrapper<R> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        self.inner
            .poll_write(cx, bs)
            .map_ok(|n| {
                self.bytes_total += n;
                n
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, err.kind());
                err
            })
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_abort(cx).map_err(|err| {
            self.metrics
                .increment_errors_total(self.scheme, self.op, err.kind());
            err
        })
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_close(cx).map_err(|err| {
            self.metrics
                .increment_errors_total(self.scheme, self.op, err.kind());
            err
        })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for PrometheusMetricWrapper<R> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        self.inner
            .write(bs)
            .map(|n| {
                self.bytes_total += n;
                n
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, err.kind());
                err
            })
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close().map_err(|err| {
            self.metrics
                .increment_errors_total(self.scheme, self.op, err.kind());
            err
        })
    }
}

impl<R> Drop for PrometheusMetricWrapper<R> {
    fn drop(&mut self) {
        self.metrics
            .observe_bytes_total(self.scheme, self.op, self.bytes_total);
        self.metrics
            .observe_request_duration(self.scheme, self.op, self.start_time.elapsed());
    }
}
