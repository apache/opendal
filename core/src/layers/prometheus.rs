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
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use futures::TryFutureExt;
use log::debug;
use prometheus;

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
/// use opendal::layers::PrometheusLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Result;
/// use prometheus::Encoder;
///
/// /// Visit [`opendal::services`] for more service related config.
/// /// Visit [`opendal::Operator`] for more operator level APIs.
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Pick a builder and configure it.
///     let builder = services::Memory::default();
///     let registry = prometheus::default_registry();
///
///     let op = Operator::new(builder)
///         .expect("must init")
///         .layer(PrometheusLayer::with_registry(registry.clone()))
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
///     let mut buffer = Vec::<u8>::new();
///     let encoder = prometheus::TextEncoder::new();
///     encoder.encode(&prometheus::gather(), &mut buffer).unwrap();
///     println!("## Prometheus Metrics");
///     println!("{}", String::from_utf8(buffer.clone()).unwrap());
///     Ok(())
/// }
/// ```
#[derive(Default, Debug, Clone)]
pub struct PrometheusLayer {
    registry: prometheus::Registry,
}

impl PrometheusLayer {
    /// create PrometheusLayer by incoming registry.
    pub fn with_registry(registry: prometheus::Registry) -> Self {
        Self { registry }
    }
}

impl<A: Accessor, M: PrometheusLayerMetrics> Layer<A> for PrometheusLayer {
    type LayeredAccessor = PrometheusAccessor<A, M>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.info();
        let scheme = meta.scheme();

        PrometheusAccessor {
            inner,
            stats: Arc::new(PrometheusLibMetrics::new(self.registry.clone())),
            scheme: scheme.to_string(),
        }
    }
}

/// [`LayerPrometheusMetrics`] is called on every operation in [`PrometheusAccessor`].
trait PrometheusLayerMetrics {
    fn increment_errors_total(&self, op: Operation, kind: ErrorKind);
    fn increment_request_total(&self, scheme: &str, op: Operation);
    fn observe_bytes_total(&self, scheme: &str, op: Operation, bytes: usize);
    fn observe_request_duration(&self, scheme: &str, op: Operation, duration: std::time::Duration);
}

/// [`PrometheusLibMetrics`] provide the performance and IO metrics with the `prometheus` crate.
#[derive(Debug)]
struct PrometheusLibMetrics {
    /// Total times of the specific operation be called.
    pub requests_total: prometheus::core::GenericCounterVec<prometheus::core::AtomicU64>,
    /// Latency of the specific operation be called.
    pub requests_duration_seconds: prometheus::HistogramVec,
    /// Size of the specific metrics.
    pub bytes_total: prometheus::HistogramVec,
}

impl PrometheusLibMetrics {
    /// new with prometheus register.
    pub fn new(registry: prometheus::Registry) -> Self {
        let requests_total = prometheus::register_int_counter_vec_with_registry!(
            "requests_total",
            "Total times of create be called",
            &["scheme", "operation"],
            registry
        )
            .unwrap();
        let opts = prometheus::histogram_opts!(
            "requests_duration_seconds",
            "Histogram of the time spent on specific operation",
            prometheus::exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let requests_duration_seconds =
            prometheus::register_histogram_vec_with_registry!(opts, &["scheme", "operation"], registry)
                .unwrap();

        let opts = prometheus::histogram_opts!(
            "bytes_total",
            "Total size of ",
            prometheus::exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let bytes_total =
            prometheus::register_histogram_vec_with_registry!(opts, &["scheme", "operation"], registry)
                .unwrap();

        Self {
            requests_total,
            requests_duration_seconds,
            bytes_total,
        }
    }
}

impl PrometheusLayerMetrics for PrometheusLibMetrics {
    /// error handling is the cold path, so we will not init error counters
    /// in advance.
    fn increment_errors_total(&self, op: Operation, kind: ErrorKind) {
        debug!(
            "Prometheus statistics metrics error, operation {} error {}",
            op.into_static(),
            kind.into_static()
        );
    }

    fn increment_request_total(&self, scheme: &str, op: Operation) {
        self.requests_total
            .with_label_values(&[scheme, op.into_static()])
            .inc();
    }

    fn observe_bytes_total(&self, scheme: &str, op: Operation, bytes: usize) {
        self.bytes_total
            .with_label_values(&[scheme, op.into_static()])
            .observe(bytes as f64);
    }

    fn observe_request_duration(&self, scheme: &str, op: Operation, duration: std::time::Duration) {
        self.requests_duration_seconds
            .with_label_values(&[scheme, op.into_static()])
            .observe(duration.as_secs_f64());
    }
}

#[derive(Clone)]
pub struct PrometheusAccessor<A: Accessor, M: PrometheusLayerMetrics> {
    inner: A,
    stats: Arc<M>,
    scheme: String,
}

impl<A: Accessor, M> Debug for PrometheusAccessor<A, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<A: Accessor, M: PrometheusLayerMetrics> LayeredAccessor for PrometheusAccessor<A, M> {
    type Inner = A;
    type Reader = PrometheusMetricWrapper<A::Reader>;
    type BlockingReader = PrometheusMetricWrapper<A::BlockingReader>;
    type Writer = PrometheusMetricWrapper<A::Writer>;
    type BlockingWriter = PrometheusMetricWrapper<A::BlockingWriter>;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.stats.increment_request_total(&self.scheme, Operation::CreateDir);

        let start_time = Instant::now();
        let create_res = self.inner.create_dir(path, args).await;

        self.stats.observe_request_duration(&self.scheme, Operation::CreateDir, start_time.elapsed());
        create_res.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::CreateDir, e.kind());
            e
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.stats.increment_request_total(&self.scheme, Operation::Read);
        let start_time = Instant::now();

        let read_res = self
            .inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    self.stats.observe_bytes_total(&self.scheme, Operation::Read, rp.metadata().content_length() as usize);
                    (
                        rp,
                        PrometheusMetricWrapper::new(
                            r,
                            Operation::Read,
                            self.stats.clone(),
                            &self.scheme,
                        ),
                    )
                })
            })
            .await;
        self.stats.observe_request_duration(&self.scheme, Operation::Read, start_time.elapsed());

        read_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::Read, e.kind());
            e
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.stats.increment_request_total(&self.scheme, Operation::Write);
        let start_time = Instant::now();

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
                            self.stats.clone(),
                            &self.scheme,
                        ),
                    )
                })
            })
            .await;

        self.stats.observe_request_duration(&self.scheme, Operation::Write, start_time.elapsed());
        write_res.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Write, e.kind());
            e
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.stats.increment_request_total(&self.scheme, Operation::Stat);
        let start_time = Instant::now();

        let stat_res = self
            .inner
            .stat(path, args)
            .inspect_err(|e| {
                self.stats.increment_errors_total(Operation::Stat, e.kind());
            })
            .await;

        self.stats.observe_request_duration(&self.scheme, Operation::Stat, start_time.elapsed());
        stat_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::Stat, e.kind());
            e
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.stats.increment_request_total(&self.scheme, Operation::Delete);
        let start_time = Instant::now();


        let delete_res = self.inner.delete(path, args).await;

        self.stats.observe_request_duration(&self.scheme, Operation::Delete, start_time.elapsed());
        delete_res.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Delete, e.kind());
            e
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.stats.increment_request_total(&self.scheme, Operation::List);
        let start_time = Intant::now();

        let list_res = self.inner.list(path, args).await;

        self.stats.observe_request_duration(&self.scheme, Operation::List, start_time.elapsed());
        list_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::List, e.kind());
            e
        })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.stats.increment_request_total(&self.scheme, Operation::Batch);
        let start_time = Instant::now();

        let result = self.inner.batch(args).await;

        self.stats.observe_request_duration(&self.scheme, Operation::Batch, start_time.elapsed());
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Batch, e.kind());
            e
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.stats.increment_request_total(&self.scheme, Operation::Presign);
        let start_time = Instant::now();

        let result = self.inner.presign(path, args).await;

        self.stats.observe_request_duration(&self.scheme, Operation::Presign, start_time.elapsed());
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Presign, e.kind());
            e
        })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.stats.increment_request_total(&self.scheme, Operation::BlockingCreateDir);
        let start_time = Instant::now();

        let result = self.inner.blocking_create_dir(path, args);

        self.stats.observe_request_duration(&self.scheme, Operation::BlockingCreateDir, start_time.elapsed());
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingCreateDir, e.kind());
            e
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.stats.increment_request_total(&self.scheme, Operation::BlockingRead);
        let start_time = Instant::now();

        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            self.stats.observe_bytes_total(&self.scheme, Operation::BlockingRead, rp.metadata().content_length() as usize);
            (
                rp,
                PrometheusMetricWrapper::new(
                    r,
                    Operation::BlockingRead,
                    self.stats.clone(),
                    &self.scheme,
                ),
            )
        });

        self.stats.observe_request_duration(&self.scheme, Operation::BlockingRead, start_time.elapsed());
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingRead, e.kind());
            e
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.stats.increment_request_total(&self.scheme, Operation::BlockingWrite);
        let start_time = Instant::now();

        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(
                    r,
                    Operation::BlockingWrite,
                    self.stats.clone(),
                    &self.scheme,
                ),
            )
        });

        self.stats.observe_request_duration(&self.scheme, Operation::BlockingWrite, start_time.elapsed());
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingWrite, e.kind());
            e
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.stats.increment_request_total(&self.scheme, Operation::BlockingStat);
        let start_time = Instant::now();

        let result = self.inner.blocking_stat(path, args);
        self.stats.observe_request_duration(&self.scheme, Operation::BlockingStat, start_time.elapsed());
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingStat, e.kind());
            e
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.stats.increment_request_total(&self.scheme, Operation::BlockingDelete);
        let start_time = Instant::now();

        let result = self.inner.blocking_delete(path, args);

        self.stats.observe_request_duration(&self.scheme, Operation::BlockingDelete, start_time.elapsed());
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingDelete, e.kind());
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.stats.increment_request_total(&self.scheme, Operation::BlockingList);
        let start_time = Instant::now();

        let result = self.inner.blocking_list(path, args);

        self.stats.observe_request_duration(&self.scheme, Operation::BlockingList, start_time.elapsed());
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingList, e.kind());
            e
        })
    }
}

pub struct PrometheusMetricWrapper<R> {
    inner: R,

    op: Operation,
    stats: Arc<PrometheusLibMetrics>,
    scheme: String,
}

impl<R> PrometheusMetricWrapper<R> {
    fn new(inner: R, op: Operation, stats: Arc<PrometheusLibMetrics>, scheme: &String) -> Self {
        Self {
            inner,
            op,
            stats,
            scheme: scheme.to_string(),
        }
    }
}

impl<R: oio::Read> oio::Read for PrometheusMetricWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        self.inner.poll_read(cx, buf).map(|res| match res {
            Ok(bytes) => {
                self.stats.observe_bytes_total(&self.scheme, self.op, bytes);
                Ok(bytes)
            }
            Err(e) => {
                self.stats.increment_errors_total(self.op, e.kind());
                Err(e)
            }
        })
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        self.inner.poll_seek(cx, pos).map(|res| match res {
            Ok(n) => Ok(n),
            Err(e) => {
                self.stats.increment_errors_total(self.op, e.kind());
                Err(e)
            }
        })
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        self.inner.poll_next(cx).map(|res| match res {
            Some(Ok(bytes)) => {
                self.stats.observe_bytes_total(&self.scheme, self.op, bytes.len());
                Some(Ok(bytes))
            }
            Some(Err(e)) => {
                self.stats.increment_errors_total(self.op, e.kind());
                Some(Err(e))
            }
            None => None,
        })
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for PrometheusMetricWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner
            .read(buf)
            .map(|n| {
                self.stats.observe_bytes_total(&self.scheme, self.op, n);
                n
            })
            .map_err(|e| {
                self.stats.increment_errors_total(self.op, e.kind());
                e
            })
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos).map_err(|err| {
            self.stats.increment_errors_total(self.op, err.kind());
            err
        })
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        self.inner.next().map(|res| match res {
            Ok(bytes) => {
                self.stats.observe_bytes_total(&self.scheme, self.op, n);
                Ok(bytes)
            }
            Err(e) => {
                self.stats.increment_errors_total(self.op, e.kind());
                Err(e)
            }
        })
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for PrometheusMetricWrapper<R> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        self.inner
            .poll_write(cx, bs)
            .map_ok(|n| {
                self.stats.observe_bytes_total(&self.scheme, self.op, n);
                n
            })
            .map_err(|err| {
                self.stats.increment_errors_total(self.op, err.kind());
                err
            })
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_abort(cx).map_err(|err| {
            self.stats.increment_errors_total(self.op, err.kind());
            err
        })
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_close(cx).map_err(|err| {
            self.stats.increment_errors_total(self.op, err.kind());
            err
        })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for PrometheusMetricWrapper<R> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        self.inner
            .write(bs)
            .map(|n| {
                self.stats.observe_bytes_total(&self.scheme, self.op, n);
                n
            })
            .map_err(|err| {
                self.stats.increment_errors_total(self.op, err.kind());
                err
            })
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close().map_err(|err| {
            self.stats.increment_errors_total(self.op, err.kind());
            err
        })
    }
}
