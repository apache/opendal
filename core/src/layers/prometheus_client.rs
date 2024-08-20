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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Buf;
use futures::TryFutureExt;
use prometheus_client::encoding::EncodeLabel;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use crate::raw::Access;
use crate::raw::*;
use crate::*;

/// Add [prometheus-client](https://docs.rs/prometheus-client) for every operation.
///
/// # Examples
///
/// ```no_build
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
///         .layer(PrometheusClientLayer::new(&mut registry))
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
    metrics: PrometheusClientMetricDefinitions,
}

impl PrometheusClientLayer {
    /// Create PrometheusClientLayer while registering itself to this registry. Please keep in caution
    /// that do NOT call this method multiple times with a same registry. If you want to initialize multiple
    /// [`PrometheusClientLayer`] with a single registry, you should use [`Arc::clone`] instead.
    pub fn new(registry: &mut Registry) -> Self {
        let metrics = PrometheusClientMetricDefinitions::register(registry);
        Self { metrics }
    }
}

impl<A: Access> Layer<A> for PrometheusClientLayer {
    type LayeredAccess = PrometheusAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let meta = inner.info();
        let scheme = meta.scheme();
        let root = meta.root().to_string();
        let name = meta.name().to_string();

        let metrics =
            PrometheusClientMetrics::new(Arc::new(self.metrics.clone()), scheme, root, name);
        PrometheusAccessor {
            inner,
            metrics,
            scheme,
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct OperationLabels {
    op: &'static str,
    scheme: &'static str,
    root: Arc<String>,
    namespace: Arc<String>,
}

impl EncodeLabelSet for OperationLabels {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::LabelSetEncoder,
    ) -> std::result::Result<(), std::fmt::Error> {
        ("op", self.op).encode(encoder.encode_label())?;
        ("scheme", self.scheme).encode(encoder.encode_label())?;
        ("namespace", self.namespace.as_str()).encode(encoder.encode_label())?;
        ("root", self.root.as_str()).encode(encoder.encode_label())?;
        Ok(())
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct ErrorLabels {
    op: &'static str,
    scheme: &'static str,
    err: &'static str,
    root: Arc<String>,
    namespace: Arc<String>,
}

impl EncodeLabelSet for ErrorLabels {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::LabelSetEncoder,
    ) -> std::result::Result<(), std::fmt::Error> {
        ("op", self.op).encode(encoder.encode_label())?;
        ("scheme", self.scheme).encode(encoder.encode_label())?;
        ("error", self.err).encode(encoder.encode_label())?;
        ("namespace", self.namespace.as_str()).encode(encoder.encode_label())?;
        ("root", self.root.as_str()).encode(encoder.encode_label())?;
        Ok(())
    }
}

/// [`PrometheusClientMetricDefinitions`] provide the definition about RED(Rate/Error/Duration) metrics with the `prometheus-client` crate.
#[derive(Debug, Clone)]
struct PrometheusClientMetricDefinitions {
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

impl PrometheusClientMetricDefinitions {
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
}

#[derive(Clone, Debug)]
struct PrometheusClientMetrics {
    metrics: Arc<PrometheusClientMetricDefinitions>,
    scheme: Scheme,
    root: Arc<String>,
    name: Arc<String>,
}

impl PrometheusClientMetrics {
    fn new(
        metrics: Arc<PrometheusClientMetricDefinitions>,
        scheme: Scheme,
        root: String,
        name: String,
    ) -> Self {
        Self {
            metrics,
            scheme,
            root: Arc::new(root),
            name: Arc::new(name),
        }
    }

    fn increment_errors_total(&self, op: &'static str, err: ErrorKind) {
        let labels = ErrorLabels {
            op,
            scheme: self.scheme.into_static(),
            err: err.into_static(),
            root: self.root.clone(),
            namespace: self.name.clone(),
        };
        self.metrics.errors_total.get_or_create(&labels).inc();
    }

    fn increment_request_total(&self, scheme: Scheme, op: &'static str) {
        let labels = OperationLabels {
            op,
            scheme: scheme.into_static(),
            root: self.root.clone(),
            namespace: self.name.clone(),
        };
        self.metrics.requests_total.get_or_create(&labels).inc();
    }

    fn observe_bytes_total(&self, scheme: Scheme, op: &'static str, bytes: usize) {
        let labels = OperationLabels {
            op,
            scheme: scheme.into_static(),
            root: self.root.clone(),
            namespace: self.name.clone(),
        };
        self.metrics
            .bytes_histogram
            .get_or_create(&labels)
            .observe(bytes as f64);
        self.metrics
            .bytes_total
            .get_or_create(&labels)
            .inc_by(bytes as u64);
    }

    fn observe_request_duration(&self, scheme: Scheme, op: &'static str, duration: Duration) {
        let labels = OperationLabels {
            op,
            scheme: scheme.into_static(),
            root: self.root.clone(),
            namespace: self.name.clone(),
        };
        self.metrics
            .request_duration_seconds
            .get_or_create(&labels)
            .observe(duration.as_secs_f64());
    }
}

#[derive(Clone)]
pub struct PrometheusAccessor<A: Access> {
    inner: A,
    metrics: PrometheusClientMetrics,
    scheme: Scheme,
}

impl<A: Access> Debug for PrometheusAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access> LayeredAccess for PrometheusAccessor<A> {
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
            .increment_request_total(self.scheme, Operation::CreateDir.into_static());

        let start_time = Instant::now();
        let create_res = self.inner.create_dir(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::CreateDir.into_static(),
            start_time.elapsed(),
        );

        create_res.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::CreateDir.into_static(), e.kind());
            e
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Read.into_static());

        let start = Instant::now();
        let res = self.inner.read(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::Read.into_static(),
            start.elapsed(),
        );

        match res {
            Ok((rp, r)) => Ok((
                rp,
                PrometheusMetricWrapper::new(r, self.metrics.clone(), self.scheme),
            )),
            Err(err) => {
                self.metrics
                    .increment_errors_total(Operation::Read.into_static(), err.kind());
                Err(err)
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Write.into_static());

        let start = Instant::now();
        let res = self.inner.write(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::Write.into_static(),
            start.elapsed(),
        );

        match res {
            Ok((rp, w)) => Ok((
                rp,
                PrometheusMetricWrapper::new(w, self.metrics.clone(), self.scheme),
            )),
            Err(err) => {
                self.metrics
                    .increment_errors_total(Operation::Write.into_static(), err.kind());
                Err(err)
            }
        }
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Stat.into_static());

        let start_time = Instant::now();
        let stat_res = self
            .inner
            .stat(path, args)
            .inspect_err(|e| {
                self.metrics
                    .increment_errors_total(Operation::Stat.into_static(), e.kind());
            })
            .await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::Stat.into_static(),
            start_time.elapsed(),
        );

        stat_res.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::Stat.into_static(), e.kind());
            e
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Delete.into_static());

        let start_time = Instant::now();
        let delete_res = self.inner.delete(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::Delete.into_static(),
            start_time.elapsed(),
        );

        delete_res.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::Delete.into_static(), e.kind());
            e
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::List.into_static());

        let start_time = Instant::now();
        let list_res = self.inner.list(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::List.into_static(),
            start_time.elapsed(),
        );

        list_res.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::List.into_static(), e.kind());
            e
        })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Batch.into_static());

        let start_time = Instant::now();
        let result = self.inner.batch(args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::Batch.into_static(),
            start_time.elapsed(),
        );

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::Batch.into_static(), e.kind());
            e
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Presign.into_static());

        let start_time = Instant::now();
        let result = self.inner.presign(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::Presign.into_static(),
            start_time.elapsed(),
        );

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::Presign.into_static(), e.kind());
            e
        })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingCreateDir.into_static());

        let start_time = Instant::now();
        let result = self.inner.blocking_create_dir(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingCreateDir.into_static(),
            start_time.elapsed(),
        );

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::BlockingCreateDir.into_static(), e.kind());
            e
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingRead.into_static());

        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(r, self.metrics.clone(), self.scheme),
            )
        });

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::BlockingRead.into_static(), e.kind());
            e
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingWrite.into_static());

        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(r, self.metrics.clone(), self.scheme),
            )
        });

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::BlockingWrite.into_static(), e.kind());
            e
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingStat.into_static());

        let start_time = Instant::now();
        let result = self.inner.blocking_stat(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingStat.into_static(),
            start_time.elapsed(),
        );

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::BlockingStat.into_static(), e.kind());
            e
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingDelete.into_static());

        let start_time = Instant::now();
        let result = self.inner.blocking_delete(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingDelete.into_static(),
            start_time.elapsed(),
        );

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::BlockingDelete.into_static(), e.kind());
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingList.into_static());

        let start_time = Instant::now();
        let result = self.inner.blocking_list(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingList.into_static(),
            start_time.elapsed(),
        );

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(Operation::BlockingList.into_static(), e.kind());
            e
        })
    }
}

pub struct PrometheusMetricWrapper<R> {
    inner: R,

    metrics: PrometheusClientMetrics,
    scheme: Scheme,
}

impl<R> PrometheusMetricWrapper<R> {
    fn new(inner: R, metrics: PrometheusClientMetrics, scheme: Scheme) -> Self {
        Self {
            inner,
            metrics,
            scheme,
        }
    }
}

impl<R: oio::Read> oio::Read for PrometheusMetricWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let start = Instant::now();

        match self.inner.read().await {
            Ok(bs) => {
                self.metrics.observe_bytes_total(
                    self.scheme,
                    Operation::ReaderRead.into_static(),
                    bs.remaining(),
                );
                self.metrics.observe_request_duration(
                    self.scheme,
                    Operation::ReaderRead.into_static(),
                    start.elapsed(),
                );
                Ok(bs)
            }
            Err(e) => {
                self.metrics
                    .increment_errors_total(Operation::ReaderRead.into_static(), e.kind());
                Err(e)
            }
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for PrometheusMetricWrapper<R> {
    fn read(&mut self) -> Result<Buffer> {
        let start = Instant::now();
        self.inner
            .read()
            .map(|bs| {
                self.metrics.observe_bytes_total(
                    self.scheme,
                    Operation::BlockingReaderRead.into_static(),
                    bs.remaining(),
                );
                self.metrics.observe_request_duration(
                    self.scheme,
                    Operation::BlockingReaderRead.into_static(),
                    start.elapsed(),
                );
                bs
            })
            .map_err(|e| {
                self.metrics
                    .increment_errors_total(Operation::BlockingReaderRead.into_static(), e.kind());
                e
            })
    }
}

impl<R: oio::Write> oio::Write for PrometheusMetricWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let start = Instant::now();
        let size = bs.len();

        self.inner
            .write(bs)
            .await
            .map(|_| {
                self.metrics.observe_bytes_total(
                    self.scheme,
                    Operation::WriterWrite.into_static(),
                    size,
                );
                self.metrics.observe_request_duration(
                    self.scheme,
                    Operation::WriterWrite.into_static(),
                    start.elapsed(),
                );
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(Operation::WriterWrite.into_static(), err.kind());
                err
            })
    }

    async fn close(&mut self) -> Result<()> {
        let start = Instant::now();

        self.inner
            .close()
            .await
            .map(|_| {
                self.metrics.observe_request_duration(
                    self.scheme,
                    Operation::WriterClose.into_static(),
                    start.elapsed(),
                );
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(Operation::WriterClose.into_static(), err.kind());
                err
            })
    }

    async fn abort(&mut self) -> Result<()> {
        let start = Instant::now();

        self.inner
            .abort()
            .await
            .map(|_| {
                self.metrics.observe_request_duration(
                    self.scheme,
                    Operation::WriterAbort.into_static(),
                    start.elapsed(),
                );
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(Operation::WriterAbort.into_static(), err.kind());
                err
            })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for PrometheusMetricWrapper<R> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        let start = Instant::now();
        let size = bs.len();

        self.inner
            .write(bs)
            .map(|_| {
                self.metrics.observe_bytes_total(
                    self.scheme,
                    Operation::BlockingWriterWrite.into_static(),
                    size,
                );
                self.metrics.observe_request_duration(
                    self.scheme,
                    Operation::BlockingWriterWrite.into_static(),
                    start.elapsed(),
                );
            })
            .map_err(|err| {
                self.metrics.increment_errors_total(
                    Operation::BlockingWriterWrite.into_static(),
                    err.kind(),
                );
                err
            })
    }

    fn close(&mut self) -> Result<()> {
        let start = Instant::now();

        self.inner
            .close()
            .map(|_| {
                self.metrics.observe_request_duration(
                    self.scheme,
                    Operation::BlockingWriterClose.into_static(),
                    start.elapsed(),
                );
            })
            .map_err(|err| {
                self.metrics.increment_errors_total(
                    Operation::BlockingWriterClose.into_static(),
                    err.kind(),
                );
                err
            })
    }
}
