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

use std::fmt;
use std::fmt::Debug;

use bytes::Buf;
use prometheus::core::AtomicU64;
use prometheus::core::GenericCounterVec;
use prometheus::exponential_buckets;
use prometheus::histogram_opts;
use prometheus::register_histogram_vec_with_registry;
use prometheus::register_int_counter_vec_with_registry;
use prometheus::HistogramTimer;
use prometheus::HistogramVec;
use prometheus::Registry;

use crate::layers::PrometheusClientLayer;
use crate::raw::Access;
use crate::raw::*;
use crate::*;

/// Add [prometheus](https://docs.rs/prometheus) for every operation.
///
/// # Prometheus Metrics
///
/// In this section, we will introduce three metrics that are currently being exported by our project. These metrics are essential for understanding the behavior and performance of our applications.
///
///
/// | Metric Name             | Type     | Description                                       | Labels              |
/// |-------------------------|----------|---------------------------------------------------|---------------------|
/// | requests_total          | Counter  | Total times of 'create' operation being called   | scheme, operation   |
/// | requests_duration_seconds | Histogram | Histogram of the time spent on specific operation | scheme, operation   |
/// | bytes_total             | Histogram | Total size                                        | scheme, operation   |
///
/// For a more detailed explanation of these metrics and how they are used, please refer to the [Prometheus documentation](https://prometheus.io/docs/introduction/overview/).
///
/// # Histogram Configuration
///
/// The metric buckets for these histograms are automatically generated based on the `exponential_buckets(0.01, 2.0, 16)` configuration.
///
/// # Examples
///
/// ```no_build
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
///         .layer(PrometheusLayer::new(registry.clone()))
///         .finish();
///     debug!("operator: {op:?}");
///
///     // Write data into object test.
///     op.write("test", "Hello, World!").await?;
///     // Read data from object.
///     let bs = op.read("test").await?;
///     info!("content: {}", String::from_utf8_lossy(&bs.to_bytes()));
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
#[derive(Clone, Debug)]
pub struct PrometheusLayer {
    metrics: PrometheusMetricDefinitions,
    /// The path level we will keep in the path label.
    path_label_level: usize,
}

impl PrometheusLayer {
    /// Create a [`PrometheusLayer`] while registering itself to this registry.
    pub fn new(registry: Registry) -> Self {
        Self::builder().build(registry)
    }

    /// Create a [`PrometheusLayerBuilder`].
    pub fn builder() -> PrometheusLayerBuilder {
        PrometheusLayerBuilder::default()
    }
}

/// [`PrometheusLayerBuilder`] is a config builder to build a [`PrometheusLayer`].
pub struct PrometheusLayerBuilder {
    requests_duration_seconds_buckets: Vec<f64>,
    bytes_buckets: Vec<f64>,
    /// The level we will keep in the path label.
    path_label_level: usize,
}

impl Default for PrometheusLayerBuilder {
    fn default() -> Self {
        Self {
            requests_duration_seconds_buckets: exponential_buckets(0.01, 2.0, 16).unwrap(),
            bytes_buckets: exponential_buckets(1.0, 2.0, 16).unwrap(),
            path_label_level: 0,
        }
    }
}

impl PrometheusLayerBuilder {
    /// Set buckets for `requests_duration_seconds` histogram.
    pub fn requests_duration_seconds_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.requests_duration_seconds_buckets = buckets;
        }
        self
    }

    /// Set buckets for `bytes` histogram.
    pub fn bytes_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.bytes_buckets = buckets;
        }
        self
    }

    /// Set the level of path label.
    ///
    /// - level = 0: no path label, the path label will be the "".
    /// - level > 0: the path label will be the path split by "/" and get the last n level,
    ///   like "/abc/def/ghi", if n=1, the path label will be "/abc".
    pub fn enable_path_label(mut self, level: usize) -> Self {
        self.path_label_level = level;
        self
    }

    /// Register the metrics and build the [`PrometheusLayer`].
    pub fn build(self, registry: Registry) -> PrometheusLayer {
        let metrics = PrometheusMetricDefinitions::register(
            registry,
            self.requests_duration_seconds_buckets,
            self.bytes_buckets,
            self.path_label_level,
        );

        PrometheusLayer {
            metrics,
            path_label_level: self.path_label_level,
        }
    }
}

impl<A: Access> Layer<A> for PrometheusLayer {
    type LayeredAccess = PrometheusAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let meta = inner.info();
        let scheme = meta.scheme();
        let root = meta.root().to_string();
        let name = meta.name().to_string();

        let metrics = self.metrics.clone();
        let metrics = PrometheusMetrics::new(metrics, scheme, root, name, self.path_label_level);

        PrometheusAccessor { inner, metrics }
    }
}

/// [`PrometheusMetricDefinitions`] provide the definition about RED(Rate/Error/Duration) metrics with the `prometheus` crate.
#[derive(Clone, Debug)]
struct PrometheusMetricDefinitions {
    /// Total times of the specific operation be called.
    requests_total: GenericCounterVec<AtomicU64>,
    /// Total times of the specific operation be called but meet error.
    errors_total: GenericCounterVec<AtomicU64>,
    /// Latency of the specific operation be called.
    requests_duration_seconds: HistogramVec,

    /// Total size of bytes.
    bytes_total: GenericCounterVec<AtomicU64>,
    /// The size of bytes.
    bytes: HistogramVec,
}

impl PrometheusMetricDefinitions {
    /// new with prometheus register.
    pub fn register(
        registry: Registry,
        requests_duration_seconds_buckets: Vec<f64>,
        bytes_buckets: Vec<f64>,
        path_label_level: usize,
    ) -> Self {
        let (labels, labels_with_error) = if path_label_level > 0 {
            (
                vec!["op", "scheme", "root", "namespace", "path"],
                vec!["op", "scheme", "root", "namespace", "error", "path"],
            )
        } else {
            (
                vec!["op", "scheme", "root", "namespace"],
                vec!["op", "scheme", "root", "namespace", "error"],
            )
        };

        let requests_total = register_int_counter_vec_with_registry!(
            "opendal_requests_total",
            "Total times of the specific operation be called",
            &labels,
            registry
        )
            .unwrap();
        let errors_total = register_int_counter_vec_with_registry!(
            "opendal_errors_total",
            "Total times of the specific operation be called but meet error",
            &labels_with_error,
            registry
        )
            .unwrap();

        let requests_duration_seconds = register_histogram_vec_with_registry!(
            histogram_opts!(
                "opendal_requests_duration_seconds",
                "Latency of the specific operation be called",
                requests_duration_seconds_buckets
            ),
            &labels,
            registry
        )
            .unwrap();

        let bytes_total = register_int_counter_vec_with_registry!(
            "opendal_bytes_total",
            "Total size of bytes",
            &labels,
            registry
        )
            .unwrap();

        let bytes = register_histogram_vec_with_registry!(
            histogram_opts!("opendal_bytes", "The size of bytes", bytes_buckets),
            &labels,
            registry
        )
            .unwrap();

        Self {
            requests_total,
            errors_total,
            requests_duration_seconds,
            bytes_total,
            bytes,
        }
    }
}

#[derive(Clone, Debug)]
struct PrometheusMetrics {
    metrics: PrometheusMetricDefinitions,
    scheme: Scheme,
    root: String,
    name: String,
    path_label_level: usize,
}

impl PrometheusMetrics {
    fn new(
        metrics: PrometheusMetricDefinitions,
        scheme: Scheme,
        root: String,
        name: String,
        path_label_level: usize,
    ) -> Self {
        Self {
            metrics,
            scheme,
            root,
            name,
            path_label_level,
        }
    }

    fn increment_request_total(&self, labels: &[&str]) {
        self.metrics.requests_total.with_label_values(labels).inc();
    }

    fn increment_errors_total(&self, op: Operation, err: ErrorKind, path: &str) {
        let labels = self.gen_error_labels(op, err, path);
        self.metrics.errors_total.with_label_values(&labels).inc();
    }

    fn requests_duration_seconds_timer(&self, labels: &[&str]) -> HistogramTimer {
        self.metrics
            .requests_duration_seconds
            .with_label_values(labels)
            .start_timer()
    }

    fn observe_bytes_total(&self, labels: &[&str], bytes: usize) {
        self.metrics
            .bytes_total
            .with_label_values(labels)
            .inc_by(bytes as u64);
        self.metrics
            .bytes
            .with_label_values(labels)
            .observe(bytes as f64);
    }

    // labels: `["op", "scheme", "root", "namespace", "path"]`
    //      or `["op", "scheme", "root", "namespace"]`
    fn gen_operation_labels<'a>(&'a self, op: Operation, path: &'a str) -> Vec<&'a str> {
        let mut labels = vec![
            op.into_static(),
            self.scheme.into_static(),
            &self.root,
            &self.name,
        ];

        if self.path_label_level > 0 {
            let path_value = get_path_label(path, self.path_label_level);
            labels.push(path_value);
        }

        labels
    }

    // labels: `["op", "scheme", "root", "namespace", "error", "path"]`
    //      or `["op", "scheme", "root", "namespace", "error"]`
    fn gen_error_labels<'a>(
        &'a self,
        op: Operation,
        error: ErrorKind,
        path: &'a str,
    ) -> Vec<&'a str> {
        let mut labels = vec![
            op.into_static(),
            self.scheme.into_static(),
            &self.root,
            &self.name,
            error.into_static(),
        ];

        if self.path_label_level > 0 {
            let path_value = get_path_label(path, self.path_label_level);
            labels.push(path_value);
        }

        labels
    }
}

#[derive(Clone)]
pub struct PrometheusAccessor<A: Access> {
    inner: A,
    metrics: PrometheusMetrics,
}

impl<A: Access> Debug for PrometheusAccessor<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        let op = Operation::CreateDir;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.create_dir(path, args).await;
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let op = Operation::Read;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.read(path, args).await;
        timer.observe_duration();

        match result {
            Ok((rp, r)) => Ok((
                rp,
                PrometheusMetricWrapper::new(r, self.metrics.clone(), path),
            )),
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let op = Operation::Write;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.write(path, args).await;
        timer.observe_duration();

        match result {
            Ok((rp, w)) => Ok((
                rp,
                PrometheusMetricWrapper::new(w, self.metrics.clone(), path),
            )),
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let op = Operation::Stat;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.stat(path, args).await;
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let op = Operation::Delete;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.delete(path, args).await;
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let op = Operation::List;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.list(path, args).await;
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let op = Operation::Batch;
        let labels = self.metrics.gen_operation_labels(op, "");

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.batch(args).await;
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), "");
            err
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let op = Operation::Presign;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.presign(path, args).await;
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let op = Operation::BlockingCreateDir;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.blocking_create_dir(path, args);
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let op = Operation::BlockingRead;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(r, self.metrics.clone(), path),
            )
        });
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let op = Operation::BlockingWrite;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(r, self.metrics.clone(), path),
            )
        });
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let op = Operation::BlockingStat;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.blocking_stat(path, args);
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let op = Operation::BlockingDelete;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.blocking_delete(path, args);
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let op = Operation::BlockingList;
        let labels = self.metrics.gen_operation_labels(op, path);

        self.metrics.increment_request_total(&labels);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.blocking_list(path, args);
        timer.observe_duration();

        result.map_err(|err| {
            self.metrics.increment_errors_total(op, err.kind(), path);
            err
        })
    }
}

pub struct PrometheusMetricWrapper<R> {
    inner: R,
    metrics: PrometheusMetrics,
    path: String,
}

impl<R> PrometheusMetricWrapper<R> {
    fn new(inner: R, metrics: PrometheusMetrics, path: impl Into<String>) -> Self {
        Self {
            inner,
            metrics,
            path: path.into(),
        }
    }
}

impl<R: oio::Read> oio::Read for PrometheusMetricWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let op = Operation::ReaderRead;
        let path = &self.path;
        let labels = self.metrics.gen_operation_labels(op, path);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.read().await;
        timer.observe_duration();

        match result {
            Ok(bs) => {
                self.metrics.observe_bytes_total(&labels, bs.remaining());
                Ok(bs)
            }
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for PrometheusMetricWrapper<R> {
    fn read(&mut self) -> Result<Buffer> {
        let op = Operation::BlockingReaderRead;
        let path = &self.path;
        let labels = self.metrics.gen_operation_labels(op, path);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.read();
        timer.observe_duration();

        match result {
            Ok(bs) => {
                self.metrics.observe_bytes_total(&labels, bs.remaining());
                Ok(bs)
            }
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }
}

impl<R: oio::Write> oio::Write for PrometheusMetricWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len();

        let op = Operation::WriterWrite;
        let path = &self.path;
        let labels = self.metrics.gen_operation_labels(op, path);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.write(bs).await;
        timer.observe_duration();

        match result {
            Ok(_) => {
                self.metrics.observe_bytes_total(&labels, size);
                Ok(())
            }
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        let op = Operation::WriterClose;
        let path = &self.path;
        let labels = self.metrics.gen_operation_labels(op, path);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.close().await;
        timer.observe_duration();

        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        let op = Operation::WriterAbort;
        let path = &self.path;
        let labels = self.metrics.gen_operation_labels(op, path);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.abort().await;
        timer.observe_duration();

        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for PrometheusMetricWrapper<R> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len();

        let op = Operation::BlockingWriterWrite;
        let path = &self.path;
        let labels = self.metrics.gen_operation_labels(op, path);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.write(bs);
        timer.observe_duration();

        match result {
            Ok(_) => {
                self.metrics.observe_bytes_total(&labels, size);
                Ok(())
            }
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        let op = Operation::BlockingWriterClose;
        let path = &self.path;
        let labels = self.metrics.gen_operation_labels(op, path);

        let timer = self.metrics.requests_duration_seconds_timer(&labels);
        let result = self.inner.close();
        timer.observe_duration();

        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.increment_errors_total(op, err.kind(), path);
                Err(err)
            }
        }
    }
}

fn get_path_label(path: &str, path_level: usize) -> &str {
    if path_level > 0 {
        return path
            .char_indices()
            .filter(|&(_, c)| c == '/')
            .nth(path_level - 1)
            .map_or(path, |(i, _)| &path[..i]);
    }
    ""
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_path_label() {
        let path = "abc/def/ghi";
        assert_eq!(get_path_label(path, 0), "");
        assert_eq!(get_path_label(path, 1), "abc");
        assert_eq!(get_path_label(path, 2), "abc/def");
        assert_eq!(get_path_label(path, 3), "abc/def/ghi");
        assert_eq!(get_path_label(path, usize::MAX), "abc/def/ghi");

        assert_eq!(get_path_label("", 0), "");
    }
}
