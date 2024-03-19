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

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::FutureExt;
use futures::TryFutureExt;
use log::debug;
use prometheus::core::AtomicU64;
use prometheus::core::GenericCounterVec;
use prometheus::exponential_buckets;
use prometheus::histogram_opts;
use prometheus::register_histogram_vec_with_registry;
use prometheus::register_int_counter_vec_with_registry;
use prometheus::HistogramVec;
use prometheus::Registry;

use crate::raw::Accessor;
use crate::raw::*;
use crate::*;

/// Add [prometheus](https://docs.rs/prometheus) for every operations.
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
    registry: Registry,
    requests_duration_seconds_buckets: Vec<f64>,
    bytes_total_buckets: Vec<f64>,
    path_label_level: usize,
}

impl PrometheusLayer {
    /// create PrometheusLayer by incoming registry.
    pub fn with_registry(registry: Registry) -> Self {
        Self {
            registry,
            requests_duration_seconds_buckets: exponential_buckets(0.01, 2.0, 16).unwrap(),
            bytes_total_buckets: exponential_buckets(0.01, 2.0, 16).unwrap(),
            path_label_level: 0,
        }
    }

    /// set buckets for requests_duration_seconds
    pub fn requests_duration_seconds_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.requests_duration_seconds_buckets = buckets;
        }
        self
    }

    /// set buckets for bytes_total
    pub fn bytes_total_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.bytes_total_buckets = buckets;
        }
        self
    }

    /// set path label level
    /// 0: no path label, the path label will be the ""
    /// >0: the path label will be the path split by "/" and get the last n level, like "/abc/def/ghi", if n=1, the path label will be "/abc"
    pub fn enable_path_label(mut self, level: usize) -> Self {
        self.path_label_level = level;
        self
    }
}

impl<A: Accessor> Layer<A> for PrometheusLayer {
    type LayeredAccessor = PrometheusAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.info();
        let scheme = meta.scheme();

        PrometheusAccessor {
            inner,
            stats: Arc::new(PrometheusMetrics::new(
                self.registry.clone(),
                self.requests_duration_seconds_buckets.clone(),
                self.bytes_total_buckets.clone(),
                self.path_label_level,
            )),
            scheme,
        }
    }
}

/// [`PrometheusMetrics`] provide the performance and IO metrics.
#[derive(Debug)]
pub struct PrometheusMetrics {
    /// Total times of the specific operation be called.
    pub requests_total: GenericCounterVec<AtomicU64>,
    /// Latency of the specific operation be called.
    pub requests_duration_seconds: HistogramVec,
    /// Size of the specific metrics.
    pub bytes_total: HistogramVec,
    /// The Path Level we will keep in the path label.
    pub path_label_level: usize,
}

impl PrometheusMetrics {
    /// new with prometheus register.
    pub fn new(
        registry: Registry,
        requests_duration_seconds_buckets: Vec<f64>,
        bytes_total_buckets: Vec<f64>,
        path_label_level: usize,
    ) -> Self {
        let labels = if path_label_level > 0 {
            vec!["scheme", "operation", "path"]
        } else {
            vec!["scheme", "operation"]
        };
        let requests_total = register_int_counter_vec_with_registry!(
            "requests_total",
            "Total times of create be called",
            &labels,
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "requests_duration_seconds",
            "Histogram of the time spent on specific operation",
            requests_duration_seconds_buckets
        );

        let requests_duration_seconds =
            register_histogram_vec_with_registry!(opts, &labels, registry).unwrap();

        let opts = histogram_opts!("bytes_total", "Total size of ", bytes_total_buckets);
        let bytes_total = register_histogram_vec_with_registry!(opts, &labels, registry).unwrap();

        Self {
            requests_total,
            requests_duration_seconds,
            bytes_total,
            path_label_level,
        }
    }

    /// error handling is the cold path, so we will not init error counters
    /// in advance.
    #[inline]
    fn increment_errors_total(&self, op: Operation, kind: ErrorKind) {
        debug!(
            "Prometheus statistics metrics error, operation {} error {}",
            op.into_static(),
            kind.into_static()
        );
    }

    /// generate metric label
    pub fn generate_metric_label<'a>(
        &self,
        scheme: &'a str,
        operation: &'a str,
        path_label: &'a str,
    ) -> Vec<&'a str> {
        match self.path_label_level {
            0 => {
                vec![scheme, operation]
            }
            n if n > 0 => {
                let path_value = get_path_label(path_label, self.path_label_level);
                vec![scheme, operation, path_value]
            }
            _ => {
                vec![scheme, operation]
            }
        }
    }
}

#[derive(Clone)]
pub struct PrometheusAccessor<A: Accessor> {
    inner: A,
    stats: Arc<PrometheusMetrics>,
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
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::CreateDir.into_static(),
            path,
        );

        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let create_res = self.inner.create_dir(path, args).await;

        timer.observe_duration();
        create_res.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::CreateDir, e.kind());
            e
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::Read.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();

        let read_res = self.inner.read(path, args).await.map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(
                    r,
                    Operation::Read,
                    self.stats.clone(),
                    self.scheme,
                    &path.to_string(),
                ),
            )
        });
        timer.observe_duration();
        read_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::Read, e.kind());
            e
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::Write.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();

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
                            self.scheme,
                            &path.to_string(),
                        ),
                    )
                })
            })
            .await;
        timer.observe_duration();
        write_res.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Write, e.kind());
            e
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::Stat.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();
        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();

        let stat_res = self
            .inner
            .stat(path, args)
            .inspect_err(|e| {
                self.stats.increment_errors_total(Operation::Stat, e.kind());
            })
            .await;
        timer.observe_duration();
        stat_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::Stat, e.kind());
            e
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::Delete.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();

        let delete_res = self.inner.delete(path, args).await;
        timer.observe_duration();
        delete_res.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Delete, e.kind());
            e
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::List.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();

        let list_res = self.inner.list(path, args).await;

        timer.observe_duration();
        list_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::List, e.kind());
            e
        })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::Batch.into_static(),
            "",
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let result = self.inner.batch(args).await;

        timer.observe_duration();
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Batch, e.kind());
            e
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::Presign.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let result = self.inner.presign(path, args).await;
        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Presign, e.kind());
            e
        })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::BlockingCreateDir.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let result = self.inner.blocking_create_dir(path, args);

        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingCreateDir, e.kind());
            e
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::BlockingRead.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(
                    r,
                    Operation::BlockingRead,
                    self.stats.clone(),
                    self.scheme,
                    &path.to_string(),
                ),
            )
        });
        timer.observe_duration();
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingRead, e.kind());
            e
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::BlockingWrite.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(
                    r,
                    Operation::BlockingWrite,
                    self.stats.clone(),
                    self.scheme,
                    &path.to_string(),
                ),
            )
        });
        timer.observe_duration();
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingWrite, e.kind());
            e
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::BlockingStat.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let result = self.inner.blocking_stat(path, args);
        timer.observe_duration();
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingStat, e.kind());
            e
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::BlockingDelete.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let result = self.inner.blocking_delete(path, args);
        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingDelete, e.kind());
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::BlockingList.into_static(),
            path,
        );
        self.stats.requests_total.with_label_values(&labels).inc();

        let timer = self
            .stats
            .requests_duration_seconds
            .with_label_values(&labels)
            .start_timer();
        let result = self.inner.blocking_list(path, args);
        timer.observe_duration();

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
    stats: Arc<PrometheusMetrics>,
    scheme: Scheme,
    path: String,
}

impl<R> PrometheusMetricWrapper<R> {
    fn new(
        inner: R,
        op: Operation,
        stats: Arc<PrometheusMetrics>,
        scheme: Scheme,
        path: &String,
    ) -> Self {
        Self {
            inner,
            op,
            stats,
            scheme,
            path: path.to_string(),
        }
    }
}

impl<R: oio::Read> oio::Read for PrometheusMetricWrapper<R> {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::Read.into_static(),
            &self.path,
        );
        match self.inner.read_at(offset, limit).await {
            Ok(bytes) => {
                self.stats
                    .bytes_total
                    .with_label_values(&labels)
                    .observe(bytes.remaining() as f64);
                Ok(bytes)
            }
            Err(e) => {
                self.stats.increment_errors_total(self.op, e.kind());
                Err(e)
            }
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for PrometheusMetricWrapper<R> {
    fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::BlockingRead.into_static(),
            &self.path,
        );
        self.inner
            .read_at(offset, limit)
            .map(|bs| {
                self.stats
                    .bytes_total
                    .with_label_values(&labels)
                    .observe(bs.remaining() as f64);
                bs
            })
            .map_err(|e| {
                self.stats.increment_errors_total(self.op, e.kind());
                e
            })
    }
}

impl<R: oio::Write> oio::Write for PrometheusMetricWrapper<R> {
    async fn write(&mut self, bs: Bytes) -> Result<usize> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::Write.into_static(),
            &self.path,
        );
        self.inner
            .write(bs)
            .await
            .map(|n| {
                self.stats
                    .bytes_total
                    .with_label_values(&labels)
                    .observe(n as f64);
                n
            })
            .map_err(|err| {
                self.stats.increment_errors_total(self.op, err.kind());
                err
            })
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await.map_err(|err| {
            self.stats.increment_errors_total(self.op, err.kind());
            err
        })
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await.map_err(|err| {
            self.stats.increment_errors_total(self.op, err.kind());
            err
        })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for PrometheusMetricWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<usize> {
        let labels = self.stats.generate_metric_label(
            self.scheme.into_static(),
            Operation::BlockingWrite.into_static(),
            &self.path,
        );
        self.inner
            .write(bs)
            .map(|n| {
                self.stats
                    .bytes_total
                    .with_label_values(&labels)
                    .observe(n as f64);
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
