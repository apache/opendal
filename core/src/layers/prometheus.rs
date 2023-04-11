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

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use futures::TryFutureExt;
use log::debug;

use crate::ops::*;
use crate::raw::*;
use crate::*;

use prometheus::{
    core::{AtomicU64, GenericCounter},
    register_int_counter_with_registry, Histogram,
};
use prometheus::{exponential_buckets, histogram_opts, register_histogram_with_registry, Registry};

use crate::raw::Accessor;
/// Add [prometheus](https://docs.rs/prometheus) for every operations.
///
/// # Examples
///
/// ```
/// use log::debug;
/// use log::info;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Result;
///
/// use opendal::layers::PrometheusLayer;
/// use prometheus::Encoder;
///
/// /// Visit [`opendal::services`] for more service related config.
/// /// Visit [`opendal::Object`] for more object level APIs.
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Pick a builder and configure it.
///     let builder = services::Memory::default();
///
///     let op = Operator::new(builder)
///         .expect("must init")
///         .layer(PrometheusLayer)
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
#[derive(Debug, Copy, Clone)]
pub struct PrometheusLayer;

impl<A: Accessor> Layer<A> for PrometheusLayer {
    type LayeredAccessor = PrometheusMetricsAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let registry = prometheus::default_registry();

        PrometheusMetricsAccessor {
            inner,
            stats: Arc::new(PrometheusMetrics::new(registry.clone())),
        }
    }
}
/// [`PrometheusMetrics`] provide the performance and IO metrics.
///
#[derive(Debug)]
pub struct PrometheusMetrics {
    // metadata
    pub metadata_request_counts: GenericCounter<AtomicU64>,
    pub metadata_request_latency: Histogram,

    // create
    pub create_request_counts: GenericCounter<AtomicU64>,
    pub create_request_latency: Histogram,

    /// read
    pub read_request_counts: GenericCounter<AtomicU64>,
    pub read_request_latency: Histogram,
    pub read_size: Histogram,

    // write
    pub write_request_counts: GenericCounter<AtomicU64>,
    pub write_request_latency: Histogram,
    pub write_size: Histogram,

    // stat
    pub stat_request_counts: GenericCounter<AtomicU64>,
    pub stat_request_latency: Histogram,

    // delete
    pub delete_request_counts: GenericCounter<AtomicU64>,
    pub delete_request_latency: Histogram,

    // list
    pub list_request_counts: GenericCounter<AtomicU64>,
    pub list_request_latency: Histogram,

    // scan
    pub scan_request_counts: GenericCounter<AtomicU64>,
    pub scan_request_latency: Histogram,

    // presign
    pub presign_request_counts: GenericCounter<AtomicU64>,
    pub presign_request_latency: Histogram,

    // batch
    pub batch_request_counts: GenericCounter<AtomicU64>,
    pub batch_request_latency: Histogram,

    // blocking create
    pub blocking_create_request_counts: GenericCounter<AtomicU64>,
    pub blocking_create_request_latency: Histogram,

    // blocking read
    pub blocking_read_request_counts: GenericCounter<AtomicU64>,
    pub blocking_read_request_latency: Histogram,
    pub blocking_read_size: Histogram,

    // blocking write
    pub blocking_write_request_counts: GenericCounter<AtomicU64>,
    pub blocking_write_request_latency: Histogram,
    pub blocking_write_size: Histogram,

    // blocking stat
    pub blocking_stat_request_counts: GenericCounter<AtomicU64>,
    pub blocking_stat_request_latency: Histogram,

    // blocking delete
    pub blocking_delete_request_counts: GenericCounter<AtomicU64>,
    pub blocking_delete_request_latency: Histogram,

    // blocking list
    pub blocking_list_request_counts: GenericCounter<AtomicU64>,
    pub blocking_list_request_latency: Histogram,

    // blocking scan
    pub blocking_scan_request_counts: GenericCounter<AtomicU64>,
    pub blocking_scan_request_latency: Histogram,
}

impl PrometheusMetrics {
    /// new with prometheus register.
    pub fn new(registry: Registry) -> Self {
        // metadata
        let metadata_request_counts = register_int_counter_with_registry!(
            "metadata_request_counts",
            "Total times of metadata be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "metadata_request_latency",
            "Histogram of the time spent on getting metadata",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let metadata_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // create
        let create_request_counts = register_int_counter_with_registry!(
            "create_request_counts",
            "Total times of create be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "create_request_latency",
            "Histogram of the time spent on creating",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let create_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // read
        let read_request_counts = register_int_counter_with_registry!(
            "read_request_counts",
            "Total times of read be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "read_request_latency",
            "Histogram of the time spent on reading",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let read_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "read_size",
            "read size",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let read_size = register_histogram_with_registry!(opts, registry).unwrap();

        // write
        let write_request_counts = register_int_counter_with_registry!(
            "write_request_counts",
            "Total times of write be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "write_request_latency",
            "Histogram of the time spent on writing",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let write_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "write_size",
            "write size",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let write_size = register_histogram_with_registry!(opts, registry).unwrap();

        // stat
        let stat_request_counts = register_int_counter_with_registry!(
            "stat_request_counts",
            "Total times of stat be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "stat_request_latency",
            "stat letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let stat_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // delete
        let delete_request_counts = register_int_counter_with_registry!(
            "delete_request_counts",
            "Total times of delete be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "delete_request_latency",
            "delete letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let delete_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // list
        let list_request_counts = register_int_counter_with_registry!(
            "list_request_counts",
            "Total times of list be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "list_request_latency",
            "list letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let list_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // scan
        let scan_request_counts = register_int_counter_with_registry!(
            "scan_request_counts",
            "Total times of scan be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "scan_request_latency",
            "scan letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let scan_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // presign
        let presign_request_counts = register_int_counter_with_registry!(
            "presign_request_counts",
            "Total times of presign be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "presign_request_latency",
            "presign letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let presign_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // batch
        let batch_request_counts = register_int_counter_with_registry!(
            "batch_request_counts",
            "Total times of batch be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "batch_request_latency",
            "batch letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let batch_request_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_create
        let blocking_create_request_counts = register_int_counter_with_registry!(
            "blocking_create_request_counts",
            "Total times of blocking_create be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "blocking_create_request_latency",
            "blocking create letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let blocking_create_request_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_read
        let blocking_read_request_counts = register_int_counter_with_registry!(
            "blocking_read_request_counts",
            "Total times of blocking_read be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "blocking_read_request_latency",
            "Histogram of the time spent on blocking_reading",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let blocking_read_request_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "blocking_read_size",
            "blocking_read size",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let blocking_read_size = register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_write
        let blocking_write_request_counts = register_int_counter_with_registry!(
            "blocking_write_request_counts",
            "Total times of blocking_write be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "blocking_write_request_latency",
            "Histogram of the time spent on blocking_writing",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let blocking_write_request_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "blocking_write_size",
            "total size by blocking_write",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let blocking_write_size = register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_stat
        let blocking_stat_request_counts = register_int_counter_with_registry!(
            "blocking_stat_request_counts",
            "Total times of blocking_stat be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "blocking_stat_request_latency",
            "blocking_stat letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let blocking_stat_request_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_delete
        let blocking_delete_request_counts = register_int_counter_with_registry!(
            "blocking_delete_request_counts",
            "Total times of blocking_delete be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "blocking_delete_request_latency",
            "blocking_delete letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let blocking_delete_request_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_list
        let blocking_list_request_counts = register_int_counter_with_registry!(
            "blocking_list_request_counts",
            "Total times of blocking_list be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "blocking_list_request_latency",
            "blocking_list letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let blocking_list_request_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_scan
        let blocking_scan_request_counts = register_int_counter_with_registry!(
            "blocking_scan_request_counts",
            "Total times of blocking_scan be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "blocking_scan_request_latency",
            "blocking_scan letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let blocking_scan_request_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            metadata_request_counts,
            metadata_request_latency,

            create_request_counts,
            create_request_latency,

            read_request_counts,
            read_request_latency,
            read_size,

            write_request_counts,
            write_request_latency,
            write_size,

            stat_request_counts,
            stat_request_latency,

            delete_request_counts,
            delete_request_latency,

            list_request_counts,
            list_request_latency,

            scan_request_counts,
            scan_request_latency,

            presign_request_counts,
            presign_request_latency,

            batch_request_counts,
            batch_request_latency,

            blocking_create_request_counts,
            blocking_create_request_latency,

            blocking_read_request_counts,
            blocking_read_request_latency,
            blocking_read_size,

            blocking_write_request_counts,
            blocking_write_request_latency,
            blocking_write_size,

            blocking_stat_request_counts,
            blocking_stat_request_latency,

            blocking_delete_request_counts,
            blocking_delete_request_latency,

            blocking_list_request_counts,
            blocking_list_request_latency,

            blocking_scan_request_counts,
            blocking_scan_request_latency,
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
}

#[derive(Clone)]
pub struct PrometheusMetricsAccessor<A: Accessor> {
    inner: A,
    stats: Arc<PrometheusMetrics>,
}

impl<A: Accessor> Debug for PrometheusMetricsAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusMetricsAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for PrometheusMetricsAccessor<A> {
    type Inner = A;
    type Reader = MetricWrapper<A::Reader>;
    type BlockingReader = MetricWrapper<A::BlockingReader>;
    type Writer = MetricWrapper<A::Writer>;
    type BlockingWriter = MetricWrapper<A::BlockingWriter>;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        self.stats.metadata_request_counts.inc();

        let timer = self.stats.metadata_request_latency.start_timer();
        let result = self.inner.info();
        timer.observe_duration();

        result
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.stats.create_request_counts.inc();

        let timer = self.stats.create_request_latency.start_timer();
        let create_res = self.inner.create(path, args).await;

        timer.observe_duration();
        create_res
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.stats.read_request_counts.inc();

        let timer = self.stats.read_request_latency.start_timer();

        let read_res = self
            .inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    self.stats
                        .read_size
                        .observe(rp.metadata().content_length() as f64);
                    (
                        rp,
                        MetricWrapper::new(r, Operation::Read, self.stats.clone()),
                    )
                })
            })
            .await;
        timer.observe_duration();
        read_res
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.stats.write_request_counts.inc();

        let timer = self.stats.write_request_latency.start_timer();

        let write_res = self
            .inner
            .write(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        MetricWrapper::new(r, Operation::Write, self.stats.clone()),
                    )
                })
            })
            .await;
        timer.observe_duration();
        write_res
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.stats.stat_request_counts.inc();
        let timer = self.stats.stat_request_latency.start_timer();

        let stat_res = self
            .inner
            .stat(path, args)
            .inspect_err(|e| {
                self.stats.increment_errors_total(Operation::Stat, e.kind());
            })
            .await;
        timer.observe_duration();
        stat_res
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.stats.delete_request_counts.inc();

        let timer = self.stats.delete_request_latency.start_timer();

        let delete_res = self.inner.delete(path, args).await;
        timer.observe_duration();
        delete_res
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.stats.list_request_counts.inc();

        let timer = self.stats.list_request_latency.start_timer();

        let list_res = self.inner.list(path, args).await;

        timer.observe_duration();
        list_res
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        self.stats.scan_request_counts.inc();

        let timer = self.stats.scan_request_latency.start_timer();

        let scan_res = self.inner.scan(path, args).await;
        timer.observe_duration();
        scan_res
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.stats.batch_request_counts.inc();

        let timer = self.stats.batch_request_latency.start_timer();
        let result = self.inner.batch(args).await;

        timer.observe_duration();
        result
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.stats.presign_request_counts.inc();

        let timer = self.stats.presign_request_latency.start_timer();
        let result = self.inner.presign(path, args);
        timer.observe_duration();

        result
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.stats.blocking_create_request_counts.inc();

        let timer = self.stats.blocking_create_request_latency.start_timer();
        let result = self.inner.blocking_create(path, args);

        timer.observe_duration();

        result
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.stats.blocking_read_request_counts.inc();

        let timer = self.stats.blocking_read_request_latency.start_timer();
        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            self.stats
                .read_size
                .observe(rp.metadata().content_length() as f64);
            (
                rp,
                MetricWrapper::new(r, Operation::BlockingRead, self.stats.clone()),
            )
        });
        timer.observe_duration();
        result
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.stats.blocking_write_request_counts.inc();

        let timer = self.stats.blocking_write_request_latency.start_timer();
        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            // todo: monitor blocking write size
            (
                rp,
                MetricWrapper::new(r, Operation::BlockingWrite, self.stats.clone()),
            )
        });
        timer.observe_duration();
        result
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.stats.blocking_stat_request_counts.inc();

        let timer = self.stats.blocking_stat_request_latency.start_timer();
        let result = self.inner.blocking_stat(path, args);
        timer.observe_duration();
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingStat, e.kind());
            e
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.stats.blocking_delete_request_counts.inc();

        let timer = self.stats.blocking_delete_request_latency.start_timer();
        let result = self.inner.blocking_delete(path, args);
        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingDelete, e.kind());
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.stats.blocking_list_request_counts.inc();

        let timer = self.stats.blocking_list_request_latency.start_timer();
        let result = self.inner.blocking_list(path, args);
        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingList, e.kind());
            e
        })
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.stats.blocking_scan_request_counts.inc();

        let timer = self.stats.blocking_scan_request_latency.start_timer();
        let result = self.inner.blocking_scan(path, args);
        timer.observe_duration();
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingScan, e.kind());
            e
        })
    }
}

/// todo: add doc
pub struct MetricWrapper<R> {
    inner: R,

    op: Operation,
    stats: Arc<PrometheusMetrics>,
}

impl<R> MetricWrapper<R> {
    fn new(inner: R, op: Operation, stats: Arc<PrometheusMetrics>) -> Self {
        Self { inner, op, stats }
    }
}

impl<R: oio::Read> oio::Read for MetricWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        self.inner.poll_read(cx, buf).map(|res| match res {
            Ok(bytes) => {
                self.stats.read_size.observe(bytes as f64);
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
                self.stats.read_size.observe(bytes.len() as f64);
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

impl<R: oio::BlockingRead> oio::BlockingRead for MetricWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner
            .read(buf)
            .map(|n| {
                self.stats.blocking_read_size.observe(n as f64);
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
                self.stats.blocking_read_size.observe(bytes.len() as f64);
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
impl<R: oio::Write> oio::Write for MetricWrapper<R> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        self.inner
            .write(bs)
            .await
            .map(|_| self.stats.write_size.observe(size as f64))
            .map_err(|err| {
                self.stats.increment_errors_total(self.op, err.kind());
                err
            })
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        self.inner
            .append(bs)
            .await
            .map(|_| self.stats.write_size.observe(size as f64))
            .map_err(|err| {
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

impl<R: oio::BlockingWrite> oio::BlockingWrite for MetricWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        self.inner
            .write(bs)
            .map(|_| self.stats.blocking_write_size.observe(size as f64))
            .map_err(|err| {
                self.stats.increment_errors_total(self.op, err.kind());
                err
            })
    }

    fn append(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        self.inner
            .append(bs)
            .map(|_| self.stats.blocking_write_size.observe(size as f64))
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
