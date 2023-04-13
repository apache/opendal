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

use prometheus::core::AtomicU64;
use prometheus::core::GenericCounter;
use prometheus::exponential_buckets;
use prometheus::histogram_opts;
use prometheus::register_histogram_with_registry;
use prometheus::register_int_counter_with_registry;
use prometheus::Histogram;
use prometheus::Registry;

use crate::ops::*;
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
#[derive(Debug)]
pub struct PrometheusMetrics {
    // metadata
    pub opendal_requests_total_metadata: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_metadata: Histogram,

    // create
    pub opendal_requests_total_create: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_create: Histogram,

    /// read
    pub opendal_requests_total_read: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_read: Histogram,
    pub opendal_bytes_total_read: Histogram,

    // write
    pub opendal_requests_total_write: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_write: Histogram,
    pub opendal_bytes_total_write: Histogram,

    // stat
    pub opendal_requests_total_stat: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_stat: Histogram,

    // delete
    pub opendal_requests_total_delete: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_delete: Histogram,

    // list
    pub opendal_requests_total_list: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_list: Histogram,

    // scan
    pub opendal_requests_total_scan: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_scan: Histogram,

    // presign
    pub opendal_requests_total_presign: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_presign: Histogram,

    // batch
    pub opendal_requests_total_batch: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_batch: Histogram,

    // blocking create
    pub opendal_requests_total_blocking_create: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_blocking_create: Histogram,

    // blocking read
    pub opendal_requests_total_blocking_read: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_blocking_read: Histogram,
    pub opendal_bytes_total_blocking_read: Histogram,

    // blocking write
    pub opendal_requests_total_blocking_write: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_blocking_write: Histogram,
    pub opendal_bytes_total_blocking_write: Histogram,

    // blocking stat
    pub opendal_requests_total_blocking_stat: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_blocking_stat: Histogram,

    // blocking delete
    pub opendal_requests_total_blocking_delete: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_blocking_delete: Histogram,

    // blocking list
    pub opendal_requests_total_blocking_list: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_blocking_list: Histogram,

    // blocking scan
    pub opendal_requests_total_blocking_scan: GenericCounter<AtomicU64>,
    pub opendal_requests_duration_seconds_blocking_scan: Histogram,
}

impl PrometheusMetrics {
    /// new with prometheus register.
    pub fn new(registry: Registry) -> Self {
        // metadata
        let opendal_requests_total_metadata = register_int_counter_with_registry!(
            "opendal_requests_total_metadata",
            "Total times of metadata be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_metadata",
            "Histogram of the time spent on getting metadata",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_metadata =
            register_histogram_with_registry!(opts, registry).unwrap();

        // create
        let opendal_requests_total_create = register_int_counter_with_registry!(
            "opendal_requests_total_create",
            "Total times of create be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_create",
            "Histogram of the time spent on creating",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_create =
            register_histogram_with_registry!(opts, registry).unwrap();

        // read
        let opendal_requests_total_read = register_int_counter_with_registry!(
            "opendal_requests_total_read",
            "Total times of read be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_read",
            "Histogram of the time spent on reading",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let opendal_requests_duration_seconds_read =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "opendal_bytes_total_read",
            "read size",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let opendal_bytes_total_read = register_histogram_with_registry!(opts, registry).unwrap();

        // write
        let opendal_requests_total_write = register_int_counter_with_registry!(
            "opendal_requests_total_write",
            "Total times of write be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_write",
            "Histogram of the time spent on writing",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let opendal_requests_duration_seconds_write =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "opendal_bytes_total_write",
            "write size",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let opendal_bytes_total_write = register_histogram_with_registry!(opts, registry).unwrap();

        // stat
        let opendal_requests_total_stat = register_int_counter_with_registry!(
            "opendal_requests_total_stat",
            "Total times of stat be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_stat",
            "stat letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_stat =
            register_histogram_with_registry!(opts, registry).unwrap();

        // delete
        let opendal_requests_total_delete = register_int_counter_with_registry!(
            "opendal_requests_total_delete",
            "Total times of delete be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_delete",
            "delete letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_delete =
            register_histogram_with_registry!(opts, registry).unwrap();

        // list
        let opendal_requests_total_list = register_int_counter_with_registry!(
            "opendal_requests_total_list",
            "Total times of list be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_list",
            "list letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_list =
            register_histogram_with_registry!(opts, registry).unwrap();

        // scan
        let opendal_requests_total_scan = register_int_counter_with_registry!(
            "opendal_requests_total_scan",
            "Total times of scan be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_scan",
            "scan letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_scan =
            register_histogram_with_registry!(opts, registry).unwrap();

        // presign
        let opendal_requests_total_presign = register_int_counter_with_registry!(
            "opendal_requests_total_presign",
            "Total times of presign be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_presign",
            "presign letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_presign =
            register_histogram_with_registry!(opts, registry).unwrap();

        // batch
        let opendal_requests_total_batch = register_int_counter_with_registry!(
            "opendal_requests_total_batch",
            "Total times of batch be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_batch",
            "batch letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_batch =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_create
        let opendal_requests_total_blocking_create = register_int_counter_with_registry!(
            "opendal_requests_total_blocking_create",
            "Total times of blocking_create be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_blocking_create",
            "blocking create letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_blocking_create =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_read
        let opendal_requests_total_blocking_read = register_int_counter_with_registry!(
            "opendal_requests_total_blocking_read",
            "Total times of blocking_read be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_blocking_read",
            "Histogram of the time spent on blocking_reading",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let opendal_requests_duration_seconds_blocking_read =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "opendal_bytes_total_blocking_read",
            "blocking_read size",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let opendal_bytes_total_blocking_read =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_write
        let opendal_requests_total_blocking_write = register_int_counter_with_registry!(
            "opendal_requests_total_blocking_write",
            "Total times of blocking_write be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_blocking_write",
            "Histogram of the time spent on blocking_writing",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let opendal_requests_duration_seconds_blocking_write =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "opendal_bytes_total_blocking_write",
            "total size by blocking_write",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );
        let opendal_bytes_total_blocking_write =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_stat
        let opendal_requests_total_blocking_stat = register_int_counter_with_registry!(
            "opendal_requests_total_blocking_stat",
            "Total times of blocking_stat be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_blocking_stat",
            "blocking_stat letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_blocking_stat =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_delete
        let opendal_requests_total_blocking_delete = register_int_counter_with_registry!(
            "opendal_requests_total_blocking_delete",
            "Total times of blocking_delete be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_blocking_delete",
            "blocking_delete letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_blocking_delete =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_list
        let opendal_requests_total_blocking_list = register_int_counter_with_registry!(
            "opendal_requests_total_blocking_list",
            "Total times of blocking_list be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_blocking_list",
            "blocking_list letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_blocking_list =
            register_histogram_with_registry!(opts, registry).unwrap();

        // blocking_scan
        let opendal_requests_total_blocking_scan = register_int_counter_with_registry!(
            "opendal_requests_total_blocking_scan",
            "Total times of blocking_scan be called",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "opendal_requests_duration_seconds_blocking_scan",
            "blocking_scan letency",
            exponential_buckets(0.01, 2.0, 16).unwrap()
        );

        let opendal_requests_duration_seconds_blocking_scan =
            register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            opendal_requests_total_metadata,
            opendal_requests_duration_seconds_metadata,

            opendal_requests_total_create,
            opendal_requests_duration_seconds_create,

            opendal_requests_total_read,
            opendal_requests_duration_seconds_read,
            opendal_bytes_total_read,

            opendal_requests_total_write,
            opendal_requests_duration_seconds_write,
            opendal_bytes_total_write,

            opendal_requests_total_stat,
            opendal_requests_duration_seconds_stat,

            opendal_requests_total_delete,
            opendal_requests_duration_seconds_delete,

            opendal_requests_total_list,
            opendal_requests_duration_seconds_list,

            opendal_requests_total_scan,
            opendal_requests_duration_seconds_scan,

            opendal_requests_total_presign,
            opendal_requests_duration_seconds_presign,

            opendal_requests_total_batch,
            opendal_requests_duration_seconds_batch,

            opendal_requests_total_blocking_create,
            opendal_requests_duration_seconds_blocking_create,

            opendal_requests_total_blocking_read,
            opendal_requests_duration_seconds_blocking_read,
            opendal_bytes_total_blocking_read,

            opendal_requests_total_blocking_write,
            opendal_requests_duration_seconds_blocking_write,
            opendal_bytes_total_blocking_write,

            opendal_requests_total_blocking_stat,
            opendal_requests_duration_seconds_blocking_stat,

            opendal_requests_total_blocking_delete,
            opendal_requests_duration_seconds_blocking_delete,

            opendal_requests_total_blocking_list,
            opendal_requests_duration_seconds_blocking_list,

            opendal_requests_total_blocking_scan,
            opendal_requests_duration_seconds_blocking_scan,
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
    type Reader = PrometheusMetricWrapper<A::Reader>;
    type BlockingReader = PrometheusMetricWrapper<A::BlockingReader>;
    type Writer = PrometheusMetricWrapper<A::Writer>;
    type BlockingWriter = PrometheusMetricWrapper<A::BlockingWriter>;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        self.stats.opendal_requests_total_metadata.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_metadata
            .start_timer();
        let result = self.inner.info();
        timer.observe_duration();

        result
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.stats.opendal_requests_total_create.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_create
            .start_timer();
        let create_res = self.inner.create(path, args).await;

        timer.observe_duration();
        create_res.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Create, e.kind());
            e
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.stats.opendal_requests_total_read.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_read
            .start_timer();

        let read_res = self
            .inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    self.stats
                        .opendal_bytes_total_read
                        .observe(rp.metadata().content_length() as f64);
                    (
                        rp,
                        PrometheusMetricWrapper::new(r, Operation::Read, self.stats.clone()),
                    )
                })
            })
            .await;
        timer.observe_duration();
        read_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::Read, e.kind());
            e
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.stats.opendal_requests_total_write.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_write
            .start_timer();

        let write_res = self
            .inner
            .write(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        PrometheusMetricWrapper::new(r, Operation::Write, self.stats.clone()),
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
        self.stats.opendal_requests_total_stat.inc();
        let timer = self
            .stats
            .opendal_requests_duration_seconds_stat
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
        self.stats.opendal_requests_total_delete.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_delete
            .start_timer();

        let delete_res = self.inner.delete(path, args).await;
        timer.observe_duration();
        delete_res.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Delete, e.kind());
            e
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.stats.opendal_requests_total_list.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_list
            .start_timer();

        let list_res = self.inner.list(path, args).await;

        timer.observe_duration();
        list_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::List, e.kind());
            e
        })
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        self.stats.opendal_requests_total_scan.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_scan
            .start_timer();

        let scan_res = self.inner.scan(path, args).await;
        timer.observe_duration();
        scan_res.map_err(|e| {
            self.stats.increment_errors_total(Operation::Scan, e.kind());
            e
        })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.stats.opendal_requests_total_batch.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_batch
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
        self.stats.opendal_requests_total_presign.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_presign
            .start_timer();
        let result = self.inner.presign(path, args).await;
        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::Presign, e.kind());
            e
        })
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.stats.opendal_requests_total_blocking_create.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_blocking_create
            .start_timer();
        let result = self.inner.blocking_create(path, args);

        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingCreate, e.kind());
            e
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.stats.opendal_requests_total_blocking_read.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_blocking_read
            .start_timer();
        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            self.stats
                .opendal_bytes_total_read
                .observe(rp.metadata().content_length() as f64);
            (
                rp,
                PrometheusMetricWrapper::new(r, Operation::BlockingRead, self.stats.clone()),
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
        self.stats.opendal_requests_total_blocking_write.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_blocking_write
            .start_timer();
        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                PrometheusMetricWrapper::new(r, Operation::BlockingWrite, self.stats.clone()),
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
        self.stats.opendal_requests_total_blocking_stat.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_blocking_stat
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
        self.stats.opendal_requests_total_blocking_delete.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_blocking_delete
            .start_timer();
        let result = self.inner.blocking_delete(path, args);
        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingDelete, e.kind());
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.stats.opendal_requests_total_blocking_list.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_blocking_list
            .start_timer();
        let result = self.inner.blocking_list(path, args);
        timer.observe_duration();

        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingList, e.kind());
            e
        })
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.stats.opendal_requests_total_blocking_scan.inc();

        let timer = self
            .stats
            .opendal_requests_duration_seconds_blocking_scan
            .start_timer();
        let result = self.inner.blocking_scan(path, args);
        timer.observe_duration();
        result.map_err(|e| {
            self.stats
                .increment_errors_total(Operation::BlockingScan, e.kind());
            e
        })
    }
}

pub struct PrometheusMetricWrapper<R> {
    inner: R,

    op: Operation,
    stats: Arc<PrometheusMetrics>,
}

impl<R> PrometheusMetricWrapper<R> {
    fn new(inner: R, op: Operation, stats: Arc<PrometheusMetrics>) -> Self {
        Self { inner, op, stats }
    }
}

impl<R: oio::Read> oio::Read for PrometheusMetricWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        self.inner.poll_read(cx, buf).map(|res| match res {
            Ok(bytes) => {
                self.stats.opendal_bytes_total_read.observe(bytes as f64);
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
                self.stats
                    .opendal_bytes_total_read
                    .observe(bytes.len() as f64);
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
                self.stats
                    .opendal_bytes_total_blocking_read
                    .observe(n as f64);
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
                self.stats
                    .opendal_bytes_total_blocking_read
                    .observe(bytes.len() as f64);
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
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        self.inner
            .write(bs)
            .await
            .map(|_| self.stats.opendal_bytes_total_write.observe(size as f64))
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
            .map(|_| self.stats.opendal_bytes_total_write.observe(size as f64))
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

impl<R: oio::BlockingWrite> oio::BlockingWrite for PrometheusMetricWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        self.inner
            .write(bs)
            .map(|_| {
                self.stats
                    .opendal_bytes_total_blocking_write
                    .observe(size as f64)
            })
            .map_err(|err| {
                self.stats.increment_errors_total(self.op, err.kind());
                err
            })
    }

    fn append(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        self.inner
            .append(bs)
            .map(|_| {
                self.stats
                    .opendal_bytes_total_blocking_write
                    .observe(size as f64)
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
