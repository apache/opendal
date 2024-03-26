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
use std::time::Instant;

use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use futures::FutureExt;
use futures::TryFutureExt;
use metrics::increment_counter;
use metrics::register_counter;
use metrics::register_histogram;
use metrics::Counter;
use metrics::Histogram;

use crate::raw::*;
use crate::*;

/// requests_total records all successful requests sent via operator.
static METRIC_REQUESTS_TOTAL: &str = "opendal_requests_total";
/// requests_duration_seconds records the duration seconds of successful
/// requests.
///
/// # NOTE
///
/// this metric will track the whole lifetime of this request:
///
/// - Building request
/// - Sending request
/// - Receiving response
/// - Consuming response
static METRIC_REQUESTS_DURATION_SECONDS: &str = "opendal_requests_duration_seconds";
static METRICS_ERRORS_TOTAL: &str = "opendal_errors_total";
/// bytes_total records all bytes processed by operator.
static METRIC_BYTES_TOTAL: &str = "opendal_bytes_total";

/// The scheme of the service.
static LABEL_SERVICE: &str = "service";
/// The operation of this request.
static LABEL_OPERATION: &str = "operation";
/// The error kind of this failed request.
static LABEL_ERROR: &str = "error";

/// Add [metrics](https://docs.rs/metrics/) for every operations.
///
/// # Metrics
///
/// - `opendal_requests_total`: Total request numbers.
/// - `opendal_requests_duration_seconds`: Request duration seconds.
/// - `opendal_errors_total`: Total error numbers.
/// - `opendal_bytes_total`: bytes read/write from/to underlying storage.
///
/// # Labels
///
/// metrics will carry the following labels
///
/// - `service`: Service name from [`Scheme`]
/// - `operation`: Operation name from [`Operation`]
/// - `error`: [`ErrorKind`] received by requests
///
/// # Notes
///
/// Please make sure the exporter has been pulled in regular time.
/// Otherwise, the histogram data collected by `requests_duration_seconds`
/// could result in OOM.
///
/// # Examples
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::layers::MetricsLayer;
/// use opendal::services;
/// use opendal::Operator;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(MetricsLayer)
///     .finish();
/// ```
///
/// # Output
///
/// OpenDAL is using [`metrics`](https://docs.rs/metrics/latest/metrics/) for metrics internally.
///
/// To enable metrics output, please enable one of the exporters that `metrics` supports.
///
/// Take [`metrics_exporter_prometheus`](https://docs.rs/metrics-exporter-prometheus/latest/metrics_exporter_prometheus/) as an example:
///
/// ```ignore
/// let builder = PrometheusBuilder::new();
/// builder.install().expect("failed to install recorder/exporter");
/// let handle = builder.install_recorder().expect("failed to install recorder");
/// let (recorder, exporter) = builder.build().expect("failed to build recorder/exporter");
/// let recorder = builder.build_recorder().expect("failed to build recorder");
/// ```
#[derive(Debug, Copy, Clone)]
pub struct MetricsLayer;

impl<A: Accessor> Layer<A> for MetricsLayer {
    type LayeredAccessor = MetricsAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.info();

        MetricsAccessor {
            inner,
            handle: Arc::new(MetricsHandler::new(meta.scheme().into_static())),
        }
    }
}

/// metrics will hold all metrics handlers in a `RwLock<HashMap>`.
///
/// By holding all metrics handlers we needed, we can reduce the lock
/// cost on fetching them. All metrics update will be atomic operations.
struct MetricsHandler {
    service: &'static str,

    requests_total_metadata: Counter,
    requests_duration_seconds_metadata: Histogram,

    requests_total_create: Counter,
    requests_duration_seconds_create: Histogram,

    requests_total_read: Counter,
    requests_duration_seconds_read: Histogram,
    bytes_total_read: Counter,

    requests_total_write: Counter,
    requests_duration_seconds_write: Histogram,
    bytes_total_write: Counter,

    requests_total_stat: Counter,
    requests_duration_seconds_stat: Histogram,

    requests_total_delete: Counter,
    requests_duration_seconds_delete: Histogram,

    requests_total_list: Counter,
    requests_duration_seconds_list: Histogram,

    requests_total_presign: Counter,
    requests_duration_seconds_presign: Histogram,

    requests_total_batch: Counter,
    requests_duration_seconds_batch: Histogram,

    requests_total_blocking_create: Counter,
    requests_duration_seconds_blocking_create: Histogram,

    requests_total_blocking_read: Counter,
    requests_duration_seconds_blocking_read: Histogram,
    bytes_total_blocking_read: Counter,

    requests_total_blocking_write: Counter,
    requests_duration_seconds_blocking_write: Histogram,
    #[allow(dead_code)]
    bytes_total_blocking_write: Counter,

    requests_total_blocking_stat: Counter,
    requests_duration_seconds_blocking_stat: Histogram,

    requests_total_blocking_delete: Counter,
    requests_duration_seconds_blocking_delete: Histogram,

    requests_total_blocking_list: Counter,
    requests_duration_seconds_blocking_list: Histogram,
}

impl MetricsHandler {
    fn new(service: &'static str) -> Self {
        Self {
            service,

            requests_total_metadata: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Info.into_static(),
            ),
            requests_duration_seconds_metadata: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Info.into_static(),
            ),

            requests_total_create: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CreateDir.into_static(),
            ),
            requests_duration_seconds_create: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CreateDir.into_static(),
            ),

            requests_total_read: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Read.into_static(),
            ),
            requests_duration_seconds_read: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Read.into_static(),
            ),
            bytes_total_read: register_counter!(
                METRIC_BYTES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Read.into_static(),
            ),

            requests_total_write: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Write.into_static(),
            ),
            requests_duration_seconds_write: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Write.into_static(),
            ),
            bytes_total_write: register_counter!(
                METRIC_BYTES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Write.into_static(),
            ),

            requests_total_stat: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Stat.into_static(),
            ),
            requests_duration_seconds_stat: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Stat.into_static(),
            ),

            requests_total_delete: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Delete.into_static(),
            ),
            requests_duration_seconds_delete: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Delete.into_static(),
            ),

            requests_total_list: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::List.into_static(),
            ),
            requests_duration_seconds_list: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::List.into_static(),
            ),

            requests_total_presign: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Presign.into_static(),
            ),
            requests_duration_seconds_presign: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Presign.into_static(),
            ),

            requests_total_batch: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Batch.into_static(),
            ),
            requests_duration_seconds_batch: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Batch.into_static(),
            ),

            requests_total_blocking_create: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingCreateDir.into_static(),
            ),
            requests_duration_seconds_blocking_create: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingCreateDir.into_static(),
            ),

            requests_total_blocking_read: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingRead.into_static(),
            ),
            requests_duration_seconds_blocking_read: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingRead.into_static(),
            ),
            bytes_total_blocking_read: register_counter!(
                METRIC_BYTES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingRead.into_static(),
            ),

            requests_total_blocking_write: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingWrite.into_static(),
            ),
            requests_duration_seconds_blocking_write: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingWrite.into_static(),
            ),
            bytes_total_blocking_write: register_counter!(
                METRIC_BYTES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingWrite.into_static(),
            ),

            requests_total_blocking_stat: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingStat.into_static(),
            ),
            requests_duration_seconds_blocking_stat: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingStat.into_static(),
            ),

            requests_total_blocking_delete: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingDelete.into_static(),
            ),
            requests_duration_seconds_blocking_delete: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingDelete.into_static(),
            ),

            requests_total_blocking_list: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingList.into_static(),
            ),
            requests_duration_seconds_blocking_list: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingList.into_static(),
            ),
        }
    }

    /// error handling is the cold path, so we will not init error counters
    /// in advance.
    #[inline]
    fn increment_errors_total(&self, op: Operation, kind: ErrorKind) {
        increment_counter!(METRICS_ERRORS_TOTAL,
            LABEL_SERVICE => self.service,
            LABEL_OPERATION => op.into_static(),
            LABEL_ERROR => kind.into_static(),
        )
    }
}

#[derive(Clone)]
pub struct MetricsAccessor<A: Accessor> {
    inner: A,
    handle: Arc<MetricsHandler>,
}

impl<A: Accessor> Debug for MetricsAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for MetricsAccessor<A> {
    type Inner = A;
    type Reader = MetricWrapper<A::Reader>;
    type BlockingReader = MetricWrapper<A::BlockingReader>;
    type Writer = MetricWrapper<A::Writer>;
    type BlockingWriter = MetricWrapper<A::BlockingWriter>;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        self.handle.requests_total_metadata.increment(1);

        let start = Instant::now();
        let result = self.inner.info();
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_metadata.record(dur);

        result
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.handle.requests_total_create.increment(1);

        let start = Instant::now();

        self.inner
            .create_dir(path, args)
            .map(|v| {
                let dur = start.elapsed().as_secs_f64();

                self.handle.requests_duration_seconds_create.record(dur);

                v.map_err(|e| {
                    self.handle
                        .increment_errors_total(Operation::CreateDir, e.kind());
                    e
                })
            })
            .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.handle.requests_total_read.increment(1);

        let _start = Instant::now();

        self.inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        MetricWrapper::new(
                            r,
                            Operation::Read,
                            self.handle.clone(),
                            self.handle.bytes_total_read.clone(),
                            self.handle.requests_duration_seconds_read.clone(),
                        ),
                    )
                })
                .map_err(|err| {
                    self.handle
                        .increment_errors_total(Operation::Read, err.kind());
                    err
                })
            })
            .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.handle.requests_total_write.increment(1);

        let _start = Instant::now();

        self.inner
            .write(path, args)
            .map_ok(|(rp, w)| {
                (
                    rp,
                    MetricWrapper::new(
                        w,
                        Operation::Write,
                        self.handle.clone(),
                        self.handle.bytes_total_write.clone(),
                        self.handle.requests_duration_seconds_write.clone(),
                    ),
                )
            })
            .inspect_err(|e| {
                self.handle
                    .increment_errors_total(Operation::Write, e.kind());
            })
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.handle.requests_total_stat.increment(1);

        let start = Instant::now();

        self.inner
            .stat(path, args)
            .inspect_ok(|_| {
                let dur = start.elapsed().as_secs_f64();

                self.handle.requests_duration_seconds_stat.record(dur);
            })
            .inspect_err(|e| {
                self.handle
                    .increment_errors_total(Operation::Stat, e.kind());
            })
            .await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.handle.requests_total_delete.increment(1);

        let start = Instant::now();

        self.inner
            .delete(path, args)
            .inspect_ok(|_| {
                let dur = start.elapsed().as_secs_f64();

                self.handle.requests_duration_seconds_delete.record(dur);
            })
            .inspect_err(|e| {
                self.handle
                    .increment_errors_total(Operation::Delete, e.kind());
            })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.handle.requests_total_list.increment(1);

        let start = Instant::now();

        self.inner
            .list(path, args)
            .inspect_ok(|_| {
                let dur = start.elapsed().as_secs_f64();

                self.handle.requests_duration_seconds_list.record(dur);
            })
            .inspect_err(|e| {
                self.handle
                    .increment_errors_total(Operation::List, e.kind());
            })
            .await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.handle.requests_total_batch.increment(1);

        let start = Instant::now();
        let result = self.inner.batch(args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_batch.record(dur);

        result.map_err(|e| {
            self.handle
                .increment_errors_total(Operation::Batch, e.kind());
            e
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.handle.requests_total_presign.increment(1);

        let start = Instant::now();
        let result = self.inner.presign(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_presign.record(dur);

        result.map_err(|e| {
            self.handle
                .increment_errors_total(Operation::Presign, e.kind());
            e
        })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.handle.requests_total_blocking_create.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_create_dir(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_create
            .record(dur);

        result.map_err(|e| {
            self.handle
                .increment_errors_total(Operation::BlockingCreateDir, e.kind());
            e
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.handle.requests_total_blocking_read.increment(1);

        let _start = Instant::now();
        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                MetricWrapper::new(
                    r,
                    Operation::BlockingRead,
                    self.handle.clone(),
                    self.handle.bytes_total_blocking_read.clone(),
                    self.handle.requests_duration_seconds_blocking_read.clone(),
                ),
            )
        });

        result.map_err(|e| {
            self.handle
                .increment_errors_total(Operation::BlockingRead, e.kind());
            e
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.handle.requests_total_blocking_write.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_write(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_write
            .record(dur);

        result
            .map(|(rp, w)| {
                (
                    rp,
                    MetricWrapper::new(
                        w,
                        Operation::BlockingWrite,
                        self.handle.clone(),
                        self.handle.bytes_total_write.clone(),
                        self.handle.requests_duration_seconds_write.clone(),
                    ),
                )
            })
            .map_err(|e| {
                self.handle
                    .increment_errors_total(Operation::BlockingWrite, e.kind());
                e
            })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.handle.requests_total_blocking_stat.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_stat(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_stat
            .record(dur);

        result.map_err(|e| {
            self.handle
                .increment_errors_total(Operation::BlockingStat, e.kind());
            e
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.handle.requests_total_blocking_delete.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_delete(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_delete
            .record(dur);

        result.map_err(|e| {
            self.handle
                .increment_errors_total(Operation::BlockingDelete, e.kind());
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.handle.requests_total_blocking_list.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_list(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_list
            .record(dur);

        result.map_err(|e| {
            self.handle
                .increment_errors_total(Operation::BlockingList, e.kind());
            e
        })
    }
}

pub struct MetricWrapper<R> {
    inner: R,

    op: Operation,
    bytes_counter: Counter,
    requests_duration_seconds: Histogram,
    handle: Arc<MetricsHandler>,
}

impl<R> MetricWrapper<R> {
    fn new(
        inner: R,
        op: Operation,
        handle: Arc<MetricsHandler>,
        bytes_counter: Counter,
        requests_duration_seconds: Histogram,
    ) -> Self {
        Self {
            inner,
            op,
            handle,
            bytes_counter,
            requests_duration_seconds,
        }
    }
}

impl<R: oio::Read> oio::Read for MetricWrapper<R> {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        let start = Instant::now();

        match self.inner.read_at(offset, limit).await {
            Ok(bs) => {
                self.bytes_counter.increment(bs.remaining() as u64);
                self.requests_duration_seconds
                    .record(start.elapsed().as_secs_f64());
                Ok(bs)
            }
            Err(e) => {
                self.handle.increment_errors_total(self.op, e.kind());
                Err(e)
            }
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for MetricWrapper<R> {
    fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        let start = Instant::now();

        self.inner
            .read_at(offset, limit)
            .map(|bs| {
                self.bytes_counter.increment(bs.remaining() as u64);
                self.requests_duration_seconds
                    .record(start.elapsed().as_secs_f64());
                bs
            })
            .map_err(|e| {
                self.handle.increment_errors_total(self.op, e.kind());
                e
            })
    }
}

impl<R: oio::Write> oio::Write for MetricWrapper<R> {
    async fn write(&mut self, bs: Bytes) -> Result<usize> {
        let start = Instant::now();

        self.inner
            .write(bs)
            .await
            .map(|n| {
                self.bytes_counter.increment(n as u64);
                self.requests_duration_seconds
                    .record(start.elapsed().as_secs_f64());
                n
            })
            .map_err(|err| {
                self.handle.increment_errors_total(self.op, err.kind());
                err
            })
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await.map_err(|err| {
            self.handle.increment_errors_total(self.op, err.kind());
            err
        })
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await.map_err(|err| {
            self.handle.increment_errors_total(self.op, err.kind());
            err
        })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for MetricWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<usize> {
        self.inner
            .write(bs)
            .map(|n| {
                self.bytes_counter.increment(n as u64);
                n
            })
            .map_err(|err| {
                self.handle.increment_errors_total(self.op, err.kind());
                err
            })
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close().map_err(|err| {
            self.handle.increment_errors_total(self.op, err.kind());
            err
        })
    }
}
