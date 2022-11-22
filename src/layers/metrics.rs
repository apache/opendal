// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use async_trait::async_trait;
use futures::AsyncRead;
use metrics::register_counter;
use metrics::register_histogram;
use metrics::Counter;
use metrics::Histogram;

use super::util::set_accessor_for_object_iterator;
use super::util::set_accessor_for_object_steamer;
use crate::ops::*;
use crate::*;

static METRIC_REQUESTS_TOTAL: &str = "opendal_requests_total";
static METRIC_REQUESTS_DURATION_SECONDS: &str = "opendal_requests_duration_seconds";
/// Metrics of Failed requests, poll ok but outcome is error
static METRIC_FAILURES_TOTAL: &str = "opendal_failures_total";
/// Metrics of Errored requests, poll error
static METRIC_ERRORS_TOTAL: &str = "opendal_errors_total";
/// Metrics of bytes consumed by opendal.
static METRIC_BYTES_TOTAL: &str = "opendal_bytes_total";

static LABEL_SERVICE: &str = "service";
static LABEL_OPERATION: &str = "operation";

/// MetricsLayer will add metrics for OpenDAL.
///
/// # Metrics
///
/// - `opendal_requests_total`: Total requests numbers;
/// - `opendal_requests_duration_seconds`: Request duration seconds;
///   - NOTE: this metric tracks the duration of the OpenDAL's function call, not the underlying http request duration.
/// - `opendal_failures_total`: number of unexpected errors encountered, like network broken;
/// - `opendal_errors_total`: number of expected errors failures encountered, like file not found;
/// - `opendal_bytes_read_total`: bytes read from underlying storage;
/// - `opendal_bytes_write_total`: bytes write to underlying storage.
///
/// # Labels
///
/// Most metrics will carry the following labels
///
/// - `service`: Service name from [`Scheme`][crate::Scheme]
/// - `operation`: Operation name from [`Operation`]
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::MetricsLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(MetricsLayer);
/// ```
#[derive(Debug, Copy, Clone)]
pub struct MetricsLayer;

impl Layer for MetricsLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        let meta = inner.metadata();

        Arc::new(MetricsAccessor {
            inner,
            handle: MetricsHandler::new(meta.scheme().into_static()),
        })
    }
}

/// metrics will hold all metrics handlers in a `RwLock<Map>`.
///
/// By holding all metrics handlers we needed, we can reduce the lock
/// cost on fetching them. All metrics update will be atomic operations.
#[derive(Clone)]
struct MetricsHandler {
    requests_total_metadata: Counter,
    requests_duration_seconds_metadata: Histogram,

    requests_total_create: Counter,
    requests_duration_seconds_create: Histogram,
    failures_total_create: Counter,
    errors_total_create: Counter,

    requests_total_read: Counter,
    requests_duration_seconds_read: Histogram,
    failures_total_read: Counter,
    errors_total_read: Counter,
    bytes_total_read: Counter,

    requests_total_write: Counter,
    requests_duration_seconds_write: Histogram,
    failures_total_write: Counter,
    errors_total_write: Counter,
    bytes_total_write: Counter,

    requests_total_stat: Counter,
    requests_duration_seconds_stat: Histogram,
    failures_total_stat: Counter,
    errors_total_stat: Counter,

    requests_total_delete: Counter,
    requests_duration_seconds_delete: Histogram,
    failures_total_delete: Counter,
    errors_total_delete: Counter,

    requests_total_list: Counter,
    requests_duration_seconds_list: Histogram,
    failures_total_list: Counter,
    errors_total_list: Counter,

    requests_total_presign: Counter,
    requests_duration_seconds_presign: Histogram,
    failures_total_presign: Counter,
    errors_total_presign: Counter,

    requests_total_create_multipart: Counter,
    requests_duration_seconds_create_multipart: Histogram,
    failures_total_create_multipart: Counter,
    errors_total_create_multipart: Counter,

    requests_total_write_multipart: Counter,
    requests_duration_seconds_write_multipart: Histogram,
    failures_total_write_multipart: Counter,
    errors_total_write_multipart: Counter,
    bytes_total_write_multipart: Counter,

    requests_total_complete_multipartt: Counter,
    requests_duration_seconds_complete_multipart: Histogram,
    failures_total_complete_multipart: Counter,
    errors_total_complete_multipart: Counter,

    requests_total_abort_multipart: Counter,
    requests_duration_seconds_abort_multipart: Histogram,
    failures_total_abort_multipart: Counter,
    errors_total_abort_multipart: Counter,

    requests_total_blocking_create: Counter,
    requests_duration_seconds_blocking_create: Histogram,
    failures_total_blocking_create: Counter,
    errors_total_blocking_create: Counter,

    requests_total_blocking_read: Counter,
    requests_duration_seconds_blocking_read: Histogram,
    failures_total_blocking_read: Counter,
    errors_total_blocking_read: Counter,
    bytes_total_blocking_read: Counter,

    requests_total_blocking_write: Counter,
    requests_duration_seconds_blocking_write: Histogram,
    failures_total_blocking_write: Counter,
    errors_total_blocking_write: Counter,
    bytes_total_blocking_write: Counter,

    requests_total_blocking_stat: Counter,
    requests_duration_seconds_blocking_stat: Histogram,
    failures_total_blocking_stat: Counter,
    errors_total_blocking_stat: Counter,

    requests_total_blocking_delete: Counter,
    requests_duration_seconds_blocking_delete: Histogram,
    failures_total_blocking_delete: Counter,
    errors_total_blocking_delete: Counter,

    requests_total_blocking_list: Counter,
    requests_duration_seconds_blocking_list: Histogram,
    failures_total_blocking_list: Counter,
    errors_total_blocking_list: Counter,
}

impl MetricsHandler {
    fn new(service: &'static str) -> Self {
        Self {
            requests_total_metadata: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Metadata.into_static(),
            ),
            requests_duration_seconds_metadata: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Metadata.into_static(),
            ),

            requests_total_create: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Create.into_static(),
            ),
            requests_duration_seconds_create: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Create.into_static(),
            ),
            failures_total_create: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Create.into_static(),
            ),
            errors_total_create: register_counter!(
                METRIC_ERRORS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Create.into_static(),
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
            failures_total_read: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Read.into_static(),
            ),
            errors_total_read: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_write: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Write.into_static(),
            ),
            errors_total_write: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_stat: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Stat.into_static(),
            ),
            errors_total_stat: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_delete: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Delete.into_static(),
            ),
            errors_total_delete: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_list: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::List.into_static(),
            ),
            errors_total_list: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_presign: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Presign.into_static(),
            ),
            errors_total_presign: register_counter!(
                METRIC_ERRORS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::Presign.into_static(),
            ),

            requests_total_create_multipart: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CreateMultipart.into_static(),
            ),
            requests_duration_seconds_create_multipart: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CreateMultipart.into_static(),
            ),
            failures_total_create_multipart: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CreateMultipart.into_static(),
            ),
            errors_total_create_multipart: register_counter!(
                METRIC_ERRORS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CreateMultipart.into_static(),
            ),

            requests_total_write_multipart: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::WriteMultipart.into_static(),
            ),
            requests_duration_seconds_write_multipart: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::WriteMultipart.into_static(),
            ),
            failures_total_write_multipart: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::WriteMultipart.into_static(),
            ),
            errors_total_write_multipart: register_counter!(
                METRIC_ERRORS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::WriteMultipart.into_static(),
            ),
            bytes_total_write_multipart: register_counter!(
                METRIC_BYTES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::WriteMultipart.into_static(),
            ),

            requests_total_complete_multipartt: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CompleteMultipart.into_static(),
            ),
            requests_duration_seconds_complete_multipart: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CompleteMultipart.into_static(),
            ),
            failures_total_complete_multipart: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CompleteMultipart.into_static(),
            ),
            errors_total_complete_multipart: register_counter!(
                METRIC_ERRORS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::CompleteMultipart.into_static(),
            ),

            requests_total_abort_multipart: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::AbortMultipart.into_static(),
            ),
            requests_duration_seconds_abort_multipart: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::AbortMultipart.into_static(),
            ),
            failures_total_abort_multipart: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::AbortMultipart.into_static(),
            ),
            errors_total_abort_multipart: register_counter!(
                METRIC_ERRORS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::AbortMultipart.into_static(),
            ),

            requests_total_blocking_create: register_counter!(
                METRIC_REQUESTS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingCreate.into_static(),
            ),
            requests_duration_seconds_blocking_create: register_histogram!(
                METRIC_REQUESTS_DURATION_SECONDS,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingCreate.into_static(),
            ),
            failures_total_blocking_create: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingCreate.into_static(),
            ),
            errors_total_blocking_create: register_counter!(
                METRIC_ERRORS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingCreate.into_static(),
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
            failures_total_blocking_read: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingRead.into_static(),
            ),
            errors_total_blocking_read: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_blocking_write: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingWrite.into_static(),
            ),
            errors_total_blocking_write: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_blocking_stat: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingStat.into_static(),
            ),
            errors_total_blocking_stat: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_blocking_delete: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingDelete.into_static(),
            ),
            errors_total_blocking_delete: register_counter!(
                METRIC_ERRORS_TOTAL,
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
            failures_total_blocking_list: register_counter!(
                METRIC_FAILURES_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingList.into_static(),
            ),
            errors_total_blocking_list: register_counter!(
                METRIC_ERRORS_TOTAL,
                LABEL_SERVICE => service,
                LABEL_OPERATION => Operation::BlockingList.into_static(),
            ),
        }
    }
}

#[derive(Clone)]
struct MetricsAccessor {
    inner: Arc<dyn Accessor>,
    handle: MetricsHandler,
}

impl Debug for MetricsAccessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Accessor for MetricsAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    fn metadata(&self) -> AccessorMetadata {
        self.handle.requests_total_metadata.increment(1);

        let start = Instant::now();
        let result = self.inner.metadata();
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_metadata.record(dur);

        result
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.handle.requests_total_create.increment(1);

        let start = Instant::now();
        let result = self.inner.create(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_create.record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_create.increment(1);
            } else {
                self.handle.errors_total_create.increment(1);
            }
            e
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        self.handle.requests_total_read.increment(1);

        let start = Instant::now();

        let result = self.inner.read(path, args).await.map(|(rp, r)| {
            (
                rp,
                Box::new(MetricReader::new(
                    r,
                    self.handle.bytes_total_read.clone(),
                    self.handle.failures_total_read.clone(),
                    self.handle.requests_duration_seconds_read.clone(),
                    Some(start),
                )) as BytesReader,
            )
        });

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_read.increment(1);
            } else {
                self.handle.errors_total_read.increment(1);
            }
            e
        })
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        self.handle.requests_total_write.increment(1);

        let r = Box::new(MetricReader::new(
            r,
            self.handle.bytes_total_write.clone(),
            self.handle.failures_total_write.clone(),
            self.handle.requests_duration_seconds_write.clone(),
            None,
        ));

        let start = Instant::now();
        let result = self.inner.write(path, args, r).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_write.record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_write.increment(1);
            } else {
                self.handle.errors_total_write.increment(1);
            }
            e
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.handle.requests_total_stat.increment(1);

        let start = Instant::now();
        let result = self.inner.stat(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_stat.record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_stat.increment(1);
            } else {
                self.handle.errors_total_stat.increment(1);
            }
            e
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.handle.requests_total_delete.increment(1);

        let start = Instant::now();
        let result = self.inner.delete(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_delete.record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_delete.increment(1);
            } else {
                self.handle.errors_total_delete.increment(1);
            }
            e
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        self.handle.requests_total_list.increment(1);

        let start = Instant::now();
        let result = self.inner.list(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_list.record(dur);

        result
            .map_err(|e| {
                if e.kind() == ErrorKind::Unexpected {
                    self.handle.failures_total_list.increment(1);
                } else {
                    self.handle.errors_total_list.increment(1);
                }
                e
            })
            .map(|s| set_accessor_for_object_steamer(s, self.clone()))
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.handle.requests_total_presign.increment(1);

        let start = Instant::now();
        let result = self.inner.presign(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle.requests_duration_seconds_presign.record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_presign.increment(1);
            } else {
                self.handle.errors_total_presign.increment(1);
            }
            e
        })
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        self.handle.requests_total_create_multipart.increment(1);

        let start = Instant::now();
        let result = self.inner.create_multipart(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_create_multipart
            .record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_create_multipart.increment(1);
            } else {
                self.handle.errors_total_create_multipart.increment(1);
            }
            e
        })
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<RpWriteMultipart> {
        self.handle.requests_total_write_multipart.increment(1);

        let r = Box::new(MetricReader::new(
            r,
            self.handle.bytes_total_write_multipart.clone(),
            self.handle.failures_total_write_multipart.clone(),
            self.handle
                .requests_duration_seconds_write_multipart
                .clone(),
            None,
        ));

        let start = Instant::now();
        let result = self.inner.write_multipart(path, args, r).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_write_multipart
            .record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_write_multipart.increment(1);
            } else {
                self.handle.errors_total_write_multipart.increment(1);
            }
            e
        })
    }

    async fn complete_multipart(&self, path: &str, args: OpCompleteMultipart) -> Result<()> {
        self.handle.requests_total_complete_multipartt.increment(1);

        let start = Instant::now();
        let result = self.inner.complete_multipart(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_complete_multipart
            .record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_complete_multipart.increment(1);
            } else {
                self.handle.errors_total_complete_multipart.increment(1);
            }
            e
        })
    }

    async fn abort_multipart(&self, path: &str, args: OpAbortMultipart) -> Result<()> {
        self.handle.requests_total_abort_multipart.increment(1);

        let start = Instant::now();
        let result = self.inner.abort_multipart(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_abort_multipart
            .record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_abort_multipart.increment(1);
            } else {
                self.handle.errors_total_abort_multipart.increment(1);
            }
            e
        })
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.handle.requests_total_blocking_create.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_create(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_create
            .record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_blocking_create.increment(1);
            } else {
                self.handle.errors_total_blocking_create.increment(1);
            }
            e
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        self.handle.requests_total_blocking_read.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_read(path, args).map(|reader| {
            Box::new(BlockingMetricReader::new(
                reader,
                self.handle.bytes_total_blocking_read.clone(),
                self.handle.failures_total_blocking_read.clone(),
                self.handle.requests_duration_seconds_blocking_read.clone(),
                Some(start),
            )) as BlockingBytesReader
        });

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_blocking_read.increment(1);
            } else {
                self.handle.errors_total_blocking_read.increment(1);
            }
            e
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        self.handle.requests_total_blocking_write.increment(1);

        let r = Box::new(BlockingMetricReader::new(
            r,
            self.handle.bytes_total_blocking_write.clone(),
            self.handle.failures_total_blocking_write.clone(),
            self.handle.requests_duration_seconds_blocking_write.clone(),
            None,
        ));

        let start = Instant::now();
        let result = self.inner.blocking_write(path, args, r);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_write
            .record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_blocking_write.increment(1);
            } else {
                self.handle.errors_total_blocking_write.increment(1);
            }
            e
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        self.handle.requests_total_blocking_stat.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_stat(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_stat
            .record(dur);

        result.map_err(|e| {
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_blocking_stat.increment(1);
            } else {
                self.handle.errors_total_blocking_stat.increment(1);
            }
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
            if e.kind() == ErrorKind::Unexpected {
                self.handle.failures_total_blocking_delete.increment(1);
            } else {
                self.handle.errors_total_blocking_delete.increment(1);
            }
            e
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<ObjectIterator> {
        self.handle.requests_total_blocking_list.increment(1);

        let start = Instant::now();
        let result = self.inner.blocking_list(path, args);
        let dur = start.elapsed().as_secs_f64();

        self.handle
            .requests_duration_seconds_blocking_list
            .record(dur);

        result
            .map_err(|e| {
                if e.kind() == ErrorKind::Unexpected {
                    self.handle.failures_total_blocking_list.increment(1);
                } else {
                    self.handle.errors_total_blocking_list.increment(1);
                }
                e
            })
            .map(|s| set_accessor_for_object_iterator(s, self.clone()))
    }
}

struct MetricReader {
    inner: BytesReader,

    bytes_counter: Counter,
    failures_counter: Counter,
    requests_duration_seconds: Histogram,

    start: Option<Instant>,
    bytes: u64,
}

impl MetricReader {
    fn new(
        inner: BytesReader,
        bytes_counter: Counter,
        failures_counter: Counter,
        requests_duration_seconds: Histogram,
        start: Option<Instant>,
    ) -> Self {
        Self {
            inner,
            bytes_counter,
            failures_counter,
            requests_duration_seconds,

            start,
            bytes: 0,
        }
    }
}

impl AsyncRead for MetricReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut (*self.inner))
            .poll_read(cx, buf)
            .map(|res| match res {
                Ok(bytes) => {
                    self.bytes += bytes as u64;
                    Ok(bytes)
                }
                Err(e) => {
                    self.failures_counter.increment(1);
                    Err(e)
                }
            })
    }
}

impl Drop for MetricReader {
    fn drop(&mut self) {
        self.bytes_counter.increment(self.bytes);
        if let Some(instant) = self.start {
            let dur = instant.elapsed().as_secs_f64();
            self.requests_duration_seconds.record(dur);
        }
    }
}

struct BlockingMetricReader {
    inner: BlockingBytesReader,

    bytes_counter: Counter,
    failures_counter: Counter,
    requests_duration_seconds: Histogram,

    start: Option<Instant>,
    bytes: u64,
}

impl BlockingMetricReader {
    fn new(
        inner: BlockingBytesReader,
        bytes_counter: Counter,
        failures_counter: Counter,
        requests_duration_seconds: Histogram,
        start: Option<Instant>,
    ) -> Self {
        Self {
            inner,
            bytes_counter,
            failures_counter,
            requests_duration_seconds,

            start,
            bytes: 0,
        }
    }
}

impl Read for BlockingMetricReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner
            .read(buf)
            .map(|n| {
                self.bytes += n as u64;
                n
            })
            .map_err(|e| {
                self.failures_counter.increment(1);
                e
            })
    }
}

impl Drop for BlockingMetricReader {
    fn drop(&mut self) {
        self.bytes_counter.increment(self.bytes);
        if let Some(instant) = self.start {
            let dur = instant.elapsed().as_secs_f64();
            self.requests_duration_seconds.record(dur);
        }
    }
}
