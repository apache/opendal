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
use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use futures::Stream;
use futures::StreamExt;
use http::StatusCode;

use crate::raw::*;
use crate::*;

const KIB: f64 = 1024.0;
const MIB: f64 = 1024.0 * KIB;
const GIB: f64 = 1024.0 * MIB;

/// Buckets for data size metrics like OperationBytes
/// Covers typical file and object sizes from small files to large objects
pub const DEFAULT_BYTES_BUCKETS: &[f64] = &[
    4.0 * KIB,   // Small files
    64.0 * KIB,  // File system block size
    256.0 * KIB, //
    1.0 * MIB,   // Common size threshold for many systems
    4.0 * MIB,   // Best size for most http based systems
    16.0 * MIB,  //
    64.0 * MIB,  // Widely used threshold for multipart uploads
    256.0 * MIB, //
    1.0 * GIB,   // Considered large for many systems
    5.0 * GIB,   // Maximum size in single upload for many cloud storage services
];

/// Buckets for data transfer rate metrics like OperationBytesRate
///
/// Covers various network speeds from slow connections to high-speed transfers
///
/// Note: this is for single operation rate, not for total bandwidth.
pub const DEFAULT_BYTES_RATE_BUCKETS: &[f64] = &[
    // Low-speed network range (mobile/weak connections)
    8.0 * KIB,   // ~64Kbps - 2G networks
    32.0 * KIB,  // ~256Kbps - 3G networks
    128.0 * KIB, // ~1Mbps - Basic broadband
    // Standard broadband range
    1.0 * MIB,  // ~8Mbps - Entry-level broadband
    8.0 * MIB,  // ~64Mbps - Fast broadband
    32.0 * MIB, // ~256Mbps - Gigabit broadband
    // High-performance network range
    128.0 * MIB, // ~1Gbps - Standard datacenter
    512.0 * MIB, // ~4Gbps - Fast datacenter
    2.0 * GIB,   // ~16Gbps - High-end interconnects
    // Ultra-high-speed range
    8.0 * GIB,  // ~64Gbps - InfiniBand/RDMA
    32.0 * GIB, // ~256Gbps - Top-tier datacenters
];

/// Buckets for batch operation entry counts (OperationEntriesCount)
/// Covers scenarios from single entry operations to large batch operations
pub const DEFAULT_ENTRIES_BUCKETS: &[f64] = &[
    1.0,     // Single item operations
    5.0,     // Very small batches
    10.0,    // Small batches
    50.0,    // Medium batches
    100.0,   // Standard batch size
    500.0,   // Large batches
    1000.0,  // Very large batches, API limits for some services
    5000.0,  // Huge batches, multi-page operations
    10000.0, // Extremely large operations, multi-request batches
];

/// Buckets for batch operation processing rates (OperationEntriesRate)
/// Measures how many entries can be processed per second
pub const DEFAULT_ENTRIES_RATE_BUCKETS: &[f64] = &[
    1.0,     // Slowest processing, heavy operations per entry
    10.0,    // Slow processing, complex operations
    50.0,    // Moderate processing speed
    100.0,   // Good processing speed, efficient operations
    500.0,   // Fast processing, optimized operations
    1000.0,  // Very fast processing, simple operations
    5000.0,  // Extremely fast processing, bulk operations
    10000.0, // Maximum speed, listing operations, local systems
];

/// Buckets for operation duration metrics like OperationDurationSeconds
/// Covers timeframes from fast metadata operations to long-running transfers
pub const DEFAULT_DURATION_SECONDS_BUCKETS: &[f64] = &[
    0.001, // 1ms - Fastest operations, cached responses
    0.01,  // 10ms - Fast metadata operations, local operations
    0.05,  // 50ms - Quick operations, nearby cloud resources
    0.1,   // 100ms - Standard API response times, typical cloud latency
    0.25,  // 250ms - Medium operations, small data transfers
    0.5,   // 500ms - Medium-long operations, larger metadata operations
    1.0,   // 1s - Long operations, small file transfers
    2.5,   // 2.5s - Extended operations, medium file transfers
    5.0,   // 5s - Long-running operations, large transfers
    10.0,  // 10s - Very long operations, very large transfers
    30.0,  // 30s - Extended operations, complex batch processes
    60.0,  // 1min - Near timeout operations, extremely large transfers
];

/// Buckets for time to first byte metrics like OperationTtfbSeconds
/// Focuses on initial response times, which are typically shorter than full operations
pub const DEFAULT_TTFB_BUCKETS: &[f64] = &[
    0.001, // 1ms - Cached or local resources
    0.01,  // 10ms - Very fast responses, same region
    0.025, // 25ms - Fast responses, optimized configurations
    0.05,  // 50ms - Good response times, standard configurations
    0.1,   // 100ms - Average response times for cloud storage
    0.2,   // 200ms - Slower responses, cross-region or throttled
    0.4,   // 400ms - Poor response times, network congestion
    0.8,   // 800ms - Very slow responses, potential issues
    1.6,   // 1.6s - Problematic responses, retry territory
    3.2,   // 3.2s - Critical latency issues, close to timeouts
];

/// The metric label for the scheme like s3, fs, cos.
pub static LABEL_SCHEME: &str = "scheme";
/// The metric label for the namespace like bucket name in s3.
pub static LABEL_NAMESPACE: &str = "namespace";
/// The metric label for the root path.
pub static LABEL_ROOT: &str = "root";
/// The metric label for the operation like read, write, list.
pub static LABEL_OPERATION: &str = "operation";
/// The metric label for the error.
pub static LABEL_ERROR: &str = "error";
/// The metric label for the http code.
pub static LABEL_STATUS_CODE: &str = "status_code";

/// MetricLabels are the labels for the metrics.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetricLabels {
    /// The storage scheme identifier (e.g., "s3", "gcs", "azblob", "fs").
    /// Used to differentiate between different storage backends.
    pub scheme: Scheme,
    /// The storage namespace (e.g., bucket name, container name).
    /// Identifies the specific storage container being accessed.
    pub namespace: Arc<str>,
    /// The root path within the namespace that was configured.
    /// Used to track operations within a specific path prefix.
    pub root: Arc<str>,
    /// The operation being performed (e.g., "read", "write", "list").
    /// Identifies which API operation generated this metric.
    pub operation: &'static str,
    /// The specific error kind that occurred during an operation.
    /// Only populated for `OperationErrorsTotal` metric.
    /// Used to track frequency of specific error types.
    pub error: Option<ErrorKind>,
    /// The HTTP status code received in an error response.
    /// Only populated for `HttpStatusErrorsTotal` metric.
    /// Used to track frequency of specific HTTP error status codes.
    pub status_code: Option<StatusCode>,
}

impl MetricLabels {
    /// Create a new set of MetricLabels.
    fn new(info: Arc<AccessorInfo>, op: &'static str) -> Self {
        MetricLabels {
            scheme: info.scheme(),
            namespace: info.name(),
            root: info.root(),
            operation: op,
            ..MetricLabels::default()
        }
    }

    /// Add error to the metric labels.
    fn with_error(mut self, err: ErrorKind) -> Self {
        self.error = Some(err);
        self
    }

    /// Add status code to the metric labels.
    fn with_status_code(mut self, code: StatusCode) -> Self {
        self.status_code = Some(code);
        self
    }
}

/// MetricValue is the value the opendal sends to the metrics impls.
///
/// Metrics impls can be `prometheus_client`, `metrics` etc.
///
/// Every metrics impls SHOULD implement observe over the MetricValue to make
/// sure they provide the consistent metrics for users.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum MetricValue {
    /// Record the size of data processed in bytes.
    /// Metrics impl: Update a Histogram with the given byte count.
    OperationBytes(u64),
    /// Record the rate of data processing in bytes/second.
    /// Metrics impl: Update a Histogram with the calculated rate value.
    OperationBytesRate(f64),
    /// Record the number of entries (files, objects, keys) processed.
    /// Metrics impl: Update a Histogram with the entry count.
    OperationEntries(u64),
    /// Record the rate of entries processing in entries/second.
    /// Metrics impl: Update a Histogram with the calculated rate value.
    OperationEntriesRate(f64),
    /// Record the total duration of an operation.
    /// Metrics impl: Update a Histogram with the duration converted to seconds (as f64).
    OperationDurationSeconds(Duration),
    /// Increment the counter for operation errors.
    /// Metrics impl: Increment a Counter by 1.
    OperationErrorsTotal,
    /// Update the current number of executing operations.
    /// Metrics impl: Add the value (positive or negative) to a Gauge.
    OperationExecuting(isize),
    /// Record the time to first byte duration.
    /// Metrics impl: Update a Histogram with the duration converted to seconds (as f64).
    OperationTtfbSeconds(Duration),
    /// Update the current number of executing HTTP requests.
    /// Metrics impl: Add the value (positive or negative) to a Gauge.
    HttpExecuting(isize),
    /// Record the size of HTTP request body in bytes.
    /// Metrics impl: Update a Histogram with the given byte count.
    HttpRequestBytes(u64),
    /// Record the rate of HTTP request data in bytes/second.
    /// Metrics impl: Update a Histogram with the calculated rate value.
    HttpRequestBytesRate(f64),
    /// Record the duration of sending an HTTP request (until first byte received).
    /// Metrics impl: Update a Histogram with the duration converted to seconds (as f64).
    HttpRequestDurationSeconds(Duration),
    /// Record the size of HTTP response body in bytes.
    /// Metrics impl: Update a Histogram with the given byte count.
    HttpResponseBytes(u64),
    /// Record the rate of HTTP response data in bytes/second.
    /// Metrics impl: Update a Histogram with the calculated rate value.
    HttpResponseBytesRate(f64),
    /// Record the duration of receiving an HTTP response (from first byte to last).
    /// Metrics impl: Update a Histogram with the duration converted to seconds (as f64).
    HttpResponseDurationSeconds(Duration),
    /// Increment the counter for HTTP connection errors.
    /// Metrics impl: Increment a Counter by 1.
    HttpConnectionErrorsTotal,
    /// Increment the counter for HTTP status errors (non-2xx responses).
    /// Metrics impl: Increment a Counter by 1.
    HttpStatusErrorsTotal,
}

impl MetricValue {
    /// Returns the full metric name for this metric value.
    pub fn name(&self) -> &'static str {
        match self {
            MetricValue::OperationBytes(_) => "opendal_operation_bytes",
            MetricValue::OperationBytesRate(_) => "opendal_operation_bytes_rate",
            MetricValue::OperationEntries(_) => "opendal_operation_entries",
            MetricValue::OperationEntriesRate(_) => "opendal_operation_entries_rate",
            MetricValue::OperationDurationSeconds(_) => "opendal_operation_duration_seconds",
            MetricValue::OperationErrorsTotal => "opendal_operation_errors_total",
            MetricValue::OperationExecuting(_) => "opendal_operation_executing",
            MetricValue::OperationTtfbSeconds(_) => "opendal_operation_ttfb_seconds",

            MetricValue::HttpConnectionErrorsTotal => "opendal_http_connection_errors_total",
            MetricValue::HttpStatusErrorsTotal => "opendal_http_status_errors_total",
            MetricValue::HttpExecuting(_) => "opendal_http_executing",
            MetricValue::HttpRequestBytes(_) => "opendal_http_request_bytes",
            MetricValue::HttpRequestBytesRate(_) => "opendal_http_request_bytes_rate",
            MetricValue::HttpRequestDurationSeconds(_) => "opendal_http_request_duration_seconds",
            MetricValue::HttpResponseBytes(_) => "opendal_http_response_bytes",
            MetricValue::HttpResponseBytesRate(_) => "opendal_http_response_bytes_rate",
            MetricValue::HttpResponseDurationSeconds(_) => "opendal_http_response_duration_seconds",
        }
    }

    /// Returns the metric name along with unit for this metric value.
    ///
    /// # Notes
    ///
    /// This API is designed for the metrics impls that unit aware. They will handle the names by themselves like append `_total` for counters.
    pub fn name_with_unit(&self) -> (&'static str, Option<&'static str>) {
        match self {
            MetricValue::OperationBytes(_) => ("opendal_operation", Some("bytes")),
            MetricValue::OperationBytesRate(_) => ("opendal_operation_bytes_rate", None),
            MetricValue::OperationEntries(_) => ("opendal_operation_entries", None),
            MetricValue::OperationEntriesRate(_) => ("opendal_operation_entries_rate", None),
            MetricValue::OperationDurationSeconds(_) => {
                ("opendal_operation_duration", Some("seconds"))
            }
            MetricValue::OperationErrorsTotal => ("opendal_operation_errors", None),
            MetricValue::OperationExecuting(_) => ("opendal_operation_executing", None),
            MetricValue::OperationTtfbSeconds(_) => ("opendal_operation_ttfb", Some("seconds")),

            MetricValue::HttpConnectionErrorsTotal => ("opendal_http_connection_errors", None),
            MetricValue::HttpStatusErrorsTotal => ("opendal_http_status_errors", None),
            MetricValue::HttpExecuting(_) => ("opendal_http_executing", None),
            MetricValue::HttpRequestBytes(_) => ("opendal_http_request", Some("bytes")),
            MetricValue::HttpRequestBytesRate(_) => ("opendal_http_request_bytes_rate", None),
            MetricValue::HttpRequestDurationSeconds(_) => {
                ("opendal_http_request_duration", Some("seconds"))
            }
            MetricValue::HttpResponseBytes(_) => ("opendal_http_response", Some("bytes")),
            MetricValue::HttpResponseBytesRate(_) => ("opendal_http_response_bytes_rate", None),
            MetricValue::HttpResponseDurationSeconds(_) => {
                ("opendal_http_response_duration", Some("seconds"))
            }
        }
    }

    /// Returns the help text for this metric value.
    pub fn help(&self) -> &'static str {
        match self {
            MetricValue::OperationBytes(_) => "Current operation size in bytes, represents the size of data being processed in the current operation",
            MetricValue::OperationBytesRate(_) => "Histogram of data processing rates in bytes per second within individual operations",
            MetricValue::OperationEntries(_) => "Current operation size in entries, represents the entries being processed in the current operation",
            MetricValue::OperationEntriesRate(_) => "Histogram of entries processing rates in entries per second within individual operations",
            MetricValue::OperationDurationSeconds(_) => "Duration of operations in seconds, measured from start to completion",
            MetricValue::OperationErrorsTotal => "Total number of failed operations",
            MetricValue::OperationExecuting(_) => "Number of operations currently being executed",
            MetricValue::OperationTtfbSeconds(_) => "Time to first byte in seconds for operations",

            MetricValue::HttpConnectionErrorsTotal => "Total number of HTTP requests that failed before receiving a response (DNS failures, connection refused, timeouts, TLS errors)",
            MetricValue::HttpStatusErrorsTotal => "Total number of HTTP requests that received error status codes (non-2xx responses)",
            MetricValue::HttpExecuting(_) => "Number of HTTP requests currently in flight from this client",
            MetricValue::HttpRequestBytes(_) => "Histogram of HTTP request body sizes in bytes",
            MetricValue::HttpRequestBytesRate(_) => "Histogram of HTTP request bytes per second rates",
            MetricValue::HttpRequestDurationSeconds(_) => "Histogram of time durations in seconds spent sending HTTP requests, from first byte sent to receiving the first byte",
            MetricValue::HttpResponseBytes(_) => "Histogram of HTTP response body sizes in bytes",
            MetricValue::HttpResponseBytesRate(_) => "Histogram of HTTP response bytes per second rates",
            MetricValue::HttpResponseDurationSeconds(_) => "Histogram of time durations in seconds spent receiving HTTP responses, from first byte received to last byte received",
        }
    }
}

/// The interceptor for metrics.
///
/// All metrics related libs should implement this trait to observe opendal's internal operations.
pub trait MetricsIntercept: Debug + Clone + Send + Sync + Unpin + 'static {
    /// Observe the metric value.
    fn observe(&self, labels: MetricLabels, value: MetricValue) {
        let _ = (labels, value);
    }
}

/// The metrics layer for opendal.
#[derive(Clone, Debug)]
pub struct MetricsLayer<I: MetricsIntercept> {
    interceptor: I,
}

impl<I: MetricsIntercept> MetricsLayer<I> {
    /// Create a new metrics layer.
    pub fn new(interceptor: I) -> Self {
        Self { interceptor }
    }
}

impl<A: Access, I: MetricsIntercept> Layer<A> for MetricsLayer<I> {
    type LayeredAccess = MetricsAccessor<A, I>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();

        // Update http client with metrics http fetcher.
        info.update_http_client(|client| {
            HttpClient::with(MetricsHttpFetcher {
                inner: client.into_inner(),
                info: info.clone(),
                interceptor: self.interceptor.clone(),
            })
        });

        MetricsAccessor {
            inner,
            info,
            interceptor: self.interceptor.clone(),
        }
    }
}

/// The metrics http fetcher for opendal.
pub struct MetricsHttpFetcher<I: MetricsIntercept> {
    inner: HttpFetcher,
    info: Arc<AccessorInfo>,
    interceptor: I,
}

impl<I: MetricsIntercept> HttpFetch for MetricsHttpFetcher<I> {
    async fn fetch(&self, req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let labels = MetricLabels::new(
            self.info.clone(),
            req.extensions()
                .get::<Operation>()
                .copied()
                .map(Operation::into_static)
                .unwrap_or("unknown"),
        );

        let start = Instant::now();
        let req_size = req.body().len();

        self.interceptor
            .observe(labels.clone(), MetricValue::HttpExecuting(1));

        let res = self.inner.fetch(req).await;
        let req_duration = start.elapsed();

        match res {
            Err(err) => {
                self.interceptor
                    .observe(labels.clone(), MetricValue::HttpExecuting(-1));
                self.interceptor
                    .observe(labels, MetricValue::HttpConnectionErrorsTotal);
                Err(err)
            }
            Ok(resp) if resp.status().is_client_error() && resp.status().is_server_error() => {
                self.interceptor
                    .observe(labels.clone(), MetricValue::HttpExecuting(-1));
                self.interceptor.observe(
                    labels.with_status_code(resp.status()),
                    MetricValue::HttpStatusErrorsTotal,
                );
                Ok(resp)
            }
            Ok(resp) => {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::HttpRequestBytes(req_size as u64),
                );
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::HttpRequestBytesRate(req_size as f64 / req_duration.as_secs_f64()),
                );
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::HttpRequestDurationSeconds(req_duration),
                );

                let (parts, body) = resp.into_parts();
                let body = body.map_inner(|s| {
                    Box::new(MetricsStream {
                        inner: s,
                        interceptor: self.interceptor.clone(),
                        labels: labels.clone(),
                        size: 0,
                        start: Instant::now(),
                    })
                });

                Ok(http::Response::from_parts(parts, body))
            }
        }
    }
}

pub struct MetricsStream<S, I> {
    inner: S,
    interceptor: I,

    labels: MetricLabels,
    size: u64,
    start: Instant,
}

impl<S, I> Stream for MetricsStream<S, I>
where
    S: Stream<Item = Result<Buffer>> + Unpin + 'static,
    I: MetricsIntercept,
{
    type Item = Result<Buffer>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(bs)) => {
                self.size += bs.len() as u64;
                Poll::Ready(Some(Ok(bs)))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => {
                let resp_size = self.size;
                let resp_duration = self.start.elapsed();

                self.interceptor.observe(
                    self.labels.clone(),
                    MetricValue::HttpResponseBytes(resp_size),
                );
                self.interceptor.observe(
                    self.labels.clone(),
                    MetricValue::HttpResponseBytesRate(
                        resp_size as f64 / resp_duration.as_secs_f64(),
                    ),
                );
                self.interceptor.observe(
                    self.labels.clone(),
                    MetricValue::HttpResponseDurationSeconds(resp_duration),
                );
                self.interceptor
                    .observe(self.labels.clone(), MetricValue::HttpExecuting(-1));

                Poll::Ready(None)
            }
        }
    }
}

/// The metrics accessor for opendal.
pub struct MetricsAccessor<A: Access, I: MetricsIntercept> {
    inner: A,
    info: Arc<AccessorInfo>,
    interceptor: I,
}

impl<A: Access, I: MetricsIntercept> Debug for MetricsAccessor<A, I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access, I: MetricsIntercept> LayeredAccess for MetricsAccessor<A, I> {
    type Inner = A;
    type Reader = MetricsWrapper<A::Reader, I>;
    type BlockingReader = MetricsWrapper<A::BlockingReader, I>;
    type Writer = MetricsWrapper<A::Writer, I>;
    type BlockingWriter = MetricsWrapper<A::BlockingWriter, I>;
    type Lister = MetricsWrapper<A::Lister, I>;
    type BlockingLister = MetricsWrapper<A::BlockingLister, I>;
    type Deleter = MetricsWrapper<A::Deleter, I>;
    type BlockingDeleter = MetricsWrapper<A::BlockingDeleter, I>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let labels = MetricLabels::new(self.info.clone(), Operation::CreateDir.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .create_dir(path, args)
            .await
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Read.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let (rp, reader) = self
            .inner
            .read(path, args)
            .await
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationTtfbSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })?;

        Ok((
            rp,
            MetricsWrapper::new(reader, self.interceptor.clone(), labels, start),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Write.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let (rp, writer) = self.inner.write(path, args).await.inspect_err(|err| {
            self.interceptor.observe(
                labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })?;

        Ok((
            rp,
            MetricsWrapper::new(writer, self.interceptor.clone(), labels, start),
        ))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Copy.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .copy(from, to, args)
            .await
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Rename.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .rename(from, to, args)
            .await
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Stat.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .stat(path, args)
            .await
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Delete.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let (rp, deleter) = self.inner.delete().await.inspect_err(|err| {
            self.interceptor.observe(
                labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })?;

        Ok((
            rp,
            MetricsWrapper::new(deleter, self.interceptor.clone(), labels, start),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::List.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let (rp, lister) = self.inner.list(path, args).await.inspect_err(|err| {
            self.interceptor.observe(
                labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })?;

        Ok((
            rp,
            MetricsWrapper::new(lister, self.interceptor.clone(), labels, start),
        ))
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Presign.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .presign(path, args)
            .await
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let labels = MetricLabels::new(self.info.clone(), Operation::CreateDir.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .blocking_create_dir(path, args)
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Read.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let (rp, reader) = self
            .inner
            .blocking_read(path, args)
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationTtfbSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })?;

        Ok((
            rp,
            MetricsWrapper::new(reader, self.interceptor.clone(), labels, start),
        ))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Write.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let (rp, writer) = self.inner.blocking_write(path, args).inspect_err(|err| {
            self.interceptor.observe(
                labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })?;

        Ok((
            rp,
            MetricsWrapper::new(writer, self.interceptor.clone(), labels, start),
        ))
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Copy.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .blocking_copy(from, to, args)
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Rename.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .blocking_rename(from, to, args)
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Stat.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let res = self
            .inner()
            .blocking_stat(path, args)
            .inspect(|_| {
                self.interceptor.observe(
                    labels.clone(),
                    MetricValue::OperationDurationSeconds(start.elapsed()),
                );
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            });

        self.interceptor
            .observe(labels, MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Delete.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let (rp, deleter) = self.inner.blocking_delete().inspect_err(|err| {
            self.interceptor.observe(
                labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })?;

        Ok((
            rp,
            MetricsWrapper::new(deleter, self.interceptor.clone(), labels, start),
        ))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::List.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));

        let (rp, lister) = self.inner.blocking_list(path, args).inspect_err(|err| {
            self.interceptor.observe(
                labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })?;

        Ok((
            rp,
            MetricsWrapper::new(lister, self.interceptor.clone(), labels, start),
        ))
    }
}

pub struct MetricsWrapper<R, I: MetricsIntercept> {
    inner: R,
    interceptor: I,
    labels: MetricLabels,

    start: Instant,
    size: u64,
}

impl<R, I: MetricsIntercept> Drop for MetricsWrapper<R, I> {
    fn drop(&mut self) {
        let size = self.size;
        let duration = self.start.elapsed();

        if self.labels.operation == Operation::Read.into_static()
            || self.labels.operation == Operation::Write.into_static()
        {
            self.interceptor
                .observe(self.labels.clone(), MetricValue::OperationBytes(self.size));
            self.interceptor.observe(
                self.labels.clone(),
                MetricValue::OperationBytesRate(size as f64 / duration.as_secs_f64()),
            );
        } else {
            self.interceptor.observe(
                self.labels.clone(),
                MetricValue::OperationEntries(self.size),
            );
            self.interceptor.observe(
                self.labels.clone(),
                MetricValue::OperationEntriesRate(size as f64 / duration.as_secs_f64()),
            );
        }

        self.interceptor.observe(
            self.labels.clone(),
            MetricValue::OperationDurationSeconds(duration),
        );
        self.interceptor
            .observe(self.labels.clone(), MetricValue::OperationExecuting(-1));
    }
}

impl<R, I: MetricsIntercept> MetricsWrapper<R, I> {
    fn new(inner: R, interceptor: I, labels: MetricLabels, start: Instant) -> Self {
        Self {
            inner,
            interceptor,
            labels,
            start,
            size: 0,
        }
    }
}

impl<R: oio::Read, I: MetricsIntercept> oio::Read for MetricsWrapper<R, I> {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner
            .read()
            .await
            .inspect(|bs| {
                self.size += bs.len() as u64;
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    self.labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })
    }
}

impl<R: oio::BlockingRead, I: MetricsIntercept> oio::BlockingRead for MetricsWrapper<R, I> {
    fn read(&mut self) -> Result<Buffer> {
        self.inner
            .read()
            .inspect(|bs| {
                self.size += bs.len() as u64;
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    self.labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })
    }
}

impl<R: oio::Write, I: MetricsIntercept> oio::Write for MetricsWrapper<R, I> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len();

        self.inner
            .write(bs)
            .await
            .inspect(|_| {
                self.size += size as u64;
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    self.labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await.inspect_err(|err| {
            self.interceptor.observe(
                self.labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await.inspect_err(|err| {
            self.interceptor.observe(
                self.labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })
    }
}

impl<R: oio::BlockingWrite, I: MetricsIntercept> oio::BlockingWrite for MetricsWrapper<R, I> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len();

        self.inner
            .write(bs)
            .inspect(|_| {
                self.size += size as u64;
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    self.labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })
    }

    fn close(&mut self) -> Result<Metadata> {
        self.inner.close().inspect_err(|err| {
            self.interceptor.observe(
                self.labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })
    }
}

impl<R: oio::List, I: MetricsIntercept> oio::List for MetricsWrapper<R, I> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner
            .next()
            .await
            .inspect(|_| {
                self.size += 1;
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    self.labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })
    }
}

impl<R: oio::BlockingList, I: MetricsIntercept> oio::BlockingList for MetricsWrapper<R, I> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner
            .next()
            .inspect(|_| {
                self.size += 1;
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    self.labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })
    }
}

impl<R: oio::Delete, I: MetricsIntercept> oio::Delete for MetricsWrapper<R, I> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner
            .delete(path, args)
            .inspect(|_| {
                self.size += 1;
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    self.labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })
    }

    async fn flush(&mut self) -> Result<usize> {
        self.inner.flush().await.inspect_err(|err| {
            self.interceptor.observe(
                self.labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })
    }
}

impl<R: oio::BlockingDelete, I: MetricsIntercept> oio::BlockingDelete for MetricsWrapper<R, I> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner
            .delete(path, args)
            .inspect(|_| {
                self.size += 1;
            })
            .inspect_err(|err| {
                self.interceptor.observe(
                    self.labels.clone().with_error(err.kind()),
                    MetricValue::OperationErrorsTotal,
                );
            })
    }

    fn flush(&mut self) -> Result<usize> {
        self.inner.flush().inspect_err(|err| {
            self.interceptor.observe(
                self.labels.clone().with_error(err.kind()),
                MetricValue::OperationErrorsTotal,
            );
        })
    }
}
