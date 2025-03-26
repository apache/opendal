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
use std::fmt::Write;
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
/// The metrics label for the path. (will be removed)
pub static LABEL_PATH: &str = "path";
/// The metric label for the http code.
pub static LABEL_STATUS_CODE: &str = "status_code";

/// MetricLabels are the labels for the metrics.
#[derive(Default, Debug, Clone)]
pub struct MetricLabels {
    pub scheme: Scheme,
    pub namespace: Arc<str>,
    pub root: Arc<str>,
    pub operation: &'static str,

    /// Only available for `OperationErrorsTotal`
    pub error: Option<ErrorKind>,
    /// Only available for `HttpStatusErrorsTotal`
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
/// Metrcis impls can be `prometheus_client`, `metrics` etc.
///
/// Every metrics impls SHOULD implement observe over the MetricValue to make
/// sure they provides the consistent metrics for users.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum MetricValue {
    OperationBytes(u64),
    OperationBytesRate(f64),
    OperationBytesTotal(u64),
    OperationCountTotal,
    OperationDurationSeconds(Duration),
    OperationErrorsTotal,
    OperationExecuting(isize),
    OperationTtfbSeconds(Duration),

    HttpCountTotal,
    HttpConnectionErrorsTotal,
    HttpExecuting(isize),
    HttpRequestBytes(u64),
    HttpRequestBytesRate(f64),
    HttpRequestBytesTotal(u64),
    HttpRequestDurationSeconds(Duration),
    HttpResponseBytes(u64),
    HttpResponseBytesRate(f64),
    HttpResponseBytesTotal(u64),
    HttpResponseDurationSeconds(Duration),
    HttpStatusErrorsTotal,
}

impl MetricValue {
    /// Returns the related info of this metric value in the format `(name, help)`.
    pub fn info(&self) -> (&'static str, &'static str) {
        match self {
            MetricValue::OperationBytes(_) => ("opendal_operation_bytes", "Current operation size in bytes, represents the size of data being processed in the current operation"),
            MetricValue::OperationBytesRate(_) => ("opendal_operation_bytes_rate", "Histogram of data processing rates in bytes per second within individual operations"),
            MetricValue::OperationBytesTotal(_) => ("opendal_operation_bytes_total", "Total number of bytes processed by operations"),
            MetricValue::OperationCountTotal => ("opendal_operation_count_total","Total number of operations completed"),
            MetricValue::OperationDurationSeconds(_) => ("opendal_operation_duration_seconds","Duration of operations in seconds, measured from start to completion"),
            MetricValue::OperationErrorsTotal => ("opendal_operation_errors_total","Total number of failed operations"),
            MetricValue::OperationExecuting(_) => ("opendal_operation_executing","Number of operations currently being executed"),
            MetricValue::OperationTtfbSeconds(_) => ("opendal_operation_ttfb_seconds","Time to first byte in seconds for operations"),

            MetricValue::HttpCountTotal => ("opendal_http_count_total","Total number of HTTP requests initiated"),
            MetricValue::HttpConnectionErrorsTotal => ("opendal_http_connection_errors_total","Total number of HTTP requests that failed before receiving a response (DNS failures, connection refused, timeouts, TLS errors)"),
            MetricValue::HttpStatusErrorsTotal => ("opendal_http_connection_errors_total","Total number of HTTP requests that received error status codes (non-2xx responses)"),
            MetricValue::HttpExecuting(_) => ("opendal_http_executing","Number of HTTP requests currently in flight from this client"),
            MetricValue::HttpRequestBytes(_) => ("opendal_http_request_bytes","Histogram of HTTP request body sizes in bytes"),
            MetricValue::HttpRequestBytesRate(_) => ("opendal_http_request_bytes_rate","Histogram of HTTP request bytes per second rates"),
            MetricValue::HttpRequestBytesTotal(_) =>( "opendal_http_request_bytes_total","Total number of bytes sent in HTTP request bodies"),
            MetricValue::HttpRequestDurationSeconds(_) => ("opendal_http_request_duration_seconds","Histogram of time durations in seconds spent sending HTTP requests, from first byte sent to receiving the first byte"),
            MetricValue::HttpResponseBytes(_) => ("opendal_http_response_bytes","Histogram of HTTP response body sizes in bytes"),
            MetricValue::HttpResponseBytesRate(_) => ("opendal_http_response_bytes_rate","Histogram of HTTP response bytes per second rates"),
            MetricValue::HttpResponseBytesTotal(_) =>( "opendal_http_response_bytes_total","Total number of bytes received in HTTP response bodies"),
            MetricValue::HttpResponseDurationSeconds(_) => ("opendal_http_response_duration_seconds","Histogram of time durations in seconds spent receiving HTTP responses, from first byte received to last byte received"),
        }
    }
}

/// The metric metadata which contains the metric name and help.
pub struct MetricMetadata {
    name: &'static str,
    help: &'static str,
}

impl MetricMetadata {
    /// Returns the metric name.
    ///
    /// We default to using the metric name with the prefix `opendal_`.
    pub fn name(&self) -> String {
        self.name_with_prefix("opendal_".to_string())
    }

    /// Returns the metric name with a given prefix.
    pub fn name_with_prefix(&self, mut prefix: String) -> String {
        // This operation must succeed. If an error does occur, let's just ignore it.
        let _ = prefix.write_str(self.name);
        prefix
    }

    /// Returns the metric help.
    pub fn help(&self) -> &'static str {
        self.help
    }
}

/// The metric metadata for the operation duration in seconds.
pub static METRIC_OPERATION_DURATION_SECONDS: MetricMetadata = MetricMetadata {
    name: "operation_duration_seconds",
    help: "Histogram of time spent during opendal operations",
};
/// The metric metadata for the operation bytes.
pub static METRIC_OPERATION_BYTES: MetricMetadata = MetricMetadata {
    name: "operation_bytes",
    help: "Histogram of the bytes transferred during opendal operations",
};
/// The metric metadata for the operation errors total.
pub static METRIC_OPERATION_ERRORS_TOTAL: MetricMetadata = MetricMetadata {
    name: "operation_errors_total",
    help: "Error counter during opendal operations",
};
/// The metric metadata for the http request duration in seconds.
pub static METRIC_HTTP_REQUEST_DURATION_SECONDS: MetricMetadata = MetricMetadata {
    name: "http_request_duration_seconds",
    help: "Histogram of time spent during http requests",
};
/// The metric metadata for the http request bytes.
pub static METRIC_HTTP_REQUEST_BYTES: MetricMetadata = MetricMetadata {
    name: "http_request_bytes",
    help: "Histogram of the bytes transferred during http requests",
};

/// The interceptor for metrics.
///
/// All metrics related libs should implement this trait to observe opendal's internal operations.
pub trait MetricsIntercept: Debug + Clone + Send + Sync + Unpin + 'static {
    /// Observe the metric value.
    fn observe(&self, labels: MetricLabels, value: MetricValue) {
        let _ = (labels, value);
    }

    /// Observe the operation duration in seconds.
    fn observe_operation_duration_seconds(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        duration: Duration,
    ) {
        let _ = (info, path, op, duration);
    }

    /// Observe the operation bytes happened in IO like read and write.
    fn observe_operation_bytes(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        bytes: usize,
    ) {
        let _ = (info, path, op, bytes);
    }

    /// Observe the operation errors total.
    fn observe_operation_errors_total(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        error: ErrorKind,
    ) {
        let _ = (info, path, op, error);
    }

    /// Observe the http request duration in seconds.
    fn observe_http_request_duration_seconds(
        &self,
        info: Arc<AccessorInfo>,
        op: Operation,
        duration: Duration,
    ) {
        let _ = (info, op, duration);
    }

    /// Observe the operation bytes happened in http request like read and write.
    fn observe_http_request_bytes(&self, info: Arc<AccessorInfo>, op: Operation, bytes: usize) {
        let _ = (info, op, bytes);
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

        self.interceptor
            .observe(labels.clone(), MetricValue::HttpCountTotal);
        self.interceptor
            .observe(labels.clone(), MetricValue::HttpExecuting(1));

        let start = Instant::now();
        let req_size = req.body().len();

        let res = self.inner.fetch(req).await;
        let req_duration = start.elapsed();

        match res {
            Err(err) => {
                self.interceptor
                    .observe(labels.clone(), MetricValue::HttpExecuting(-1));
                self.interceptor
                    .observe(labels.clone(), MetricValue::HttpConnectionErrorsTotal);
                Err(err)
            }
            Ok(resp) if resp.status().is_client_error() && resp.status().is_server_error() => {
                self.interceptor
                    .observe(labels.clone(), MetricValue::HttpExecuting(-1));

                self.interceptor.observe(
                    labels.clone().with_status_code(resp.status()),
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
                    MetricValue::HttpRequestBytesTotal(req_size as u64),
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
    S: Stream<Item = Result<Buffer>> + Send + Sync + Unpin + 'static,
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
                    MetricValue::HttpResponseBytesTotal(resp_size),
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
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
            .observe(labels.clone(), MetricValue::OperationExecuting(-1));
        res
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::ReaderRead.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
        let labels = MetricLabels::new(self.info.clone(), Operation::WriterStart.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
            .observe(labels.clone(), MetricValue::OperationExecuting(-1));
        res
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Rename.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
            .observe(labels.clone(), MetricValue::OperationExecuting(-1));
        res
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Stat.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
            .observe(labels.clone(), MetricValue::OperationExecuting(-1));
        res
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::DeleterStart.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

        let (rp, deleter) = self
            .inner
            .delete()
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
            })?;

        Ok((
            rp,
            MetricsWrapper::new(deleter, self.interceptor.clone(), labels, start),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::ListerNext.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

        let (rp, lister) = self
            .inner
            .list(path, args)
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
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
            .observe(labels.clone(), MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let labels = MetricLabels::new(self.info.clone(), Operation::CreateDir.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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

        res
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::ReaderStart.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
        let labels = MetricLabels::new(self.info.clone(), Operation::WriterWrite.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
            .observe(labels.clone(), MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Rename.into_static());

        let start = Instant::now();
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
            .observe(labels.clone(), MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let labels = MetricLabels::new(self.info.clone(), Operation::Stat.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

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
            .observe(labels.clone(), MetricValue::OperationExecuting(-1));
        res
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::DeleterStart.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

        let (rp, deleter) = self
            .inner
            .blocking_delete()
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
            })?;

        Ok((
            rp,
            MetricsWrapper::new(deleter, self.interceptor.clone(), labels, start),
        ))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let labels = MetricLabels::new(self.info.clone(), Operation::ListerNext.into_static());

        let start = Instant::now();

        self.interceptor
            .observe(labels.clone(), MetricValue::OperationExecuting(1));
        self.interceptor
            .observe(labels.clone(), MetricValue::OperationCountTotal);

        let (rp, lister) = self
            .inner
            .blocking_list(path, args)
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

        self.interceptor
            .observe(self.labels.clone(), MetricValue::OperationBytes(self.size));
        self.interceptor.observe(
            self.labels.clone(),
            MetricValue::OperationBytesRate(size as f64 / duration.as_secs_f64()),
        );
        self.interceptor
            .observe(self.labels.clone(), MetricValue::OperationBytesTotal(size));
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
