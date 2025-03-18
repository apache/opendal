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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::raw::*;
use crate::*;

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

/// The metric label for the scheme like s3, fs, cos.
pub static LABEL_SCHEME: &str = "scheme";
/// The metric label for the namespace like bucket name in s3.
pub static LABEL_NAMESPACE: &str = "namespace";
/// The metric label for the root path.
pub static LABEL_ROOT: &str = "root";
/// The metric label for the path used by request.
pub static LABEL_PATH: &str = "path";
/// The metric label for the operation like read, write, list.
pub static LABEL_OPERATION: &str = "operation";
/// The metric label for the error kind.
pub static LABEL_ERROR: &str = "error";

/// The interceptor for metrics.
///
/// All metrics related libs should implement this trait to observe opendal's internal operations.
pub trait MetricsIntercept: Debug + Clone + Send + Sync + Unpin + 'static {
    /// Observe the operation duration in seconds.
    fn observe_operation_duration_seconds(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        duration: Duration,
    );

    /// Observe the operation bytes happened in IO like read and write.
    fn observe_operation_bytes(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        bytes: usize,
    );

    /// Observe the operation errors total.
    fn observe_operation_errors_total(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        error: ErrorKind,
    );

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
        // Extract context from the http request.
        let size = req.body().len();
        let op = req.extensions().get::<Operation>().copied();

        let start = Instant::now();
        let res = self.inner.fetch(req).await;
        let duration = start.elapsed();

        // We only inject the metrics when the operation exists.
        if let Some(op) = op {
            if res.is_ok() {
                self.interceptor.observe_http_request_duration_seconds(
                    self.info.clone(),
                    op,
                    duration,
                );
                self.interceptor
                    .observe_http_request_bytes(self.info.clone(), op, size);
            }
        }

        res
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
        let op = Operation::CreateDir;

        let start = Instant::now();
        self.inner()
            .create_dir(path, args)
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(move |err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let op = Operation::ReaderStart;

        let start = Instant::now();
        let (rp, reader) = self
            .inner
            .read(path, args)
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })?;

        Ok((
            rp,
            MetricsWrapper::new(
                reader,
                self.interceptor.clone(),
                self.info.clone(),
                path.to_string(),
            ),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let op = Operation::WriterStart;

        let start = Instant::now();
        let (rp, writer) = self
            .inner
            .write(path, args)
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })?;

        Ok((
            rp,
            MetricsWrapper::new(
                writer,
                self.interceptor.clone(),
                self.info.clone(),
                path.to_string(),
            ),
        ))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let op = Operation::Copy;

        let start = Instant::now();
        self.inner()
            .copy(from, to, args)
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    from,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(move |err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    from,
                    op,
                    err.kind(),
                );
                err
            })
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let op = Operation::Rename;

        let start = Instant::now();
        self.inner()
            .rename(from, to, args)
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    from,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(move |err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    from,
                    op,
                    err.kind(),
                );
                err
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let op = Operation::Stat;

        let start = Instant::now();
        self.inner()
            .stat(path, args)
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(move |err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let op = Operation::DeleterStart;

        let start = Instant::now();
        let (rp, writer) = self
            .inner
            .delete()
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    "",
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    "",
                    op,
                    err.kind(),
                );
                err
            })?;

        Ok((
            rp,
            MetricsWrapper::new(
                writer,
                self.interceptor.clone(),
                self.info.clone(),
                "".to_string(),
            ),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let op = Operation::ListerStart;

        let start = Instant::now();
        let (rp, lister) = self
            .inner
            .list(path, args)
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })?;

        Ok((
            rp,
            MetricsWrapper::new(
                lister,
                self.interceptor.clone(),
                self.info.clone(),
                path.to_string(),
            ),
        ))
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let op = Operation::Presign;

        let start = Instant::now();
        self.inner()
            .presign(path, args)
            .await
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(move |err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let op = Operation::CreateDir;

        let start = Instant::now();
        self.inner()
            .blocking_create_dir(path, args)
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(move |err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let op = Operation::ReaderStart;

        let start = Instant::now();
        let (rp, reader) = self
            .inner
            .blocking_read(path, args)
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })?;

        Ok((
            rp,
            MetricsWrapper::new(
                reader,
                self.interceptor.clone(),
                self.info.clone(),
                path.to_string(),
            ),
        ))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let op = Operation::WriterStart;

        let start = Instant::now();
        let (rp, writer) = self
            .inner
            .blocking_write(path, args)
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })?;

        Ok((
            rp,
            MetricsWrapper::new(
                writer,
                self.interceptor.clone(),
                self.info.clone(),
                path.to_string(),
            ),
        ))
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let op = Operation::Copy;

        let start = Instant::now();
        self.inner()
            .blocking_copy(from, to, args)
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    from,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(move |err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    from,
                    op,
                    err.kind(),
                );
                err
            })
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let op = Operation::Rename;

        let start = Instant::now();
        self.inner()
            .blocking_rename(from, to, args)
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    from,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    from,
                    op,
                    err.kind(),
                );
                err
            })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let op = Operation::Stat;

        let start = Instant::now();
        self.inner()
            .blocking_stat(path, args)
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(move |err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        let op = Operation::DeleterStart;

        let start = Instant::now();
        let (rp, writer) = self
            .inner
            .blocking_delete()
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    "",
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    "",
                    op,
                    err.kind(),
                );
                err
            })?;

        Ok((
            rp,
            MetricsWrapper::new(
                writer,
                self.interceptor.clone(),
                self.info.clone(),
                "".to_string(),
            ),
        ))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let op = Operation::ListerStart;

        let start = Instant::now();
        let (rp, lister) = self
            .inner
            .blocking_list(path, args)
            .map(|v| {
                self.interceptor.observe_operation_duration_seconds(
                    self.info.clone(),
                    path,
                    op,
                    start.elapsed(),
                );
                v
            })
            .map_err(|err| {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    path,
                    op,
                    err.kind(),
                );
                err
            })?;

        Ok((
            rp,
            MetricsWrapper::new(
                lister,
                self.interceptor.clone(),
                self.info.clone(),
                path.to_string(),
            ),
        ))
    }
}

pub struct MetricsWrapper<R, I: MetricsIntercept> {
    inner: R,
    interceptor: I,
    info: Arc<AccessorInfo>,

    path: String,
}

impl<R, I: MetricsIntercept> MetricsWrapper<R, I> {
    fn new(inner: R, interceptor: I, info: Arc<AccessorInfo>, path: String) -> Self {
        Self {
            inner,
            interceptor,
            info,
            path,
        }
    }
}

impl<R: oio::Read, I: MetricsIntercept> oio::Read for MetricsWrapper<R, I> {
    async fn read(&mut self) -> Result<Buffer> {
        let op = Operation::ReaderRead;

        let start = Instant::now();

        let res = match self.inner.read().await {
            Ok(bs) => {
                self.interceptor.observe_operation_bytes(
                    self.info.clone(),
                    &self.path,
                    op,
                    bs.len(),
                );
                Ok(bs)
            }
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}

impl<R: oio::BlockingRead, I: MetricsIntercept> oio::BlockingRead for MetricsWrapper<R, I> {
    fn read(&mut self) -> Result<Buffer> {
        let op = Operation::ReaderRead;

        let start = Instant::now();

        let res = match self.inner.read() {
            Ok(bs) => {
                self.interceptor.observe_operation_bytes(
                    self.info.clone(),
                    &self.path,
                    op,
                    bs.len(),
                );
                Ok(bs)
            }
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}

impl<R: oio::Write, I: MetricsIntercept> oio::Write for MetricsWrapper<R, I> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let op = Operation::WriterWrite;

        let start = Instant::now();
        let size = bs.len();

        let res = match self.inner.write(bs).await {
            Ok(()) => {
                self.interceptor
                    .observe_operation_bytes(self.info.clone(), &self.path, op, size);
                Ok(())
            }
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }

    async fn close(&mut self) -> Result<Metadata> {
        let op = Operation::WriterClose;

        let start = Instant::now();

        let res = match self.inner.close().await {
            Ok(meta) => Ok(meta),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }

    async fn abort(&mut self) -> Result<()> {
        let op = Operation::WriterAbort;

        let start = Instant::now();

        let res = match self.inner.abort().await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}

impl<R: oio::BlockingWrite, I: MetricsIntercept> oio::BlockingWrite for MetricsWrapper<R, I> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        let op = Operation::WriterWrite;

        let start = Instant::now();
        let size = bs.len();

        let res = match self.inner.write(bs) {
            Ok(()) => {
                self.interceptor
                    .observe_operation_bytes(self.info.clone(), &self.path, op, size);
                Ok(())
            }
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }

    fn close(&mut self) -> Result<Metadata> {
        let op = Operation::WriterClose;

        let start = Instant::now();

        let res = match self.inner.close() {
            Ok(meta) => Ok(meta),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}

impl<R: oio::List, I: MetricsIntercept> oio::List for MetricsWrapper<R, I> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let op = Operation::ListerNext;

        let start = Instant::now();

        let res = match self.inner.next().await {
            Ok(entry) => Ok(entry),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}

impl<R: oio::BlockingList, I: MetricsIntercept> oio::BlockingList for MetricsWrapper<R, I> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let op = Operation::ListerNext;

        let start = Instant::now();

        let res = match self.inner.next() {
            Ok(entry) => Ok(entry),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}

impl<R: oio::Delete, I: MetricsIntercept> oio::Delete for MetricsWrapper<R, I> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let op = Operation::DeleterDelete;

        let start = Instant::now();

        let res = match self.inner.delete(path, args) {
            Ok(entry) => Ok(entry),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }

    async fn flush(&mut self) -> Result<usize> {
        let op = Operation::DeleterFlush;

        let start = Instant::now();

        let res = match self.inner.flush().await {
            Ok(entry) => Ok(entry),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}

impl<R: oio::BlockingDelete, I: MetricsIntercept> oio::BlockingDelete for MetricsWrapper<R, I> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let op = Operation::DeleterDelete;

        let start = Instant::now();

        let res = match self.inner.delete(path, args) {
            Ok(entry) => Ok(entry),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }

    fn flush(&mut self) -> Result<usize> {
        let op = Operation::DeleterFlush;

        let start = Instant::now();

        let res = match self.inner.flush() {
            Ok(entry) => Ok(entry),
            Err(err) => {
                self.interceptor.observe_operation_errors_total(
                    self.info.clone(),
                    &self.path,
                    op,
                    err.kind(),
                );
                Err(err)
            }
        };
        self.interceptor.observe_operation_duration_seconds(
            self.info.clone(),
            &self.path,
            op,
            start.elapsed(),
        );
        res
    }
}
