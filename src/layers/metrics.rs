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
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use async_trait::async_trait;
use futures::AsyncRead;
use metrics::counter;
use metrics::histogram;
use metrics::increment_counter;

use crate::multipart::ObjectPart;
use crate::ops::OpAbortMultipart;
use crate::ops::OpCompleteMultipart;
use crate::ops::OpCreate;
use crate::ops::OpCreateMultipart;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::OpWriteMultipart;
use crate::ops::Operation;
use crate::ops::PresignedRequest;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::DirIterator;
use crate::DirStreamer;
use crate::Layer;
use crate::ObjectMetadata;
use crate::Scheme;

static METRIC_REQUESTS_TOTAL: &str = "opendal_requests_total";
static METRIC_REQUESTS_DURATION_SECONDS: &str = "opendal_requests_duration_seconds";
/// Metrics of Failed requests, poll ok but outcome is error
static METRIC_FAILURES_TOTAL: &str = "opendal_failures_total";
/// Metrics of Errored requests, poll error
static METRIC_ERRORS_TOTAL: &str = "opendal_errors_total";
static METRIC_BYTES_READ_TOTAL: &str = "opendal_bytes_read_total";
static METRIC_BYTES_WRITTEN_TOTAL: &str = "opendal_bytes_written_total";

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

        Arc::new(MetricsAccessor { meta, inner })
    }
}

#[derive(Debug)]
struct MetricsAccessor {
    meta: AccessorMetadata,
    inner: Arc<dyn Accessor>,
}

#[async_trait]
impl Accessor for MetricsAccessor {
    fn metadata(&self) -> AccessorMetadata {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Metadata.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.metadata();
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Metadata.into_static(),
        );

        result
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Create.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.create(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Create.into_static(),
        );

        result
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Read.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.read(path, args).await.map(|reader| {
            Box::new(MetricReader::new(
                self.meta.scheme(),
                Operation::Read,
                reader,
            )) as BytesReader
        });
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Read.into_static(),
        );

        result
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Write.into_static(),
        );

        let r = Box::new(MetricReader::new(self.meta.scheme(), Operation::Write, r));

        let start = Instant::now();
        let result = self.inner.write(path, args, r).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Write.into_static(),
        );

        result
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Stat.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.stat(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Stat.into_static(),
        );

        result
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Delete.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.delete(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Delete.into_static(),
        );

        result
    }

    async fn list(&self, path: &str, args: OpList) -> Result<DirStreamer> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::List.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.list(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::List.into_static(),
        );

        result
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<PresignedRequest> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Presign.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.presign(path, args);
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Presign.into_static(),
        );

        result
    }

    async fn create_multipart(&self, path: &str, args: OpCreateMultipart) -> Result<String> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::CreateMultipart.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.create_multipart(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::CreateMultipart.into_static(),
        );

        result
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<ObjectPart> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::WriteMultipart.into_static(),
        );

        let r = Box::new(MetricReader::new(self.meta.scheme(), Operation::Write, r));

        let start = Instant::now();
        let result = self.inner.write_multipart(path, args, r).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::WriteMultipart.into_static(),
        );

        result
    }

    async fn complete_multipart(&self, path: &str, args: OpCompleteMultipart) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::CompleteMultipart.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.complete_multipart(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::CompleteMultipart.into_static(),
        );

        result
    }

    async fn abort_multipart(&self, path: &str, args: OpAbortMultipart) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::AbortMultipart.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.abort_multipart(path, args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::AbortMultipart.into_static(),
        );

        result
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingCreate.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.blocking_create(path, args);
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingCreate.into_static(),
        );

        result
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingRead.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.blocking_read(path, args).map(|reader| {
            Box::new(BlockingMetricReader::new(
                self.meta.scheme(),
                Operation::BlockingRead,
                reader,
            )) as BlockingBytesReader
        });
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingRead.into_static(),
        );

        result
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingWrite.into_static(),
        );

        let r = Box::new(BlockingMetricReader::new(
            self.meta.scheme(),
            Operation::BlockingWrite,
            r,
        ));

        let start = Instant::now();
        let result = self.inner.blocking_write(path, args, r);
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingWrite.into_static(),
        );

        result
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingStat.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.blocking_stat(path, args);
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingStat.into_static(),
        );

        result
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingDelete.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.blocking_delete(path, args);
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingDelete.into_static(),
        );

        result
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<DirIterator> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingList.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.blocking_list(path, args);
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::BlockingList.into_static(),
        );

        result
    }
}

struct MetricReader {
    scheme: Scheme,
    op: Operation,
    inner: BytesReader,
}

impl MetricReader {
    fn new(scheme: Scheme, op: Operation, inner: BytesReader) -> Self {
        Self { scheme, op, inner }
    }
}

impl AsyncRead for MetricReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut (*self.inner))
            .poll_read(cx, buf)
            .map(|res| match res {
                Ok(bytes) => {
                    match self.op {
                        Operation::Read => {
                            counter!(
                                METRIC_BYTES_READ_TOTAL, bytes as u64,
                                LABEL_SERVICE => self.scheme.into_static(),
                                LABEL_OPERATION => self.op.into_static(),
                            );
                        }
                        Operation::Write => {
                            counter!(
                                METRIC_BYTES_WRITTEN_TOTAL, bytes as u64,
                                LABEL_SERVICE => self.scheme.into_static(),
                                LABEL_OPERATION => self.op.into_static(),
                            );
                        }
                        _ => {
                            unreachable!();
                        }
                    };
                    Ok(bytes)
                }
                Err(e) => {
                    if e.kind() == ErrorKind::Other {
                        increment_counter!(
                            METRIC_FAILURES_TOTAL,
                            LABEL_SERVICE => self.scheme.into_static(),
                            LABEL_OPERATION => self.op.into_static(),
                        );
                    } else {
                        increment_counter!(
                            METRIC_ERRORS_TOTAL,
                            LABEL_SERVICE => self.scheme.into_static(),
                            LABEL_OPERATION => self.op.into_static(),
                        );
                    }
                    Err(e)
                }
            })
    }
}

struct BlockingMetricReader {
    scheme: Scheme,
    op: Operation,
    inner: BlockingBytesReader,
}

impl BlockingMetricReader {
    fn new(scheme: Scheme, op: Operation, inner: BlockingBytesReader) -> Self {
        Self { scheme, op, inner }
    }
}

impl Read for BlockingMetricReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner
            .read(buf)
            .map(|n| {
                match self.op {
                    Operation::Read => {
                        counter!(
                            METRIC_BYTES_READ_TOTAL, n as u64,
                            LABEL_SERVICE => self.scheme.into_static(),
                            LABEL_OPERATION => self.op.into_static(),
                        );
                    }
                    Operation::Write => {
                        counter!(
                            METRIC_BYTES_WRITTEN_TOTAL, n as u64,
                            LABEL_SERVICE => self.scheme.into_static(),
                            LABEL_OPERATION => self.op.into_static(),
                        );
                    }
                    _ => {
                        unreachable!();
                    }
                }
                n
            })
            .map_err(|e| {
                if e.kind() == ErrorKind::Other {
                    increment_counter!(
                        METRIC_FAILURES_TOTAL,
                        LABEL_SERVICE => self.scheme.into_static(),
                        LABEL_OPERATION => self.op.into_static(),
                    );
                } else {
                    increment_counter!(
                        METRIC_ERRORS_TOTAL,
                        LABEL_SERVICE => self.scheme.into_static(),
                        LABEL_OPERATION => self.op.into_static(),
                    );
                }
                e
            })
    }
}
