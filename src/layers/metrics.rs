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
use std::io::Result;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use metrics::histogram;
use metrics::increment_counter;

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
use crate::BytesReader;
use crate::DirStreamer;
use crate::Layer;
use crate::ObjectMetadata;

static METRIC_REQUESTS_TOTAL: &str = "opendal_requests_total";
static METRIC_REQUESTS_DURATION_SECONDS: &str = "opendal_requests_duration_seconds";
static LABEL_SERVICE: &str = "service";
static LABEL_OPERATION: &str = "operation";

/// MetricsLayer will add metrics for OpenDAL.
///
/// # Metrics
///
/// - `opendal_requests_total`: Total requests numbers
/// - `opendal_requests_duration_seconds`: Request duration seconds.
///   - NOTE: this metric tracks the duration of the OpenDAL's function call, not the underlying http request duration
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

    async fn create(&self, args: &OpCreate) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Create.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.create(args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Create.into_static(),
        );

        result
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Read.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.read(args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Read.into_static(),
        );

        result
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Write.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.write(args, r).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Write.into_static(),
        );

        result
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Stat.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.stat(args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Stat.into_static(),
        );

        result
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Delete.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.delete(args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Delete.into_static(),
        );

        result
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::List.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.list(args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::List.into_static(),
        );

        result
    }

    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Presign.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.presign(args);
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::Presign.into_static(),
        );

        result
    }

    async fn create_multipart(&self, args: &OpCreateMultipart) -> Result<String> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::CreateMultipart.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.create_multipart(args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::CreateMultipart.into_static(),
        );

        result
    }

    async fn write_multipart(&self, args: &OpWriteMultipart, r: BytesReader) -> Result<u64> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::WriteMultipart.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.write_multipart(args, r).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::WriteMultipart.into_static(),
        );

        result
    }

    async fn complete_multipart(&self, args: &OpCompleteMultipart) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::CompleteMultipart.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.complete_multipart(args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::CompleteMultipart.into_static(),
        );

        result
    }

    async fn abort_multipart(&self, args: &OpAbortMultipart) -> Result<()> {
        increment_counter!(
            METRIC_REQUESTS_TOTAL,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::AbortMultipart.into_static(),
        );

        let start = Instant::now();
        let result = self.inner.abort_multipart(args).await;
        let dur = start.elapsed().as_secs_f64();

        histogram!(
            METRIC_REQUESTS_DURATION_SECONDS, dur,
            LABEL_SERVICE => self.meta.scheme().into_static(),
            LABEL_OPERATION => Operation::AbortMultipart.into_static(),
        );

        result
    }
}
