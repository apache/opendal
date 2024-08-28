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

use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::encoding::EncodeLabel;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::LabelSetEncoder;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use prometheus_client::registry::Unit;

use crate::layers::observe;
use crate::raw::*;
use crate::*;

/// Add [prometheus-client](https://docs.rs/prometheus-client) for every operation.
///
/// # Examples
///
/// ```no_build
/// use log::debug;
/// use log::info;
/// use opendal::layers::PrometheusClientLayer;
/// use opendal::layers::PrometheusClientInterceptor;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Result;
///
/// /// Visit [`opendal::services`] for more service related config.
/// /// Visit [`opendal::Operator`] for more operator level APIs.
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Pick a builder and configure it.
///     let builder = services::Memory::default();
///     let mut registry = prometheus_client::registry::Registry::default();
///
///     let op = Operator::new(builder)
///         .expect("must init")
///         .layer(PrometheusClientLayer::new(&mut registry))
///         .finish();
///     debug!("operator: {op:?}");
///
///     // Write data into object test.
///     op.write("test", "Hello, World!").await?;
///     // Read data from object.
///     let bs = op.read("test").await?;
///     info!("content: {}", String::from_utf8_lossy(&bs.to_bytes()));
///
///     // Get object metadata.
///     let meta = op.stat("test").await?;
///     info!("meta: {:?}", meta);
///
///     // Export prometheus metrics.
///     let mut buf = String::new();
///     prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
///     println!("## Prometheus Metrics");
///     println!("{}", buf);
///     Ok(())
/// }
/// ```
pub struct PrometheusClientLayer(observe::MetricsLayer<PrometheusClientInterceptor>);

impl PrometheusClientLayer {
    /// Create a new [`PrometheusClientLayer`].
    pub fn new(registry: &mut Registry) -> Self {
        let operation_duration_seconds = Family::<OperationLabels, _>::new_with_constructor(|| {
            let buckets = histogram::exponential_buckets(0.01, 2.0, 16);
            Histogram::new(buckets)
        });
        registry.register_with_unit(
            observe::METRIC_OPERATION_DURATION_SECONDS.name(),
            observe::METRIC_OPERATION_DURATION_SECONDS.help(),
            Unit::Seconds,
            operation_duration_seconds.clone(),
        );

        let operation_bytes = Family::<OperationLabels, _>::new_with_constructor(|| {
            let buckets = histogram::exponential_buckets(1.0, 2.0, 16);
            Histogram::new(buckets)
        });
        registry.register_with_unit(
            observe::METRIC_OPERATION_BYTES.name(),
            observe::METRIC_OPERATION_BYTES.help(),
            Unit::Bytes,
            operation_bytes.clone(),
        );

        let operation_errors_total =
            Family::<OperationErrorsTotalLabels, Counter>::new_with_constructor(|| {
                Counter::default()
            });
        registry.register(
            observe::METRIC_OPERATION_ERRORS_TOTAL.name(),
            observe::METRIC_OPERATION_ERRORS_TOTAL.help(),
            operation_errors_total.clone(),
        );

        let interceptor = PrometheusClientInterceptor {
            operation_duration_seconds,
            operation_bytes,
            operation_errors_total,
        };
        Self(observe::MetricsLayer::new(interceptor))
    }
}

impl<A: Access> Layer<A> for PrometheusClientLayer {
    type LayeredAccess = observe::MetricsAccessor<A, PrometheusClientInterceptor>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        self.0.layer(inner)
    }
}

#[derive(Debug, Clone)]
pub struct PrometheusClientInterceptor {
    operation_duration_seconds: Family<OperationLabels, Histogram>,
    operation_bytes: Family<OperationLabels, Histogram>,
    operation_errors_total: Family<OperationErrorsTotalLabels, Counter>,
}

impl observe::MetricsIntercept for PrometheusClientInterceptor {
    fn observe_operation_duration_seconds(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        _: &str,
        op: Operation,
        duration: Duration,
    ) {
        self.operation_duration_seconds
            .get_or_create(&OperationLabels {
                scheme,
                namespace,
                root,
                path: None,
                op,
            })
            .observe(duration.as_secs_f64())
    }

    fn observe_operation_bytes(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        _: &str,
        op: Operation,
        bytes: usize,
    ) {
        self.operation_bytes
            .get_or_create(&OperationLabels {
                scheme,
                namespace,
                root,
                path: None,
                op,
            })
            .observe(bytes as f64)
    }

    fn observe_operation_errors_total(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        _: &str,
        op: Operation,
        error: ErrorKind,
    ) {
        self.operation_errors_total
            .get_or_create(&OperationErrorsTotalLabels {
                scheme,
                namespace,
                root,
                path: None,
                op,
                error: error.into_static(),
            })
            .inc();
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct OperationLabels {
    scheme: Scheme,
    namespace: Arc<String>,
    root: Arc<String>,
    path: Option<String>,
    op: Operation,
}

impl EncodeLabelSet for OperationLabels {
    fn encode(&self, mut encoder: LabelSetEncoder) -> Result<(), fmt::Error> {
        (observe::LABEL_SCHEME, self.scheme.into_static()).encode(encoder.encode_label())?;
        (observe::LABEL_NAMESPACE, self.namespace.as_str()).encode(encoder.encode_label())?;
        (observe::LABEL_ROOT, self.root.as_str()).encode(encoder.encode_label())?;
        if let Some(path) = &self.path {
            (observe::LABEL_PATH, path.as_str()).encode(encoder.encode_label())?;
        }
        (observe::LABEL_OPERATION, self.op.into_static()).encode(encoder.encode_label())?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct OperationErrorsTotalLabels {
    scheme: Scheme,
    namespace: Arc<String>,
    root: Arc<String>,
    path: Option<String>,
    op: Operation,
    error: &'static str,
}

impl EncodeLabelSet for OperationErrorsTotalLabels {
    fn encode(&self, mut encoder: LabelSetEncoder) -> Result<(), fmt::Error> {
        (observe::LABEL_SCHEME, self.scheme.into_static()).encode(encoder.encode_label())?;
        (observe::LABEL_NAMESPACE, self.namespace.as_str()).encode(encoder.encode_label())?;
        (observe::LABEL_ROOT, self.root.as_str()).encode(encoder.encode_label())?;
        if let Some(path) = &self.path {
            (observe::LABEL_PATH, path.as_str()).encode(encoder.encode_label())?;
        }
        (observe::LABEL_OPERATION, self.op.into_static()).encode(encoder.encode_label())?;
        (observe::LABEL_ERROR, self.error).encode(encoder.encode_label())?;
        Ok(())
    }
}
