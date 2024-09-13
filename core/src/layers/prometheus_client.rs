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
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::encoding::EncodeLabel;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::LabelSetEncoder;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::family::MetricConstructor;
use prometheus_client::metrics::histogram::exponential_buckets;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use crate::layers::observe;
use crate::raw::*;
use crate::*;

/// Add [prometheus-client](https://docs.rs/prometheus-client) for every operation.
///
/// # Prometheus Metrics
///
/// We provide several metrics, please see the documentation of [`observe`] module.
/// For a more detailed explanation of these metrics and how they are used, please refer to the [Prometheus documentation](https://prometheus.io/docs/introduction/overview/).
///
/// # Examples
///
/// ```no_run
/// # use log::debug;
/// # use log::info;
/// # use opendal::layers::PrometheusClientLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let mut registry = prometheus_client::registry::Registry::default();
///
/// let op = Operator::new(services::Memory::default())?
///     .layer(PrometheusClientLayer::builder().register(&mut registry))
///     .finish();
/// debug!("operator: {op:?}");
///
/// // Write data into object test.
/// op.write("test", "Hello, World!").await?;
/// // Read data from object.
/// let bs = op.read("test").await?;
/// info!("content: {}", String::from_utf8_lossy(&bs.to_bytes()));
///
/// // Get object metadata.
/// let meta = op.stat("test").await?;
/// info!("meta: {:?}", meta);
///
/// // Export prometheus metrics.
/// let mut buf = String::new();
/// prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
/// println!("## Prometheus Metrics");
/// println!("{}", buf);
///
/// Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct PrometheusClientLayer {
    interceptor: PrometheusClientInterceptor,
}

impl PrometheusClientLayer {
    /// Create a [`PrometheusClientLayerBuilder`] to set the configuration of metrics.
    ///
    /// # Default Configuration
    ///
    /// - `operation_duration_seconds_buckets`: `exponential_buckets(0.01, 2.0, 16)`
    /// - `operation_bytes_buckets`: `exponential_buckets(1.0, 2.0, 16)`
    /// - `path_label`: `0`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use log::debug;
    /// # use opendal::layers::PrometheusClientLayer;
    /// # use opendal::services;
    /// # use opendal::Operator;
    /// # use opendal::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // Pick a builder and configure it.
    /// let builder = services::Memory::default();
    /// let mut registry = prometheus_client::registry::Registry::default();
    ///
    /// let duration_seconds_buckets =
    ///     prometheus_client::metrics::histogram::exponential_buckets(0.01, 2.0, 16).collect();
    /// let bytes_buckets =
    ///     prometheus_client::metrics::histogram::exponential_buckets(1.0, 2.0, 16).collect();
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusClientLayer::builder()
    ///             .operation_duration_seconds_buckets(duration_seconds_buckets)
    ///             .operation_bytes_buckets(bytes_buckets)
    ///             .path_label(0)
    ///             .register(&mut registry),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn builder() -> PrometheusClientLayerBuilder {
        let operation_duration_seconds_buckets = exponential_buckets(0.01, 2.0, 16).collect();
        let operation_bytes_buckets = exponential_buckets(1.0, 2.0, 16).collect();
        PrometheusClientLayerBuilder::new(
            operation_duration_seconds_buckets,
            operation_bytes_buckets,
            0,
        )
    }
}

impl<A: Access> Layer<A> for PrometheusClientLayer {
    type LayeredAccess = observe::MetricsAccessor<A, PrometheusClientInterceptor>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        observe::MetricsLayer::new(self.interceptor.clone()).layer(inner)
    }
}

/// [`PrometheusClientLayerBuilder`] is a config builder to build a [`PrometheusClientLayer`].
pub struct PrometheusClientLayerBuilder {
    operation_duration_seconds_buckets: Vec<f64>,
    operation_bytes_buckets: Vec<f64>,
    path_label_level: usize,
}

impl PrometheusClientLayerBuilder {
    fn new(
        operation_duration_seconds_buckets: Vec<f64>,
        operation_bytes_buckets: Vec<f64>,
        path_label_level: usize,
    ) -> Self {
        Self {
            operation_duration_seconds_buckets,
            operation_bytes_buckets,
            path_label_level,
        }
    }

    /// Set buckets for `operation_duration_seconds` histogram.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use log::debug;
    /// # use opendal::layers::PrometheusClientLayer;
    /// # use opendal::services;
    /// # use opendal::Operator;
    /// # use opendal::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // Pick a builder and configure it.
    /// let builder = services::Memory::default();
    /// let mut registry = prometheus_client::registry::Registry::default();
    ///
    /// let buckets =
    ///     prometheus_client::metrics::histogram::exponential_buckets(0.01, 2.0, 16).collect();
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusClientLayer::builder()
    ///             .operation_duration_seconds_buckets(buckets)
    ///             .register(&mut registry),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn operation_duration_seconds_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.operation_duration_seconds_buckets = buckets;
        }
        self
    }

    /// Set buckets for `operation_bytes` histogram.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use log::debug;
    /// # use opendal::layers::PrometheusClientLayer;
    /// # use opendal::services;
    /// # use opendal::Operator;
    /// # use opendal::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // Pick a builder and configure it.
    /// let builder = services::Memory::default();
    /// let mut registry = prometheus_client::registry::Registry::default();
    ///
    /// let buckets =
    ///     prometheus_client::metrics::histogram::exponential_buckets(1.0, 2.0, 16).collect();
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusClientLayer::builder()
    ///             .operation_bytes_buckets(buckets)
    ///             .register(&mut registry),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn operation_bytes_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.operation_bytes_buckets = buckets;
        }
        self
    }

    /// Set the level of path label.
    ///
    /// - level = 0: we will ignore the path label.
    /// - level > 0: the path label will be the path split by "/" and get the last n level,
    ///   if n=1 and input path is "abc/def/ghi", and then we will get "abc/" as the path label.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use log::debug;
    /// # use opendal::layers::PrometheusClientLayer;
    /// # use opendal::services;
    /// # use opendal::Operator;
    /// # use opendal::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // Pick a builder and configure it.
    /// let builder = services::Memory::default();
    /// let mut registry = prometheus_client::registry::Registry::default();
    ///
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusClientLayer::builder()
    ///             .path_label(1)
    ///             .register(&mut registry),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn path_label(mut self, level: usize) -> Self {
        self.path_label_level = level;
        self
    }

    /// Register the metrics into the registry and return a [`PrometheusClientLayer`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use log::debug;
    /// # use opendal::layers::PrometheusClientLayer;
    /// # use opendal::services;
    /// # use opendal::Operator;
    /// # use opendal::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // Pick a builder and configure it.
    /// let builder = services::Memory::default();
    /// let mut registry = prometheus_client::registry::Registry::default();
    ///
    /// let op = Operator::new(builder)?
    ///     .layer(PrometheusClientLayer::builder().register(&mut registry))
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn register(self, registry: &mut Registry) -> PrometheusClientLayer {
        let operation_duration_seconds =
            Family::<OperationLabels, Histogram, _>::new_with_constructor(HistogramConstructor {
                buckets: self.operation_duration_seconds_buckets,
            });
        let operation_bytes =
            Family::<OperationLabels, Histogram, _>::new_with_constructor(HistogramConstructor {
                buckets: self.operation_bytes_buckets,
            });
        let operation_errors_total = Family::<OperationLabels, Counter>::default();

        registry.register(
            observe::METRIC_OPERATION_DURATION_SECONDS.name(),
            observe::METRIC_OPERATION_DURATION_SECONDS.help(),
            operation_duration_seconds.clone(),
        );
        registry.register(
            observe::METRIC_OPERATION_BYTES.name(),
            observe::METRIC_OPERATION_BYTES.help(),
            operation_bytes.clone(),
        );
        // `prometheus-client` will automatically add `_total` suffix into the name of counter
        // metrics, so we can't use `METRIC_OPERATION_ERRORS_TOTAL.name()` here.
        registry.register(
            "opendal_operation_errors",
            observe::METRIC_OPERATION_ERRORS_TOTAL.help(),
            operation_errors_total.clone(),
        );

        PrometheusClientLayer {
            interceptor: PrometheusClientInterceptor {
                operation_duration_seconds,
                operation_bytes,
                operation_errors_total,
                path_label_level: self.path_label_level,
            },
        }
    }
}

#[derive(Clone)]
struct HistogramConstructor {
    buckets: Vec<f64>,
}

impl MetricConstructor<Histogram> for HistogramConstructor {
    fn new_metric(&self) -> Histogram {
        Histogram::new(self.buckets.iter().cloned())
    }
}

#[derive(Clone, Debug)]
pub struct PrometheusClientInterceptor {
    operation_duration_seconds: Family<OperationLabels, Histogram, HistogramConstructor>,
    operation_bytes: Family<OperationLabels, Histogram, HistogramConstructor>,
    operation_errors_total: Family<OperationLabels, Counter>,
    path_label_level: usize,
}

impl observe::MetricsIntercept for PrometheusClientInterceptor {
    fn observe_operation_duration_seconds(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        path: &str,
        op: Operation,
        duration: Duration,
    ) {
        self.operation_duration_seconds
            .get_or_create(&OperationLabels {
                scheme,
                namespace,
                root,
                operation: op,
                path: observe::path_label_value(path, self.path_label_level).map(Into::into),
                error: None,
            })
            .observe(duration.as_secs_f64())
    }

    fn observe_operation_bytes(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        path: &str,
        op: Operation,
        bytes: usize,
    ) {
        self.operation_bytes
            .get_or_create(&OperationLabels {
                scheme,
                namespace,
                root,
                operation: op,
                path: observe::path_label_value(path, self.path_label_level).map(Into::into),
                error: None,
            })
            .observe(bytes as f64)
    }

    fn observe_operation_errors_total(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        path: &str,
        op: Operation,
        error: ErrorKind,
    ) {
        self.operation_errors_total
            .get_or_create(&OperationLabels {
                scheme,
                namespace,
                root,
                operation: op,
                path: observe::path_label_value(path, self.path_label_level).map(Into::into),
                error: Some(error.into_static()),
            })
            .inc();
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct OperationLabels {
    scheme: Scheme,
    namespace: Arc<String>,
    root: Arc<String>,
    operation: Operation,
    path: Option<String>,
    error: Option<&'static str>,
}

impl EncodeLabelSet for OperationLabels {
    fn encode(&self, mut encoder: LabelSetEncoder) -> Result<(), fmt::Error> {
        (observe::LABEL_SCHEME, self.scheme.into_static()).encode(encoder.encode_label())?;
        (observe::LABEL_NAMESPACE, self.namespace.as_str()).encode(encoder.encode_label())?;
        (observe::LABEL_ROOT, self.root.as_str()).encode(encoder.encode_label())?;
        (observe::LABEL_OPERATION, self.operation.into_static()).encode(encoder.encode_label())?;
        if let Some(path) = &self.path {
            (observe::LABEL_PATH, path.as_str()).encode(encoder.encode_label())?;
        }
        if let Some(error) = self.error {
            (observe::LABEL_ERROR, error).encode(encoder.encode_label())?;
        }
        Ok(())
    }
}
