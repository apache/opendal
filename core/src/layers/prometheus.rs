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

use std::time::Duration;

use prometheus::core::AtomicI64;
use prometheus::core::AtomicU64;
use prometheus::core::GenericCounterVec;
use prometheus::core::GenericGaugeVec;
use prometheus::register_histogram_vec_with_registry;
use prometheus::register_int_counter_vec_with_registry;
use prometheus::register_int_gauge_vec_with_registry;
use prometheus::HistogramVec;
use prometheus::Registry;

use crate::layers::observe;
use crate::raw::Access;
use crate::raw::*;
use crate::*;

/// Add [prometheus](https://docs.rs/prometheus) for every operation.
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
/// # use opendal::layers::PrometheusLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use prometheus::Encoder;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let registry = prometheus::default_registry();
///
/// let op = Operator::new(services::Memory::default())?
///     .layer(
///         PrometheusLayer::builder()
///             .register(registry)
///             .expect("register metrics successfully"),
///     )
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
/// let mut buffer = Vec::<u8>::new();
/// let encoder = prometheus::TextEncoder::new();
/// encoder.encode(&prometheus::gather(), &mut buffer).unwrap();
/// println!("## Prometheus Metrics");
/// println!("{}", String::from_utf8(buffer.clone()).unwrap());
///
/// Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct PrometheusLayer {
    interceptor: PrometheusInterceptor,
}

impl PrometheusLayer {
    /// Create a [`PrometheusLayerBuilder`] to set the configuration of metrics.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use log::debug;
    /// # use opendal::layers::PrometheusLayer;
    /// # use opendal::services;
    /// # use opendal::Operator;
    /// # use opendal::Result;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // Pick a builder and configure it.
    /// let builder = services::Memory::default();
    /// let registry = prometheus::default_registry();
    ///
    /// let duration_seconds_buckets = prometheus::exponential_buckets(0.01, 2.0, 16).unwrap();
    /// let bytes_buckets = prometheus::exponential_buckets(1.0, 2.0, 16).unwrap();
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusLayer::builder()
    ///             .duration_seconds_buckets(duration_seconds_buckets)
    ///             .bytes_buckets(bytes_buckets)
    ///             .register(registry)
    ///             .expect("register metrics successfully"),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn builder() -> PrometheusLayerBuilder {
        PrometheusLayerBuilder::default()
    }
}

impl<A: Access> Layer<A> for PrometheusLayer {
    type LayeredAccess = observe::MetricsAccessor<A, PrometheusInterceptor>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        observe::MetricsLayer::new(self.interceptor.clone()).layer(inner)
    }
}

/// [`PrometheusLayerBuilder`] is a config builder to build a [`PrometheusLayer`].
pub struct PrometheusLayerBuilder {
    bytes_buckets: Vec<f64>,
    bytes_rate_buckets: Vec<f64>,
    entries_buckets: Vec<f64>,
    entries_rate_buckets: Vec<f64>,
    duration_seconds_buckets: Vec<f64>,
    ttfb_buckets: Vec<f64>,
}

impl Default for PrometheusLayerBuilder {
    fn default() -> Self {
        Self {
            bytes_buckets: observe::DEFAULT_BYTES_BUCKETS.to_vec(),
            bytes_rate_buckets: observe::DEFAULT_BYTES_RATE_BUCKETS.to_vec(),
            entries_buckets: observe::DEFAULT_ENTRIES_BUCKETS.to_vec(),
            entries_rate_buckets: observe::DEFAULT_ENTRIES_RATE_BUCKETS.to_vec(),
            duration_seconds_buckets: observe::DEFAULT_DURATION_SECONDS_BUCKETS.to_vec(),
            ttfb_buckets: observe::DEFAULT_TTFB_BUCKETS.to_vec(),
        }
    }
}

impl PrometheusLayerBuilder {
    /// Set buckets for bytes related histogram like `operation_bytes`.
    pub fn bytes_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.bytes_buckets = buckets;
        }
        self
    }

    /// Set buckets for bytes rate related histogram like `operation_bytes_rate`.
    pub fn bytes_rate_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.bytes_rate_buckets = buckets;
        }
        self
    }

    /// Set buckets for entries related histogram like `operation_entries`.
    pub fn entries_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.entries_buckets = buckets;
        }
        self
    }

    /// Set buckets for entries rate related histogram like `operation_entries_rate`.
    pub fn entries_rate_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.entries_rate_buckets = buckets;
        }
        self
    }

    /// Set buckets for duration seconds related histogram like `operation_duration_seconds`.
    pub fn duration_seconds_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.duration_seconds_buckets = buckets;
        }
        self
    }

    /// Set buckets for ttfb related histogram like `operation_ttfb_seconds`.
    pub fn ttfb_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.ttfb_buckets = buckets;
        }
        self
    }

    /// Register the metrics into the given registry and return a [`PrometheusLayer`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use log::debug;
    /// # use opendal::layers::PrometheusLayer;
    /// # use opendal::services;
    /// # use opendal::Operator;
    /// # use opendal::Result;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // Pick a builder and configure it.
    /// let builder = services::Memory::default();
    /// let registry = prometheus::default_registry();
    ///
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusLayer::builder()
    ///             .register(registry)
    ///             .expect("register metrics successfully"),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn register(self, registry: &Registry) -> Result<PrometheusLayer> {
        let labels = OperationLabels::names();
        let operation_bytes = {
            let metric = observe::MetricValue::OperationBytes(0);
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.bytes_buckets.clone(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let operation_bytes_rate = {
            let metric = observe::MetricValue::OperationBytesRate(0.0);
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.bytes_rate_buckets.clone(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let operation_entries = {
            let metric = observe::MetricValue::OperationEntries(0);
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.entries_buckets,
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let operation_entries_rate = {
            let metric = observe::MetricValue::OperationEntriesRate(0.0);
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.entries_rate_buckets,
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let operation_duration_seconds = {
            let metric = observe::MetricValue::OperationDurationSeconds(Duration::default());
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.duration_seconds_buckets.clone(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let operation_executing = {
            let metric = observe::MetricValue::OperationExecuting(0);
            register_int_gauge_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let operation_ttfb_seconds = {
            let metric = observe::MetricValue::OperationTtfbSeconds(Duration::default());
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.ttfb_buckets.clone(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };

        let labels_with_error = OperationLabels::names().with_error();
        let operation_errors_total = {
            let metric = observe::MetricValue::OperationErrorsTotal;
            register_int_counter_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels_with_error.as_ref(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };

        let http_executing = {
            let metric = observe::MetricValue::HttpExecuting(0);
            register_int_gauge_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let http_request_bytes = {
            let metric = observe::MetricValue::HttpRequestBytes(0);
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.bytes_buckets.clone(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let http_request_bytes_rate = {
            let metric = observe::MetricValue::HttpRequestBytesRate(0.0);
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.bytes_rate_buckets.clone(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let http_request_duration_seconds = {
            let metric = observe::MetricValue::HttpRequestDurationSeconds(Duration::default());
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.duration_seconds_buckets.clone(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let http_response_bytes = {
            let metric = observe::MetricValue::HttpResponseBytes(0);
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.bytes_buckets,
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let http_response_bytes_rate = {
            let metric = observe::MetricValue::HttpResponseBytesRate(0.0);
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.bytes_rate_buckets,
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let http_response_duration_seconds = {
            let metric = observe::MetricValue::HttpResponseDurationSeconds(Duration::default());
            register_histogram_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                self.duration_seconds_buckets,
                registry
            )
            .map_err(parse_prometheus_error)?
        };
        let http_connection_errors_total = {
            let metric = observe::MetricValue::HttpConnectionErrorsTotal;
            register_int_counter_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels.as_ref(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };

        let labels_with_status_code = OperationLabels::names().with_status_code();
        let http_status_errors_total = {
            let metric = observe::MetricValue::HttpStatusErrorsTotal;
            register_int_counter_vec_with_registry!(
                metric.name(),
                metric.help(),
                labels_with_status_code.as_ref(),
                registry
            )
            .map_err(parse_prometheus_error)?
        };

        Ok(PrometheusLayer {
            interceptor: PrometheusInterceptor {
                operation_bytes,
                operation_bytes_rate,
                operation_entries,
                operation_entries_rate,
                operation_duration_seconds,
                operation_errors_total,
                operation_executing,
                operation_ttfb_seconds,

                http_executing,
                http_request_bytes,
                http_request_bytes_rate,
                http_request_duration_seconds,
                http_response_bytes,
                http_response_bytes_rate,
                http_response_duration_seconds,
                http_connection_errors_total,
                http_status_errors_total,
            },
        })
    }

    /// Register the metrics into the default registry and return a [`PrometheusLayer`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use log::debug;
    /// # use opendal::layers::PrometheusLayer;
    /// # use opendal::services;
    /// # use opendal::Operator;
    /// # use opendal::Result;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // Pick a builder and configure it.
    /// let builder = services::Memory::default();
    ///
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusLayer::builder()
    ///             .register_default()
    ///             .expect("register metrics successfully"),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn register_default(self) -> Result<PrometheusLayer> {
        let registry = prometheus::default_registry();
        self.register(registry)
    }
}

/// Convert the [`prometheus::Error`] to [`Error`].
fn parse_prometheus_error(err: prometheus::Error) -> Error {
    Error::new(ErrorKind::Unexpected, err.to_string()).set_source(err)
}

#[derive(Clone, Debug)]
pub struct PrometheusInterceptor {
    operation_bytes: HistogramVec,
    operation_bytes_rate: HistogramVec,
    operation_entries: HistogramVec,
    operation_entries_rate: HistogramVec,
    operation_duration_seconds: HistogramVec,
    operation_errors_total: GenericCounterVec<AtomicU64>,
    operation_executing: GenericGaugeVec<AtomicI64>,
    operation_ttfb_seconds: HistogramVec,

    http_executing: GenericGaugeVec<AtomicI64>,
    http_request_bytes: HistogramVec,
    http_request_bytes_rate: HistogramVec,
    http_request_duration_seconds: HistogramVec,
    http_response_bytes: HistogramVec,
    http_response_bytes_rate: HistogramVec,
    http_response_duration_seconds: HistogramVec,
    http_connection_errors_total: GenericCounterVec<AtomicU64>,
    http_status_errors_total: GenericCounterVec<AtomicU64>,
}

impl observe::MetricsIntercept for PrometheusInterceptor {
    fn observe(&self, labels: observe::MetricLabels, value: observe::MetricValue) {
        let labels = OperationLabels(labels);
        match value {
            observe::MetricValue::OperationBytes(v) => self
                .operation_bytes
                .with_label_values(&labels.values())
                .observe(v as f64),
            observe::MetricValue::OperationBytesRate(v) => self
                .operation_bytes_rate
                .with_label_values(&labels.values())
                .observe(v),
            observe::MetricValue::OperationEntries(v) => self
                .operation_entries
                .with_label_values(&labels.values())
                .observe(v as f64),
            observe::MetricValue::OperationEntriesRate(v) => self
                .operation_entries_rate
                .with_label_values(&labels.values())
                .observe(v),
            observe::MetricValue::OperationDurationSeconds(v) => self
                .operation_duration_seconds
                .with_label_values(&labels.values())
                .observe(v.as_secs_f64()),
            observe::MetricValue::OperationErrorsTotal => self
                .operation_errors_total
                .with_label_values(&labels.values())
                .inc(),
            observe::MetricValue::OperationExecuting(v) => self
                .operation_executing
                .with_label_values(&labels.values())
                .add(v as i64),
            observe::MetricValue::OperationTtfbSeconds(v) => self
                .operation_ttfb_seconds
                .with_label_values(&labels.values())
                .observe(v.as_secs_f64()),

            observe::MetricValue::HttpExecuting(v) => self
                .http_executing
                .with_label_values(&labels.values())
                .add(v as i64),
            observe::MetricValue::HttpRequestBytes(v) => self
                .http_request_bytes
                .with_label_values(&labels.values())
                .observe(v as f64),
            observe::MetricValue::HttpRequestBytesRate(v) => self
                .http_request_bytes_rate
                .with_label_values(&labels.values())
                .observe(v),
            observe::MetricValue::HttpRequestDurationSeconds(v) => self
                .http_request_duration_seconds
                .with_label_values(&labels.values())
                .observe(v.as_secs_f64()),
            observe::MetricValue::HttpResponseBytes(v) => self
                .http_response_bytes
                .with_label_values(&labels.values())
                .observe(v as f64),
            observe::MetricValue::HttpResponseBytesRate(v) => self
                .http_response_bytes_rate
                .with_label_values(&labels.values())
                .observe(v),
            observe::MetricValue::HttpResponseDurationSeconds(v) => self
                .http_response_duration_seconds
                .with_label_values(&labels.values())
                .observe(v.as_secs_f64()),
            observe::MetricValue::HttpConnectionErrorsTotal => self
                .http_connection_errors_total
                .with_label_values(&labels.values())
                .inc(),
            observe::MetricValue::HttpStatusErrorsTotal => self
                .http_status_errors_total
                .with_label_values(&labels.values())
                .inc(),
        }
    }
}

struct OperationLabelNames(Vec<&'static str>);

impl AsRef<[&'static str]> for OperationLabelNames {
    fn as_ref(&self) -> &[&'static str] {
        &self.0
    }
}

impl OperationLabelNames {
    fn with_error(mut self) -> Self {
        self.0.push(observe::LABEL_ERROR);
        self
    }

    fn with_status_code(mut self) -> Self {
        self.0.push(observe::LABEL_STATUS_CODE);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OperationLabels(observe::MetricLabels);

impl OperationLabels {
    fn names() -> OperationLabelNames {
        OperationLabelNames(vec![
            observe::LABEL_SCHEME,
            observe::LABEL_NAMESPACE,
            observe::LABEL_ROOT,
            observe::LABEL_OPERATION,
        ])
    }

    fn values(&self) -> Vec<&str> {
        let mut labels = Vec::with_capacity(6);

        labels.extend([
            self.0.scheme.into_static(),
            self.0.namespace.as_ref(),
            self.0.root.as_ref(),
            self.0.operation,
        ]);

        if let Some(error) = self.0.error {
            labels.push(error.into_static());
        }

        if let Some(status_code) = &self.0.status_code {
            labels.push(status_code.as_str());
        }

        labels
    }
}
