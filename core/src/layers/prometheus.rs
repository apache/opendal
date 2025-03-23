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

use std::sync::Arc;
use std::time::Duration;

use prometheus::core::AtomicU64;
use prometheus::core::GenericCounterVec;
use prometheus::exponential_buckets;
use prometheus::histogram_opts;
use prometheus::HistogramVec;
use prometheus::Opts;
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
    /// # Default Configuration
    ///
    /// - `operation_duration_seconds_buckets`: `exponential_buckets(0.01, 2.0, 16)`
    /// - `operation_bytes_buckets`: `exponential_buckets(1.0, 2.0, 16)`
    /// - `path_label`: `0`
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
    ///             .operation_duration_seconds_buckets(duration_seconds_buckets)
    ///             .operation_bytes_buckets(bytes_buckets)
    ///             .path_label(0)
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
        let operation_duration_seconds_buckets = exponential_buckets(0.01, 2.0, 16).unwrap();
        let operation_bytes_buckets = exponential_buckets(1.0, 2.0, 16).unwrap();
        let http_request_duration_seconds_buckets = exponential_buckets(0.01, 2.0, 16).unwrap();
        let http_request_bytes_buckets = exponential_buckets(1.0, 2.0, 16).unwrap();
        let path_label_level = 0;
        PrometheusLayerBuilder::new(
            operation_duration_seconds_buckets,
            operation_bytes_buckets,
            http_request_duration_seconds_buckets,
            http_request_bytes_buckets,
            path_label_level,
        )
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
    operation_duration_seconds_buckets: Vec<f64>,
    operation_bytes_buckets: Vec<f64>,
    http_request_duration_seconds_buckets: Vec<f64>,
    http_request_bytes_buckets: Vec<f64>,
    path_label_level: usize,
}

impl PrometheusLayerBuilder {
    fn new(
        operation_duration_seconds_buckets: Vec<f64>,
        operation_bytes_buckets: Vec<f64>,
        http_request_duration_seconds_buckets: Vec<f64>,
        http_request_bytes_buckets: Vec<f64>,
        path_label_level: usize,
    ) -> Self {
        Self {
            operation_duration_seconds_buckets,
            operation_bytes_buckets,
            http_request_duration_seconds_buckets,
            http_request_bytes_buckets,
            path_label_level,
        }
    }

    /// Set buckets for `operation_duration_seconds` histogram.
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
    /// let buckets = prometheus::exponential_buckets(0.01, 2.0, 16).unwrap();
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusLayer::builder()
    ///             .operation_duration_seconds_buckets(buckets)
    ///             .register(registry)
    ///             .expect("register metrics successfully"),
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
    /// let buckets = prometheus::exponential_buckets(1.0, 2.0, 16).unwrap();
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusLayer::builder()
    ///             .operation_bytes_buckets(buckets)
    ///             .register(registry)
    ///             .expect("register metrics successfully"),
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

    /// Set buckets for `http_request_duration_seconds` histogram.
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
    /// let buckets = prometheus::exponential_buckets(0.01, 2.0, 16).unwrap();
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusLayer::builder()
    ///             .http_request_duration_seconds_buckets(buckets)
    ///             .register(registry)
    ///             .expect("register metrics successfully"),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn http_request_duration_seconds_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.http_request_duration_seconds_buckets = buckets;
        }
        self
    }

    /// Set buckets for `http_request_bytes` histogram.
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
    /// let buckets = prometheus::exponential_buckets(1.0, 2.0, 16).unwrap();
    /// let op = Operator::new(builder)?
    ///     .layer(
    ///         PrometheusLayer::builder()
    ///             .http_request_bytes_buckets(buckets)
    ///             .register(registry)
    ///             .expect("register metrics successfully"),
    ///     )
    ///     .finish();
    /// debug!("operator: {op:?}");
    ///
    /// Ok(())
    /// # }
    /// ```
    pub fn http_request_bytes_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.http_request_bytes_buckets = buckets;
        }
        self
    }

    /// Set the level of path label.
    ///
    /// - level = 0: we will ignore the path label.
    /// - level > 0: the path label will be the path split by "/" and get the last n level,
    ///   if n=1 and input path is "abc/def/ghi", and then we will get "abc/" as the path label.
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
    ///             .path_label(1)
    ///             .register(registry)
    ///             .expect("register metrics successfully"),
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
        let labels = OperationLabels::names(false, self.path_label_level > 0);
        let operation_duration_seconds = HistogramVec::new(
            histogram_opts!(
                observe::METRIC_OPERATION_DURATION_SECONDS.name(),
                observe::METRIC_OPERATION_DURATION_SECONDS.help(),
                self.operation_duration_seconds_buckets
            ),
            &labels,
        )
        .map_err(parse_prometheus_error)?;
        let operation_bytes = HistogramVec::new(
            histogram_opts!(
                observe::METRIC_OPERATION_BYTES.name(),
                observe::METRIC_OPERATION_BYTES.help(),
                self.operation_bytes_buckets
            ),
            &labels,
        )
        .map_err(parse_prometheus_error)?;

        let labels = OperationLabels::names(true, self.path_label_level > 0);
        let operation_errors_total = GenericCounterVec::new(
            Opts::new(
                observe::METRIC_OPERATION_ERRORS_TOTAL.name(),
                observe::METRIC_OPERATION_ERRORS_TOTAL.help(),
            ),
            &labels,
        )
        .map_err(parse_prometheus_error)?;

        let labels = OperationLabels::names(false, false);
        let http_request_duration_seconds = HistogramVec::new(
            histogram_opts!(
                observe::METRIC_HTTP_REQUEST_DURATION_SECONDS.name(),
                observe::METRIC_HTTP_REQUEST_DURATION_SECONDS.help(),
                self.http_request_duration_seconds_buckets
            ),
            &labels,
        )
        .map_err(parse_prometheus_error)?;
        let http_request_bytes = HistogramVec::new(
            histogram_opts!(
                observe::METRIC_HTTP_REQUEST_BYTES.name(),
                observe::METRIC_HTTP_REQUEST_BYTES.help(),
                self.http_request_bytes_buckets
            ),
            &labels,
        )
        .map_err(parse_prometheus_error)?;

        registry
            .register(Box::new(operation_duration_seconds.clone()))
            .map_err(parse_prometheus_error)?;
        registry
            .register(Box::new(operation_bytes.clone()))
            .map_err(parse_prometheus_error)?;
        registry
            .register(Box::new(operation_errors_total.clone()))
            .map_err(parse_prometheus_error)?;
        registry
            .register(Box::new(http_request_duration_seconds.clone()))
            .map_err(parse_prometheus_error)?;
        registry
            .register(Box::new(http_request_bytes.clone()))
            .map_err(parse_prometheus_error)?;

        Ok(PrometheusLayer {
            interceptor: PrometheusInterceptor {
                operation_duration_seconds,
                operation_bytes,
                operation_errors_total,
                http_request_duration_seconds,
                http_request_bytes,
                path_label_level: self.path_label_level,
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
    operation_duration_seconds: HistogramVec,
    operation_bytes: HistogramVec,
    operation_errors_total: GenericCounterVec<AtomicU64>,
    http_request_duration_seconds: HistogramVec,
    http_request_bytes: HistogramVec,
    path_label_level: usize,
}

impl observe::MetricsIntercept for PrometheusInterceptor {
    fn observe_operation_duration_seconds(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        duration: Duration,
    ) {
        let labels = OperationLabels {
            scheme: info.scheme(),
            name: info.name(),
            root: info.root(),
            operation: op,
            error: None,
            path: observe::path_label_value(path, self.path_label_level),
        };

        self.operation_duration_seconds
            .with_label_values(&labels.values())
            .observe(duration.as_secs_f64())
    }

    fn observe_operation_bytes(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        bytes: usize,
    ) {
        let labels = OperationLabels {
            scheme: info.scheme(),
            name: info.name(),
            root: info.root(),
            operation: op,
            error: None,
            path: observe::path_label_value(path, self.path_label_level),
        };

        self.operation_bytes
            .with_label_values(&labels.values())
            .observe(bytes as f64);
    }

    fn observe_operation_errors_total(
        &self,
        info: Arc<AccessorInfo>,
        path: &str,
        op: Operation,
        error: ErrorKind,
    ) {
        let labels = OperationLabels {
            scheme: info.scheme(),
            name: info.name(),
            root: info.root(),
            operation: op,
            error: Some(error),
            path: observe::path_label_value(path, self.path_label_level),
        };

        self.operation_errors_total
            .with_label_values(&labels.values())
            .inc();
    }

    fn observe_http_request_duration_seconds(
        &self,
        info: Arc<AccessorInfo>,
        op: Operation,
        duration: Duration,
    ) {
        let labels = OperationLabels {
            scheme: info.scheme(),
            name: info.name(),
            root: info.root(),
            operation: op,
            error: None,
            path: None,
        };

        self.http_request_duration_seconds
            .with_label_values(&labels.values())
            .observe(duration.as_secs_f64())
    }

    fn observe_http_request_bytes(&self, info: Arc<AccessorInfo>, op: Operation, bytes: usize) {
        let labels = OperationLabels {
            scheme: info.scheme(),
            name: info.name(),
            root: info.root(),
            operation: op,
            error: None,
            path: None,
        };

        self.http_request_bytes
            .with_label_values(&labels.values())
            .observe(bytes as f64);
    }
}

struct OperationLabels<'a> {
    scheme: Scheme,
    name: Arc<str>,
    root: Arc<str>,
    operation: Operation,
    path: Option<&'a str>,
    error: Option<ErrorKind>,
}

impl<'a> OperationLabels<'a> {
    fn names(error: bool, path: bool) -> Vec<&'a str> {
        let mut names = Vec::with_capacity(6);

        names.extend([
            observe::LABEL_SCHEME,
            observe::LABEL_NAMESPACE,
            observe::LABEL_ROOT,
            observe::LABEL_OPERATION,
        ]);

        if path {
            names.push(observe::LABEL_PATH);
        }

        if error {
            names.push(observe::LABEL_ERROR);
        }

        names
    }

    /// labels:
    ///
    /// 1. `["scheme", "namespace", "root", "operation"]`
    /// 2. `["scheme", "namespace", "root", "operation", "path"]`
    /// 3. `["scheme", "namespace", "root", "operation", "error"]`
    /// 4. `["scheme", "namespace", "root", "operation", "path", "error"]`
    fn values(&'a self) -> Vec<&'a str> {
        let mut labels = Vec::with_capacity(6);

        labels.extend([
            self.scheme.into_static(),
            self.name.as_ref(),
            self.root.as_ref(),
            self.operation.into_static(),
        ]);

        if let Some(path) = self.path {
            labels.push(path);
        }

        if let Some(error) = self.error {
            labels.push(error.into_static());
        }

        labels
    }
}
