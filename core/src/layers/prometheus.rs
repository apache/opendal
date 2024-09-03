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
use prometheus::histogram_opts;
use prometheus::HistogramVec;
use prometheus::Registry;
use prometheus::{exponential_buckets, Opts};

use crate::layers::observe;
use crate::raw::Access;
use crate::raw::*;
use crate::*;

/// Add [prometheus](https://docs.rs/prometheus) for every operation.
///
/// # Prometheus Metrics
///
/// In this section, we will introduce three metrics that are currently being exported by our project. These metrics are essential for understanding the behavior and performance of our applications.
///
///
/// | Metric Name             | Type     | Description                                       | Labels              |
/// |-------------------------|----------|---------------------------------------------------|---------------------|
/// | requests_total          | Counter  | Total times of 'create' operation being called   | scheme, operation   |
/// | requests_duration_seconds | Histogram | Histogram of the time spent on specific operation | scheme, operation   |
/// | bytes_total             | Histogram | Total size                                        | scheme, operation   |
///
/// For a more detailed explanation of these metrics and how they are used, please refer to the [Prometheus documentation](https://prometheus.io/docs/introduction/overview/).
///
/// # Histogram Configuration
///
/// The metric buckets for these histograms are automatically generated based on the `exponential_buckets(0.01, 2.0, 16)` configuration.
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
///     // Pick a builder and configure it.
///     let builder = services::Memory::default();
///     let registry = prometheus::default_registry();
///
///     let op = Operator::new(builder)
///         .expect("must init")
///         .layer(PrometheusLayer::new(registry))
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
///     let mut buffer = Vec::<u8>::new();
///     let encoder = prometheus::TextEncoder::new();
///     encoder.encode(&prometheus::gather(), &mut buffer).unwrap();
///     println!("## Prometheus Metrics");
///     println!("{}", String::from_utf8(buffer.clone()).unwrap());
///     Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct PrometheusLayer {
    interceptor: PrometheusInterceptor,
}

impl Default for PrometheusLayer {
    fn default() -> Self {
        let register = prometheus::default_registry();
        Self::new(register)
    }
}

impl PrometheusLayer {
    /// Create a [`PrometheusLayer`] while registering itself to this registry.
    pub fn new(registry: &Registry) -> Self {
        PrometheusLayerBuilder::default().register(registry)
    }

    /// Create a [`PrometheusLayerBuilder`].
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
    ///     // Pick a builder and configure it.
    ///     let builder = services::Memory::default();
    ///     let registry = prometheus::default_registry();
    ///
    ///     let duration_seconds_buckets = prometheus::exponential_buckets(0.01, 2.0, 16).unwrap();
    ///     let bytes_buckets = prometheus::exponential_buckets(1.0, 2.0, 16).unwrap();
    ///     let op = Operator::new(builder)
    ///         .expect("must init")
    ///         .layer(
    ///             PrometheusLayer::builder()
    ///                 .operation_duration_seconds_buckets(duration_seconds_buckets)
    ///                 .operation_bytes_buckets(bytes_buckets)
    ///                 .enable_path_label(1)
    ///                 .register(registry)
    ///         )
    ///         .finish();
    ///     debug!("operator: {op:?}");
    ///
    ///     Ok(())
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
    operation_duration_seconds_buckets: Vec<f64>,
    operation_bytes_buckets: Vec<f64>,
    path_label_level: usize,
}

impl Default for PrometheusLayerBuilder {
    fn default() -> Self {
        Self {
            operation_duration_seconds_buckets: exponential_buckets(0.01, 2.0, 16).unwrap(),
            operation_bytes_buckets: exponential_buckets(1.0, 2.0, 16).unwrap(),
            path_label_level: 0,
        }
    }
}

impl PrometheusLayerBuilder {
    /// Set buckets for `operation_duration_seconds` histogram.
    pub fn operation_duration_seconds_buckets(mut self, buckets: Vec<f64>) -> Self {
        if !buckets.is_empty() {
            self.operation_duration_seconds_buckets = buckets;
        }
        self
    }

    /// Set buckets for `operation_bytes` histogram.
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
    pub fn enable_path_label(mut self, level: usize) -> Self {
        self.path_label_level = level;
        self
    }

    /// Register the metrics into the registry and return a [`PrometheusLayer`].
    pub fn register(self, registry: &Registry) -> PrometheusLayer {
        let labels = OperationLabels::names(false, self.path_label_level);
        let operation_duration_seconds = HistogramVec::new(
            histogram_opts!(
                observe::METRIC_OPERATION_DURATION_SECONDS.name(),
                observe::METRIC_OPERATION_DURATION_SECONDS.help(),
                self.operation_duration_seconds_buckets
            ),
            &labels,
        )
        .unwrap();
        let operation_bytes = HistogramVec::new(
            histogram_opts!(
                observe::METRIC_OPERATION_BYTES.name(),
                observe::METRIC_OPERATION_BYTES.help(),
                self.operation_bytes_buckets
            ),
            &labels,
        )
        .unwrap();

        let labels = OperationLabels::names(true, self.path_label_level);
        let operation_errors_total = GenericCounterVec::new(
            Opts::new(
                observe::METRIC_OPERATION_ERRORS_TOTAL.name(),
                observe::METRIC_OPERATION_ERRORS_TOTAL.help(),
            ),
            &labels,
        )
        .unwrap();

        registry
            .register(Box::new(operation_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(operation_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(operation_errors_total.clone()))
            .unwrap();

        PrometheusLayer {
            interceptor: PrometheusInterceptor {
                operation_duration_seconds,
                operation_bytes,
                operation_errors_total,
                path_label_level: self.path_label_level,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct PrometheusInterceptor {
    operation_duration_seconds: HistogramVec,
    operation_bytes: HistogramVec,
    operation_errors_total: GenericCounterVec<AtomicU64>,
    path_label_level: usize,
}

impl observe::MetricsIntercept for PrometheusInterceptor {
    fn observe_operation_duration_seconds(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        path: &str,
        op: Operation,
        duration: Duration,
    ) {
        let labels = OperationLabels {
            scheme,
            namespace: &namespace,
            root: &root,
            op,
            error: None,
            path,
        }
        .into_values(self.path_label_level);

        self.operation_duration_seconds
            .with_label_values(&labels)
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
        let labels = OperationLabels {
            scheme,
            namespace: &namespace,
            root: &root,
            op,
            error: None,
            path,
        }
        .into_values(self.path_label_level);

        self.operation_bytes
            .with_label_values(&labels)
            .observe(bytes as f64);
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
        let labels = OperationLabels {
            scheme,
            namespace: &namespace,
            root: &root,
            op,
            error: Some(error),
            path,
        }
        .into_values(self.path_label_level);

        self.operation_errors_total.with_label_values(&labels).inc();
    }
}

struct OperationLabels<'a> {
    scheme: Scheme,
    namespace: &'a str,
    root: &'a str,
    op: Operation,
    path: &'a str,
    error: Option<ErrorKind>,
}

impl<'a> OperationLabels<'a> {
    fn names(error: bool, path_label_level: usize) -> Vec<&'a str> {
        let mut names = Vec::with_capacity(6);

        names.extend([
            observe::LABEL_SCHEME,
            observe::LABEL_NAMESPACE,
            observe::LABEL_ROOT,
            observe::LABEL_OPERATION,
        ]);

        if path_label_level > 0 {
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
    fn into_values(self, path_label_level: usize) -> Vec<&'a str> {
        let mut labels = Vec::with_capacity(6);

        labels.extend([
            self.scheme.into_static(),
            self.namespace,
            self.root,
            self.op.into_static(),
        ]);

        if let Some(path) = observe::path_label_value(self.path, path_label_level) {
            labels.push(path);
        }

        if let Some(error) = self.error {
            labels.push(error.into_static());
        }

        labels
    }
}
