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

use metrics::counter;
use metrics::histogram;
use metrics::Label;

use crate::layers::observe;
use crate::raw::*;
use crate::*;

/// Add [metrics](https://docs.rs/metrics/) for every operation.
///
/// # Metrics
///
/// We provide several metrics, please see the documentation of [`observe`] module.
///
/// # Notes
///
/// Please make sure the exporter has been pulled in regular time.
/// Otherwise, the histogram data collected by `requests_duration_seconds`
/// could result in OOM.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # use opendal::layers::MetricsLayer;
/// # use opendal::services;
/// # use opendal::Operator;
///
/// # fn main() -> Result<()> {
///     let _ = Operator::new(services::Memory::default())?
///         .layer(MetricsLayer::default())
///         .finish();
///     Ok(())
/// # }
/// ```
///
/// # Output
///
/// OpenDAL is using [`metrics`](https://docs.rs/metrics/latest/metrics/) for metrics internally.
///
/// To enable metrics output, please enable one of the exporters that `metrics` supports.
///
/// Take [`metrics_exporter_prometheus`](https://docs.rs/metrics-exporter-prometheus/latest/metrics_exporter_prometheus/) as an example:
///
/// ```ignore
/// let builder = PrometheusBuilder::new();
/// builder.install().expect("failed to install recorder/exporter");
/// let handle = builder.install_recorder().expect("failed to install recorder");
/// let (recorder, exporter) = builder.build().expect("failed to build recorder/exporter");
/// let recorder = builder.build_recorder().expect("failed to build recorder");
/// ```
#[derive(Clone, Debug)]
pub struct MetricsLayer {
    path_label_level: usize,
}

impl Default for MetricsLayer {
    fn default() -> Self {
        Self {
            path_label_level: 0,
        }
    }
}

impl MetricsLayer {
    /// Set the level of path label.
    ///
    /// - level = 0: we will ignore the path label.
    /// - level > 0: the path label will be the path split by "/" and get the last n level,
    ///   if n=1 and input path is "abc/def/ghi", and then we will get "abc/" as the path label.
    pub fn path_label(mut self, level: usize) -> Self {
        self.path_label_level = level;
        self
    }
}

impl<A: Access> Layer<A> for MetricsLayer {
    type LayeredAccess = observe::MetricsAccessor<A, MetricsInterceptor>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let interceptor = MetricsInterceptor {
            path_label_level: self.path_label_level,
        };
        observe::MetricsLayer::new(interceptor).layer(inner)
    }
}

#[derive(Clone, Debug)]
pub struct MetricsInterceptor {
    path_label_level: usize,
}

impl observe::MetricsIntercept for MetricsInterceptor {
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
            namespace,
            root,
            path,
            operation: op,
            error: None,
        }
        .into_labels(self.path_label_level);
        histogram!(observe::METRIC_OPERATION_DURATION_SECONDS.name(), labels).record(duration)
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
            namespace,
            root,
            path,
            operation: op,
            error: None,
        }
        .into_labels(self.path_label_level);
        histogram!(observe::METRIC_OPERATION_BYTES.name(), labels).record(bytes as f64)
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
            namespace,
            root,
            path,
            operation: op,
            error: Some(error),
        }
        .into_labels(self.path_label_level);
        counter!(observe::METRIC_OPERATION_ERRORS_TOTAL.name(), labels).increment(1)
    }
}

struct OperationLabels<'a> {
    scheme: Scheme,
    namespace: Arc<String>,
    root: Arc<String>,
    path: &'a str,
    operation: Operation,
    error: Option<ErrorKind>,
}

impl<'a> OperationLabels<'a> {
    /// labels:
    ///
    /// 1. `["scheme", "namespace", "root", "operation"]`
    /// 2. `["scheme", "namespace", "root", "operation", "path"]`
    /// 3. `["scheme", "namespace", "root", "operation", "error"]`
    /// 4. `["scheme", "namespace", "root", "operation", "path", "error"]`
    fn into_labels(self, path_label_level: usize) -> Vec<Label> {
        let mut labels = Vec::with_capacity(6);

        labels.extend([
            Label::new(observe::LABEL_SCHEME, self.scheme.into_static()),
            Label::new(observe::LABEL_NAMESPACE, (*self.namespace).clone()),
            Label::new(observe::LABEL_ROOT, (*self.root).clone()),
            Label::new(observe::LABEL_OPERATION, self.operation.into_static()),
        ]);

        if let Some(path) = observe::path_label_value(self.path, path_label_level) {
            labels.push(Label::new(observe::LABEL_PATH, path.to_owned()));
        }

        if let Some(error) = self.error {
            labels.push(Label::new(observe::LABEL_ERROR, error.into_static()));
        }

        labels
    }
}
