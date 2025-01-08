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

use opentelemetry::global;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Histogram;
use opentelemetry::KeyValue;

use crate::layers::observe;
use crate::raw::*;
use crate::*;

/// Add [opentelemetry::metrics](https://docs.rs/opentelemetry/latest/opentelemetry/metrics/index.html) for every operation.
///
/// # Examples
///
/// ```no_run
/// # use opendal::layers::OtelMetricsLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(OtelMetricsLayer::default())
///     .finish();
/// Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct OtelMetricsLayer {
    path_label_level: usize,
}

impl OtelMetricsLayer {
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

impl<A: Access> Layer<A> for OtelMetricsLayer {
    type LayeredAccess = observe::MetricsAccessor<A, OtelMetricsInterceptor>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let meter = global::meter("opendal.operation");
        let duration_seconds = meter
            .f64_histogram("duration")
            .with_description("Duration of operations")
            .with_unit("second")
            .with_boundaries(exponential_boundary(0.01, 2.0, 16))
            .build();
        let bytes = meter
            .u64_histogram("size")
            .with_description("Size of operations")
            .with_unit("byte")
            .with_boundaries(exponential_boundary(1.0, 2.0, 16))
            .build();
        let errors = meter
            .u64_counter("errors")
            .with_description("Number of operation errors")
            .build();
        let interceptor = OtelMetricsInterceptor {
            duration_seconds,
            bytes,
            errors,
            path_label_level: self.path_label_level,
        };
        observe::MetricsLayer::new(interceptor).layer(inner)
    }
}

#[derive(Clone, Debug)]
pub struct OtelMetricsInterceptor {
    duration_seconds: Histogram<f64>,
    bytes: Histogram<u64>,
    errors: Counter<u64>,
    path_label_level: usize,
}

impl observe::MetricsIntercept for OtelMetricsInterceptor {
    fn observe_operation_duration_seconds(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        path: &str,
        op: Operation,
        duration: Duration,
    ) {
        let attributes = self.create_attributes(scheme, namespace, root, path, op, None);
        self.duration_seconds
            .record(duration.as_secs_f64(), &attributes);
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
        let attributes = self.create_attributes(scheme, namespace, root, path, op, None);
        self.bytes.record(bytes as u64, &attributes);
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
        let attributes = self.create_attributes(scheme, namespace, root, path, op, Some(error));
        self.errors.add(1, &attributes);
    }
}

impl OtelMetricsInterceptor {
    fn create_attributes(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        path: &str,
        operation: Operation,
        error: Option<ErrorKind>,
    ) -> Vec<KeyValue> {
        let mut attributes = Vec::with_capacity(6);

        attributes.extend([
            KeyValue::new(observe::LABEL_SCHEME, scheme.into_static()),
            KeyValue::new(observe::LABEL_NAMESPACE, (*namespace).clone()),
            KeyValue::new(observe::LABEL_ROOT, (*root).clone()),
            KeyValue::new(observe::LABEL_OPERATION, operation.into_static()),
        ]);

        if let Some(path) = observe::path_label_value(path, self.path_label_level) {
            attributes.push(KeyValue::new(observe::LABEL_PATH, path.to_owned()));
        }

        if let Some(error) = error {
            attributes.push(KeyValue::new(observe::LABEL_ERROR, error.into_static()));
        }

        attributes
    }
}

fn exponential_boundary(start: f64, factor: f64, count: usize) -> Vec<f64> {
    let mut boundaries = Vec::with_capacity(count);
    let mut current = start;
    for _ in 0..count {
        boundaries.push(current);
        current *= factor;
    }
    boundaries
}
