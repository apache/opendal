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

use crate::*;
use opendal::Operator;

pub trait PythonLayer: Send + Sync {
    fn layer(&self, op: Operator) -> Operator;
}

/// Layer is used to intercept the operations on the underlying storage.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.layers", subclass)]
pub struct Layer(pub Box<dyn PythonLayer>);

/// RetryLayer
///
/// A layer that retries operations that fail with temporary errors.
///
/// Operations are retried if they fail with an error for which
/// `Error.is_temporary` returns `True`. If all retries are exhausted,
/// the error is marked as persistent and then returned.
///
/// Notes:
///     After an operation on a `Reader` or `Writer` has failed through
///     all retries, the object is in an undefined state. Reusing it
///     can lead to a panic.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.layers", extends=Layer)]
#[derive(Clone)]
pub struct RetryLayer(ocore::layers::RetryLayer);

impl PythonLayer for RetryLayer {
    fn layer(&self, op: Operator) -> Operator {
        op.layer(self.0.clone())
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl RetryLayer {
    /// Create a new RetryLayer.
    ///
    /// # Arguments
    ///
    /// * `max_times` - The maximum number of retries.
    /// * `factor` - The exponential backoff factor.
    /// * `jitter` - Whether to add jitter to the backoff.
    /// * `max_delay` - The maximum delay between retries.
    /// * `min_delay` - The minimum delay between retries.
    #[gen_stub(override_return_type(type_repr = "RetryLayer"))]
    #[new]
    #[pyo3(signature = (
        max_times = None,
        factor = None,
        jitter = false,
        max_delay = None,
        min_delay = None
    ))]
    fn new(
        max_times: Option<usize>,
        factor: Option<f32>,
        jitter: bool,
        max_delay: Option<f64>,
        min_delay: Option<f64>,
    ) -> PyResult<PyClassInitializer<Self>> {
        let mut retry = ocore::layers::RetryLayer::default();
        if let Some(max_times) = max_times {
            retry = retry.with_max_times(max_times);
        }
        if let Some(factor) = factor {
            retry = retry.with_factor(factor);
        }
        if jitter {
            retry = retry.with_jitter();
        }
        if let Some(max_delay) = max_delay {
            retry = retry.with_max_delay(Duration::from_micros((max_delay * 1000000.0) as u64));
        }
        if let Some(min_delay) = min_delay {
            retry = retry.with_min_delay(Duration::from_micros((min_delay * 1000000.0) as u64));
        }

        let retry_layer = Self(retry);
        let class = PyClassInitializer::from(Layer(Box::new(retry_layer.clone())))
            .add_subclass(retry_layer);

        Ok(class)
    }
}

/// ConcurrentLimitLayer.
///
/// A layer that adds a concurrent request limit to control the number of
/// concurrent connections could be established between OpenDAL and
/// underlying storage services.
///
/// Notes:
///     Users can control how many concurrent connections could be established
///     between OpenDAL and underlying storage services.
///
///     All operators wrapped by this layer will share a common semaphore. This
///     allows you to reuse the same layer across multiple operators, ensuring
///     that the total number of concurrent requests across the entire
///     application does not exceed the limit.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.layers", extends=Layer)]
#[derive(Clone)]
pub struct ConcurrentLimitLayer(ocore::layers::ConcurrentLimitLayer);

impl PythonLayer for ConcurrentLimitLayer {
    fn layer(&self, op: Operator) -> Operator {
        op.layer(self.0.clone())
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ConcurrentLimitLayer {
    /// Create a new ConcurrentLimitLayer.
    ///
    /// # Arguments
    ///
    /// * `limit` - The maximum number of concurrent requests.
    #[gen_stub(override_return_type(type_repr = "ConcurrentLimitLayer"))]
    #[new]
    #[pyo3(signature = (limit))]
    fn new(limit: usize) -> PyResult<PyClassInitializer<Self>> {
        let concurrent_limit = Self(ocore::layers::ConcurrentLimitLayer::new(limit));
        let class = PyClassInitializer::from(Layer(Box::new(concurrent_limit.clone())))
            .add_subclass(concurrent_limit);

        Ok(class)
    }
}

/// MimeGuessLayer.
///
/// A layer to automatically set `Content-Type` from a path's extension.
///
/// This layer uses [`mime_guess`](https://crates.io/crates/mime_guess)
/// to infer the `Content-Type`.
///
/// Notes:
///     This layer will NOT override a `Content-Type` that has already
///     been set, either manually or by the backend service. It is only
///     applied if no content type is present.
///
///     A `Content-Type` is not guaranteed. If the file extension is
///     uncommon or unknown, the content type will remain unset.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.layers", extends=Layer)]
#[derive(Clone)]
pub struct MimeGuessLayer(ocore::layers::MimeGuessLayer);

impl PythonLayer for MimeGuessLayer {
    fn layer(&self, op: Operator) -> Operator {
        op.layer(self.0.clone())
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl MimeGuessLayer {
    /// Create a new MimeGuessLayer.
    #[gen_stub(override_return_type(type_repr = "MimeGuessLayer"))]
    #[new]
    #[pyo3(signature = ())]
    fn new() -> PyResult<PyClassInitializer<Self>> {
        let mime_guess_layer = Self(ocore::layers::MimeGuessLayer::default());
        let class = PyClassInitializer::from(Layer(Box::new(mime_guess_layer.clone())))
            .add_subclass(mime_guess_layer);
        Ok(class)
    }
}
