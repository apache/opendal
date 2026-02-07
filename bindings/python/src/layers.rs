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

/// Layers are used to intercept the operations on the underlying storage.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.layers", subclass)]
pub struct Layer(pub Box<dyn PythonLayer>);

/// A layer that retries operations that fail with temporary errors.
///
/// Operations are retried if they fail with an error for which
/// `Error.is_temporary` returns `True`. If all retries are exhausted,
/// the error is marked as persistent and then returned.
///
/// Notes
/// -----
/// After an operation on a `Reader` or `Writer` has failed through
/// all retries, the object is in an undefined state. Reusing it
/// can lead to exceptions.
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
    /// Parameters
    /// ----------
    /// max_times : Optional[int]
    ///     Maximum number of retry attempts. Defaults to ``3``.
    /// factor : Optional[float]
    ///     Backoff factor applied between retries. Defaults to ``2.0``.
    /// jitter : bool
    ///     Whether to apply jitter to the backoff. Defaults to ``False``.
    /// max_delay : Optional[float]
    ///     Maximum delay (in seconds) between retries. Defaults to ``60.0``.
    /// min_delay : Optional[float]
    ///     Minimum delay (in seconds) between retries. Defaults to ``1.0``.
    ///
    /// Returns
    /// -------
    /// RetryLayer
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
            retry = retry.with_max_delay(Duration::from_micros((max_delay * 1_000_000.0) as u64));
        }
        if let Some(min_delay) = min_delay {
            retry = retry.with_min_delay(Duration::from_micros((min_delay * 1_000_000.0) as u64));
        }

        let retry_layer = Self(retry);
        let class = PyClassInitializer::from(Layer(Box::new(retry_layer.clone())))
            .add_subclass(retry_layer);

        Ok(class)
    }
}

/// A layer that limits the number of concurrent operations.
///
/// Notes
/// -----
/// All operators wrapped by this layer will share a common semaphore. This
/// allows you to reuse the same layer across multiple operators, ensuring
/// that the total number of concurrent requests across the entire
/// application does not exceed the limit.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.layers", extends = Layer)]
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
    /// Parameters
    /// ----------
    /// limit : int
    ///     Maximum number of concurrent operations allowed.
    ///
    /// Returns
    /// -------
    /// ConcurrentLimitLayer
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

/// A layer that guesses MIME types for objects based on their paths or content.
///
/// This layer uses the `mime_guess` crate
/// (see https://crates.io/crates/mime_guess) to infer the
/// ``Content-Type``.
///
/// Notes
/// -----
/// This layer will not override a ``Content-Type`` that has already
/// been set, either manually or by the backend service. It is only
/// applied if no content type is present.
///
/// A ``Content-Type`` is not guaranteed. If the file extension is
/// uncommon or unknown, the content type will remain unset.
#[gen_stub_pyclass]
#[pyclass(module = "opendal.layers", extends = Layer)]
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
    ///
    /// Returns
    /// -------
    /// MimeGuessLayer
    #[gen_stub(override_return_type(type_repr = "MimeGuessLayer"))]
    #[new]
    fn new() -> PyResult<PyClassInitializer<Self>> {
        let mime_guess = Self(ocore::layers::MimeGuessLayer::default());
        let class =
            PyClassInitializer::from(Layer(Box::new(mime_guess.clone()))).add_subclass(mime_guess);
        Ok(class)
    }
}
