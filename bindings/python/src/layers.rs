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

use opendal::Operator;
use pyo3::prelude::*;

use crate::*;

pub trait PythonLayer: Send + Sync {
    fn layer(&self, op: Operator) -> Operator;
}

#[pyclass(module = "opendal.layers", subclass)]
pub struct Layer(pub Box<dyn PythonLayer>);

#[pyclass(module = "opendal.layers", extends=Layer)]
#[derive(Clone)]
pub struct RetryLayer(ocore::layers::RetryLayer);

impl PythonLayer for RetryLayer {
    fn layer(&self, op: Operator) -> Operator {
        op.layer(self.0.clone())
    }
}

#[pymethods]
impl RetryLayer {
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

#[pyclass(module = "opendal.layers", extends=Layer)]
#[derive(Clone)]
pub struct ConcurrentLimitLayer(ocore::layers::ConcurrentLimitLayer);

impl PythonLayer for ConcurrentLimitLayer {
    fn layer(&self, op: Operator) -> Operator {
        op.layer(self.0.clone())
    }
}

#[pymethods]
impl ConcurrentLimitLayer {
    #[new]
    #[pyo3(signature = (limit))]
    fn new(limit: usize) -> PyResult<PyClassInitializer<Self>> {
        let concurrent_limit = Self(ocore::layers::ConcurrentLimitLayer::new(limit));
        let class = PyClassInitializer::from(Layer(Box::new(concurrent_limit.clone())))
            .add_subclass(concurrent_limit);

        Ok(class)
    }
}
