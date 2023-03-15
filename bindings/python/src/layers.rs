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

use ::opendal as od;
use chrono::Duration;
use pyo3::exceptions::PyOverflowError;
use pyo3::prelude::*;

#[derive(FromPyObject)]
pub enum Layer {
    ConcurrentLimit(ConcurrentLimitLayer),
    ImmutableIndex(ImmutableIndexLayer),
    Retry(RetryLayer),
}

#[pyclass(module = "opendal.layers")]
#[derive(Clone)]
pub struct ConcurrentLimitLayer(pub od::layers::ConcurrentLimitLayer);

#[pymethods]
impl ConcurrentLimitLayer {
    #[new]
    fn new(permits: usize) -> Self {
        Self(od::layers::ConcurrentLimitLayer::new(permits))
    }
}

#[pyclass(module = "opendal.layers")]
#[derive(Clone)]
pub struct ImmutableIndexLayer(pub od::layers::ImmutableIndexLayer);

#[pymethods]
impl ImmutableIndexLayer {
    #[new]
    fn new() -> Self {
        Self(od::layers::ImmutableIndexLayer::default())
    }

    fn insert(&mut self, key: String) {
        self.0.insert(key);
    }
}

#[pyclass(module = "opendal.layers")]
#[derive(Clone)]
pub struct RetryLayer(pub od::layers::RetryLayer);

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
        max_delay: Option<Duration>,
        min_delay: Option<Duration>,
    ) -> PyResult<Self> {
        let mut retry = od::layers::RetryLayer::default();
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
            retry = retry.with_max_delay(
                max_delay
                    .to_std()
                    .map_err(|err| PyOverflowError::new_err(err.to_string()))?,
            );
        }
        if let Some(min_delay) = min_delay {
            retry = retry.with_min_delay(
                min_delay
                    .to_std()
                    .map_err(|err| PyOverflowError::new_err(err.to_string()))?,
            );
        }
        Ok(Self(retry))
    }
}

pub fn create_submodule(py: Python) -> PyResult<&PyModule> {
    let submod = PyModule::new(py, "layers")?;
    submod.add_class::<ConcurrentLimitLayer>()?;
    submod.add_class::<ImmutableIndexLayer>()?;
    submod.add_class::<RetryLayer>()?;
    Ok(submod)
}
