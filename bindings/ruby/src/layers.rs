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
use std::sync::Mutex;
use std::time::Duration;

use magnus::class;
use magnus::prelude::*;
use magnus::Error;
use magnus::RModule;
use magnus::Ruby;

use opendal::raw::Accessor as OCoreAccessor;
use opendal::raw::Layer as OCoreLayer;

use crate::operator::Operator;
use crate::*;

// Wraps OpenDAL layers
//
// Each layer is represented as a variant of the `Layer` enum. We use this approach because:
// 1. We don't require dynamic plugin registration.
// 2. The set of layers is known at compile time.
// 3. Enums are a native, efficient Rust construct with zero-cost dispatch.
// 4. OpenDAL's layers form a closed set.
//
// Ruby sees this as a class, but Rust handles variant dispatch. magnus also generates additional
// classes to each enum variant.
//
// We’re not using `rb_data_type_t.parent` because we don’t need inheritance-style casting.
// Magnus sets the correct class via `.class_for()` using the enum variant.
// Rust tracks the exact variant in memory; Ruby doesn’t need to know.
#[magnus::wrap(class = "OpenDAL::Layer", free_immediately, size)]
pub enum Layer {
    #[magnus(class = "OpenDAL::RetryLayer")]
    Retry(Arc<Mutex<ocore::layers::RetryLayer>>),
    #[magnus(class = "OpenDAL::ConcurrentLimitLayer")]
    ConcurrentLimit(Arc<Mutex<ocore::layers::ConcurrentLimitLayer>>),
    #[magnus(class = "OpenDAL::ThrottleLayer")]
    Throttle(Arc<Mutex<ocore::layers::ThrottleLayer>>),
    #[magnus(class = "OpenDAL::TimeoutLayer")]
    Timeout(Arc<Mutex<ocore::layers::TimeoutLayer>>),
}

impl Layer {
    fn new_retry_layer() -> Self {
        Layer::Retry(Arc::new(Mutex::new(ocore::layers::RetryLayer::default())))
    }

    fn new_concurrent_limit_layer(permits: usize) -> Self {
        Layer::ConcurrentLimit(Arc::new(Mutex::new(
            ocore::layers::ConcurrentLimitLayer::new(permits),
        )))
    }

    fn new_throttle_layer(bandwidth: u32, burst: u32) -> Self {
        Layer::Throttle(Arc::new(Mutex::new(ocore::layers::ThrottleLayer::new(
            bandwidth, burst,
        ))))
    }

    fn new_timeout_layer(ruby: &Ruby, timeout: f64, io_timeout: f64) -> Result<Self, Error> {
        let layer = ocore::layers::TimeoutLayer::new()
            .with_timeout(parse_duration(timeout, ruby)?)
            .with_io_timeout(parse_duration(io_timeout, ruby)?);
        Ok(Layer::Timeout(Arc::new(Mutex::new(layer))))
    }
}

fn parse_duration(val: f64, ruby: &Ruby) -> Result<Duration, Error> {
    Duration::try_from_secs_f64(val).map_err(|e| {
        Error::new(
            ruby.exception_arg_error(),
            format!("invalid float duration: {e}"),
        )
    })
}

impl Layer {
    // OpenDAL core adds layers to an `Operator`, returning a new `Operator`.
    // Users typically apply layers directly to an Operator within a single-threaded context.
    // However, we still accommodate occasional cases where layers may need to be accessed
    // across Ruby threads. Hence, we use `Arc<Mutex<_>>` for thread-safe access.
    pub fn apply_to(&self, ruby: &Ruby, operator: &Operator) -> Result<Operator, Error> {
        match self {
            Layer::Retry(inner) => Self::call(inner, ruby, operator, "RetryLayer"),
            Layer::ConcurrentLimit(inner) => {
                Self::call(inner, ruby, operator, "ConcurrentLimitLayer")
            }
            Layer::Throttle(inner) => Self::call(inner, ruby, operator, "ThrottleLayer"),
            Layer::Timeout(inner) => Self::call(inner, ruby, operator, "TimeoutLayer"),
        }
    }

    fn call<T>(
        inner: &Arc<Mutex<T>>,
        ruby: &Ruby,
        operator: &Operator,
        name: &'static str,
    ) -> Result<Operator, Error>
    where
        T: OCoreLayer<OCoreAccessor> + Clone + Send + Sync + 'static,
    {
        let guard = inner.lock().map_err(|_| {
            Error::new(
                ruby.exception_runtime_error(),
                format!("poisoned {} mutex", name),
            )
        })?;

        let layered = operator.operator.clone().layer(guard.clone());

        Ok(Operator::from_operator(layered))
    }
}

pub fn include(gem_module: &RModule) -> Result<(), Error> {
    let layer_class = gem_module.define_class("Layer", class::object())?;
    layer_class.undef_default_alloc_func();

    let retry_layer_class = gem_module.define_class("RetryLayer", layer_class)?;
    retry_layer_class.define_singleton_method("new", function!(Layer::new_retry_layer, 0))?;

    let concurrent_limit_layer_class =
        gem_module.define_class("ConcurrentLimitLayer", layer_class)?;
    concurrent_limit_layer_class
        .define_singleton_method("new", function!(Layer::new_concurrent_limit_layer, 1))?;

    let throttle_layer_class = gem_module.define_class("ThrottleLayer", layer_class)?;
    throttle_layer_class.define_singleton_method("new", function!(Layer::new_throttle_layer, 2))?;

    let timeout_layer_class = gem_module.define_class("TimeoutLayer", layer_class)?;
    timeout_layer_class.define_singleton_method("new", function!(Layer::new_timeout_layer, 2))?;

    Ok(())
}
