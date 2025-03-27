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
// In the Ruby community, people often refer layers as middlewares.
// For public class names, we use "Middleware".
// When applying actions with an OpenDAL layer, we refer to layers and interfaces simply as layers,
// since a "layer" is an implementation detail.
//
// Each layer is represented as a variant of the `Middleware` enum.
//
// We use this approach because:
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
#[magnus::wrap(class = "OpenDAL::Middleware", free_immediately, size)]
pub enum Middleware {
    #[magnus(class = "OpenDAL::RetryMiddleware")]
    Retry(Arc<Mutex<ocore::layers::RetryLayer>>),
    #[magnus(class = "OpenDAL::ConcurrentLimitMiddleware")]
    ConcurrentLimit(Arc<Mutex<ocore::layers::ConcurrentLimitLayer>>),
    #[magnus(class = "OpenDAL::ThrottleMiddleware")]
    Throttle(Arc<Mutex<ocore::layers::ThrottleLayer>>),
    #[magnus(class = "OpenDAL::TimeoutMiddleware")]
    Timeout(Arc<Mutex<ocore::layers::TimeoutLayer>>),
}

impl Middleware {
    fn new_retry_middleware() -> Self {
        Middleware::Retry(Arc::new(Mutex::new(ocore::layers::RetryLayer::default())))
    }

    fn new_concurrent_limit_middleware(permits: usize) -> Self {
        Middleware::ConcurrentLimit(Arc::new(Mutex::new(
            ocore::layers::ConcurrentLimitLayer::new(permits),
        )))
    }

    fn new_throttle_middleware(bandwidth: u32, burst: u32) -> Self {
        Middleware::Throttle(Arc::new(Mutex::new(ocore::layers::ThrottleLayer::new(
            bandwidth, burst,
        ))))
    }

    fn new_timeout_middleware(ruby: &Ruby, timeout: f64, io_timeout: f64) -> Result<Self, Error> {
        let layer = ocore::layers::TimeoutLayer::new()
            .with_timeout(parse_duration(timeout, ruby)?)
            .with_io_timeout(parse_duration(io_timeout, ruby)?);
        Ok(Middleware::Timeout(Arc::new(Mutex::new(layer))))
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

impl Middleware {
    // OpenDAL core adds layers to an `Operator`, returning a new `Operator`.
    // Users typically apply layers directly to an Operator within a single-threaded context.
    // However, we still accommodate occasional cases where layers may need to be accessed
    // across Ruby threads. Hence, we use `Arc<Mutex<_>>` for thread-safe access.
    pub fn apply_to(&self, ruby: &Ruby, operator: &Operator) -> Result<Operator, Error> {
        match self {
            Middleware::Retry(inner) => Self::call(inner, ruby, operator, "RetryMiddleware"),
            Middleware::ConcurrentLimit(inner) => {
                Self::call(inner, ruby, operator, "ConcurrentLimitMiddleware")
            }
            Middleware::Throttle(inner) => Self::call(inner, ruby, operator, "ThrottleMiddleware"),
            Middleware::Timeout(inner) => Self::call(inner, ruby, operator, "TimeoutMiddleware"),
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
    let middleware_class = gem_module.define_class("Middleware", class::object())?;
    middleware_class.undef_default_alloc_func();

    let retry_middleware_class = gem_module.define_class("RetryMiddleware", middleware_class)?;
    retry_middleware_class
        .define_singleton_method("new", function!(Middleware::new_retry_middleware, 0))?;

    let concurrent_limit_middleware_class =
        gem_module.define_class("ConcurrentLimitMiddleware", middleware_class)?;
    concurrent_limit_middleware_class.define_singleton_method(
        "new",
        function!(Middleware::new_concurrent_limit_middleware, 1),
    )?;

    let throttle_middleware_class =
        gem_module.define_class("ThrottleMiddleware", middleware_class)?;
    throttle_middleware_class
        .define_singleton_method("new", function!(Middleware::new_throttle_middleware, 2))?;

    let timeout_middleware_class =
        gem_module.define_class("TimeoutMiddleware", middleware_class)?;
    timeout_middleware_class
        .define_singleton_method("new", function!(Middleware::new_timeout_middleware, 2))?;

    Ok(())
}
