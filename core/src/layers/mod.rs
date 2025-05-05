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

//! `Layer` is the mechanism to intercept operations.

mod type_eraser;
pub(crate) use type_eraser::TypeEraseLayer;

mod error_context;
pub(crate) use error_context::ErrorContextLayer;

mod complete;
pub(crate) use complete::CompleteLayer;

mod concurrent_limit;
pub use concurrent_limit::ConcurrentLimitLayer;

mod immutable_index;
pub use immutable_index::ImmutableIndexLayer;

mod logging;
pub use logging::LoggingInterceptor;
pub use logging::LoggingLayer;

mod timeout;
pub use timeout::TimeoutLayer;

#[cfg(feature = "layers-blocking")]
mod blocking;
#[cfg(feature = "layers-blocking")]
pub use blocking::BlockingLayer;

#[cfg(feature = "layers-chaos")]
mod chaos;
#[cfg(feature = "layers-chaos")]
pub use chaos::ChaosLayer;

#[cfg(feature = "layers-metrics")]
mod metrics;
#[cfg(feature = "layers-metrics")]
pub use self::metrics::MetricsLayer;

#[cfg(feature = "layers-mime-guess")]
mod mime_guess;
#[cfg(feature = "layers-mime-guess")]
pub use self::mime_guess::MimeGuessLayer;

#[cfg(feature = "layers-prometheus")]
mod prometheus;
#[cfg(feature = "layers-prometheus")]
pub use self::prometheus::PrometheusLayer;
#[cfg(feature = "layers-prometheus")]
pub use self::prometheus::PrometheusLayerBuilder;

#[cfg(feature = "layers-prometheus-client")]
mod prometheus_client;
#[cfg(feature = "layers-prometheus-client")]
pub use self::prometheus_client::PrometheusClientLayer;
#[cfg(feature = "layers-prometheus-client")]
pub use self::prometheus_client::PrometheusClientLayerBuilder;

mod retry;
pub use self::retry::RetryInterceptor;
pub use self::retry::RetryLayer;

#[cfg(feature = "layers-tracing")]
mod tracing;
#[cfg(feature = "layers-tracing")]
pub use self::tracing::TracingLayer;

#[cfg(feature = "layers-fastrace")]
mod fastrace;
#[cfg(feature = "layers-fastrace")]
pub use self::fastrace::FastraceLayer;

#[cfg(feature = "layers-otel-metrics")]
mod otelmetrics;
#[cfg(feature = "layers-otel-metrics")]
pub use self::otelmetrics::OtelMetricsLayer;

#[cfg(feature = "layers-otel-trace")]
mod oteltrace;
#[cfg(feature = "layers-otel-trace")]
pub use self::oteltrace::OtelTraceLayer;

#[cfg(feature = "layers-throttle")]
mod throttle;
#[cfg(feature = "layers-throttle")]
pub use self::throttle::ThrottleLayer;

#[cfg(feature = "layers-await-tree")]
mod await_tree;
#[cfg(feature = "layers-await-tree")]
pub use self::await_tree::AwaitTreeLayer;

#[cfg(feature = "layers-async-backtrace")]
mod async_backtrace;
#[cfg(feature = "layers-async-backtrace")]
pub use self::async_backtrace::AsyncBacktraceLayer;

#[cfg(all(target_os = "linux", feature = "layers-dtrace"))]
mod dtrace;
#[cfg(all(target_os = "linux", feature = "layers-dtrace"))]
pub use self::dtrace::DtraceLayer;

pub mod observe;

mod correctness_check;
pub(crate) use correctness_check::CorrectnessCheckLayer;
mod capability_check;
pub use capability_check::CapabilityCheckLayer;
