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

mod simulate;
pub use simulate::SimulateLayer;

mod concurrent_limit;
pub use concurrent_limit::ConcurrentLimitLayer;

mod immutable_index;
pub use immutable_index::ImmutableIndexLayer;

mod logging;
pub use logging::LoggingInterceptor;
pub use logging::LoggingLayer;

mod timeout;
pub use timeout::TimeoutLayer;

#[cfg(feature = "layers-chaos")]
mod chaos;
#[cfg(feature = "layers-chaos")]
pub use chaos::ChaosLayer;

mod retry;
pub use self::retry::RetryInterceptor;
pub use self::retry::RetryLayer;

#[cfg(feature = "layers-otel-trace")]
mod oteltrace;
#[cfg(feature = "layers-otel-trace")]
pub use self::oteltrace::OtelTraceLayer;

#[cfg(feature = "layers-throttle")]
mod throttle;
#[cfg(feature = "layers-throttle")]
pub use self::throttle::ThrottleLayer;

#[cfg(all(target_os = "linux", feature = "layers-dtrace"))]
mod dtrace;
#[cfg(all(target_os = "linux", feature = "layers-dtrace"))]
pub use self::dtrace::DtraceLayer;

mod correctness_check;
pub(crate) use correctness_check::CorrectnessCheckLayer;

mod http_client;
pub use http_client::HttpClientLayer;
