// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Providing Layer trait and its implementations.
//!
//! # Available Layers
//!
//! - [`ImmutableIndexLayer`]: Add an immutable in-memory index for OpenDAL.
//! - [`LoggingLayer`]: Add logging for OpenDAL.
//! - [`MetadataCacheLayer`]: Add metadata cache for OpenDAL.
//! - [`MetricsLayer`]: Add metrics for OpenDAL, requires feature `layers-metrics`
//! - [`RetryLayer`]: Add retry for OpenDAL.
//! - [`SubdirLayer`]: Allow OpenDAL to switch directory.
//! - [`TracingLayer`]: Add tracing for OpenDAL, requires feature `layers-tracing`

mod layer;
pub use layer::Layer;

mod immutable_index;
pub use immutable_index::ImmutableIndexLayer;

mod logging;
pub use logging::LoggingLayer;

#[cfg(feature = "layers-metadata-cache")]
mod metadata_cache;
#[cfg(feature = "layers-metadata-cache")]
pub use metadata_cache::MetadataCacheLayer;

#[cfg(feature = "layers-metrics")]
mod metrics;
#[cfg(feature = "layers-metrics")]
pub use self::metrics::MetricsLayer;

mod retry;
pub use self::retry::RetryLayer;

mod subdir;
pub use subdir::SubdirLayer;

#[cfg(feature = "layers-tracing")]
mod tracing;
#[cfg(feature = "layers-tracing")]
pub use self::tracing::TracingLayer;
