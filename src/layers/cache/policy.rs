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

use std::fmt::Debug;

use crate::*;

/// CachePolicy allows user to specify the policy while caching.
pub trait CachePolicy: Send + Sync + Debug + 'static {
    /// The policy for reading cache.
    ///
    /// Returning result is (cache_read, cache_fill_method).
    ///
    /// ## Cache Read
    ///
    /// `cache_read` indicate whether read from cache or not.
    ///
    /// - `true`: read from cache first, if missing, read from inner instead.
    /// - `false`: read from inner directly without touching cache.
    ///
    /// ## Cache Fill Method
    ///
    /// `cache_fill_method` indicate the cache filling method after missing.
    ///
    /// The detailed behavior please visit [`CacheFillMethod`].
    fn on_read(&self, path: &str, args: &OpRead) -> (CacheReadMethod, CacheFillMethod);
}

/// CacheReadMethod specify the cache read method before start reading.
#[derive(Debug, Clone)]
pub enum CacheReadMethod {
    /// Don't read from cache
    Skip,
    /// Always cache the whole object content.
    Whole,
    /// Cache the object content in parts with fixed size.
    Fixed(u64),
}

/// CacheFillMethod specify the cache fill method while cache missing.
#[derive(Debug, Clone, Copy)]
pub enum CacheFillMethod {
    /// Don't fill cache.
    ///
    /// Return data from inner directly without fill into the cache.
    Skip,
    /// Fill cache in sync way.
    ///
    /// Write data into cache first and than read from cache.
    Sync,
    /// Fill cache in async way.
    ///
    /// Spawn an async task to runtime and return data directly.
    Async,
}

#[derive(Debug)]
pub struct DefaultCachePolicy;

impl CachePolicy for DefaultCachePolicy {
    fn on_read(&self, _: &str, _: &OpRead) -> (CacheReadMethod, CacheFillMethod) {
        (CacheReadMethod::Whole, CacheFillMethod::Sync)
    }
}
