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

use async_trait::async_trait;
use std::fmt::Debug;

use crate::{raw::*, *};

/// CachePolicy allows user to specify the policy while caching.
#[async_trait]
pub trait CachePolicy: Send + Sync + Debug + 'static {
    /// The policy for reading cache.
    ///
    /// `on_read` will return a [`CacheReadEntryIterator`] which can iterate
    /// serval [`CacheReadEntry`]. Cache layer will take different operations
    /// as specified by [`CacheReadEntry`].
    ///
    /// # Notes
    ///
    /// It's implementor's abailty to make sure the returning entry is
    /// correct.
    async fn on_read(&self, path: &str, offset: u64, size: u64) -> CacheReadEntryIterator;
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

/// CacheReadEntryIterator is a boxed iterator for [`CacheReadEntry`].
pub type CacheReadEntryIterator = Box<dyn Iterator<Item = CacheReadEntry> + Send>;

#[derive(Debug, Clone)]
/// CacheReadEntry indicates the operations that cache layer needs to take.
pub struct CacheReadEntry {
    /// cache_path is the path that we need to read or fill.
    cache_path: String,

    /// read_method indicates that do we need to read from cache.
    read_cache: bool,
    /// the range to read from cache file if we decide to read cache.
    cache_read_range: BytesRange,
    /// the range to read from inner file if we decide to skip cache
    /// or cache missed.
    inner_read_range: BytesRange,

    /// fill_method indicates that how we will fill the cache.
    fill_method: CacheFillMethod,
    /// the range to read from inner file to fill the cache.
    cache_fill_range: BytesRange,
}

impl CacheReadEntry {
    pub fn cache_path(&self) -> &str {
        &self.cache_path
    }

    pub fn read_cache(&self) -> bool {
        self.read_cache
    }

    pub fn cache_read_range(&self) -> BytesRange {
        self.cache_read_range
    }

    pub fn cache_read_op(&self) -> OpRead {
        OpRead::new().with_range(self.cache_read_range)
    }

    pub fn inner_read_range(&self) -> BytesRange {
        self.inner_read_range
    }

    pub fn inner_read_op(&self) -> OpRead {
        OpRead::new().with_range(self.inner_read_range)
    }

    pub fn fill_method(&self) -> CacheFillMethod {
        self.fill_method
    }

    pub fn cache_fill_range(&self) -> BytesRange {
        self.cache_fill_range
    }

    pub fn cache_fill_op(&self) -> OpRead {
        OpRead::new().with_range(self.cache_fill_range)
    }
}

#[derive(Debug)]
pub struct DefaultCachePolicy;

#[async_trait]
impl CachePolicy for DefaultCachePolicy {
    async fn on_read(&self, path: &str, offset: u64, size: u64) -> CacheReadEntryIterator {
        let br: BytesRange = (offset..offset + size).into();

        Box::new(
            vec![CacheReadEntry {
                cache_path: path.to_string(),

                read_cache: true,
                cache_read_range: br,
                inner_read_range: br,

                fill_method: CacheFillMethod::Async,
                cache_fill_range: br,
            }]
            .into_iter(),
        )
    }
}
