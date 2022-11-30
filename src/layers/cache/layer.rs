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
use std::sync::Arc;

use super::*;
use crate::raw::*;
use crate::*;

/// ContentCacheLayer will add content data cache support for OpenDAL.
///
/// # Notes
///
/// This layer only maintains its own states. Users should care about the cache
/// consistency by themselves. For example, in the following situations, users
/// could get out-dated metadata cache:
///
/// - Users have operations on underlying operator directly.
/// - Other nodes have operations on underlying storage directly.
/// - Concurrent read/write/delete on the same path.
///
/// To make sure content cache consistent across the cluster, please make sure
/// all nodes in the cluster use the same cache services like redis or tikv.
///
/// # Examples
///
/// ## Use memory as cache.
///
/// ```
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::layers::CacheLayer;
/// use opendal::layers::CacheStrategy;
/// use opendal::services::memory;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(CacheLayer::new(
///         Operator::from_env(Scheme::Memory).expect("must init"),
///     ));
/// ```
///
/// ## Use memory and fs as a two level cache.
///
/// ```
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::layers::CacheLayer;
/// use opendal::layers::CacheStrategy;
/// use opendal::services::memory;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(CacheLayer::new(
///         Operator::from_env(Scheme::Fs).expect("must init"),
///     ))
///     .layer(CacheLayer::new(
///         Operator::from_env(Scheme::Memory).expect("must init"),
///     ));
/// ```
#[derive(Debug, Clone)]
pub struct CacheLayer {
    cache: Arc<dyn Accessor>,
    strategy: CacheStrategy,
    policy: Arc<dyn CachePolicy>,
}

impl CacheLayer {
    /// Create a new metadata cache layer.
    pub fn new(cache: Operator) -> Self {
        Self {
            cache: cache.inner(),
            strategy: CacheStrategy::Whole,
            policy: Arc::new(DefaultCachePolicy),
        }
    }

    /// Update the cache layer's strategy.
    pub fn with_strategy(mut self, strategy: CacheStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Update the cache layer's logic.
    pub fn with_policy(mut self, policy: impl CachePolicy) -> Self {
        self.policy = Arc::new(policy);
        self
    }
}

impl Layer for CacheLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(CacheAccessor::new(
            inner,
            self.cache.clone(),
            self.strategy.clone(),
            self.policy.clone(),
        ))
    }
}
