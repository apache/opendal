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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use opendal_core::raw::{Layer, Servicer};
use opendal_core::{Error, ErrorKind, Operator, Result};

mod flight;
mod key;
mod mutation;
mod reader;
mod record;
mod service;
mod state;

use service::CacheService;
use state::State;

/// The source path supplied to the read cache policy.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct CacheReadRequest<'a> {
    path: &'a str,
}

impl<'a> CacheReadRequest<'a> {
    /// Return the normalized source path.
    pub fn path(&self) -> &'a str {
        self.path
    }
}

/// Select how the cache handles a reader.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheDecision {
    /// Bypass the cache completely for this reader.
    Bypass,
    /// Use the cache with the layer's default TTL.
    Cache,
    /// Use the cache with this metadata TTL.
    /// Zero revalidates before serving. The maximum disables TTL revalidation.
    CacheFor(Duration),
}

/// Decide by source path whether to cache reads and how long their metadata remains fresh.
pub trait CachePolicy: Send + Sync + 'static {
    /// Decide once for the reader, before any I/O.
    fn on_read(&self, req: &CacheReadRequest<'_>) -> CacheDecision;
}

impl<F> CachePolicy for F
where
    F: Fn(&CacheReadRequest<'_>) -> CacheDecision + Send + Sync + 'static,
{
    fn on_read(&self, req: &CacheReadRequest<'_>) -> CacheDecision {
        self(req)
    }
}

const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// Cache aligned source read chunks in another OpenDAL operator.
///
/// The default chunk size is 1 MiB, capped by the cache operator's advertised
/// maximum total write size. The default metadata TTL is 60 seconds.
/// Use a cache backend with capacity eviction to reclaim chunks from old object
/// identities. Use separate cache roots or instances for different tenants,
/// endpoints, or credentials because fresh cache hits skip source I/O.
///
/// Add source retry layers before this layer so they retry individual source
/// requests. If a cached stream detects that the source changed after yielding
/// bytes, restart the whole read and discard the partial result.
///
/// Sources must expose a verifiable identity (an immutable `version` other than
/// `"null"`, a strong `etag`, or `last_modified` + `content_length`) to be
/// cached; reads without one pass through to the source. When a source exposes
/// only `last_modified` and `content_length`, equal-length overwrites within
/// the backend's timestamp resolution (such as 1-second mtime granularity) can
/// keep the same identity and serve cached chunks across `ttl` boundaries.
///
/// Single-path mutations invalidate cached metadata for affected paths on
/// completion. For unversioned reads and non-immutable versions such as
/// `"null"`, external writes, recursive deletes, cancelled mutations, and
/// cross-instance read/write races are bounded by `ttl` (subject to weak
/// `last_modified` granularity above). Reads selecting an explicit immutable
/// `version` skip `ttl` revalidation once cached until invalidated or evicted,
/// even if the version is deleted externally. A reused `Reader` caches its
/// resolved metadata pointer locally for up to `min(ttl, 1s)` across range
/// reads, so completed cross-instance invalidations become visible to an
/// already-reused `Reader` within `min(ttl, 1s)` and immediately to a newly
/// opened `Reader` (or immediately when `ttl` is [`Duration::ZERO`]).
#[derive(Clone)]
pub struct CacheLayer {
    state: Arc<State>,
    policy: Arc<dyn CachePolicy>,
    chunk_size: usize,
    max_chunk_size: usize,
    ttl: Duration,
}

impl fmt::Debug for CacheLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheLayer")
            .field("chunk_size", &self.chunk_size)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl CacheLayer {
    /// Create a layer backed by `cache`.
    ///
    /// Returns [`ErrorKind::ConfigInvalid`] if the cache operator's full
    /// capabilities do not include read, write, and delete, or if its
    /// advertised maximum total write size is zero.
    pub fn new(cache: Operator) -> Result<Self> {
        let capability = cache.info().capability();
        if !(capability.read && capability.write && capability.delete) {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "cache must support read, write, and delete",
            ));
        }
        let max_chunk_size = capability.write_total_max_size.unwrap_or(usize::MAX);
        if max_chunk_size == 0 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "cache maximum total write size must be positive",
            ));
        }
        Ok(Self {
            state: Arc::new(State::new(cache)),
            policy: Arc::new(|_: &CacheReadRequest<'_>| CacheDecision::Cache),
            chunk_size: DEFAULT_CHUNK_SIZE.min(max_chunk_size),
            max_chunk_size,
            ttl: Duration::from_secs(60),
        })
    }

    /// Set the aligned chunk size in bytes.
    ///
    /// Zero restores the default of 1 MiB. Positive sizes are used without
    /// rounding. The cache operator's advertised maximum total write size
    /// caps both the default and explicit sizes.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        let size = if size == 0 { DEFAULT_CHUNK_SIZE } else { size };
        self.chunk_size = size.min(self.max_chunk_size);
        self
    }

    /// Set the default metadata freshness window.
    ///
    /// [`Duration::ZERO`] revalidates before serving a reader.
    /// [`Duration::MAX`] disables periodic revalidation. Reads of an explicitly
    /// selected immutable version skip periodic revalidation regardless of TTL.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Set a custom per-read admission and TTL policy.
    ///
    /// The default policy returns [`CacheDecision::Cache`] for all reads.
    pub fn with_policy(mut self, policy: impl CachePolicy) -> Self {
        self.policy = Arc::new(policy);
        self
    }
}

impl Layer for CacheLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(CacheService {
            inner,
            layer: self.clone(),
        })
    }
}
