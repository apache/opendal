<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Apache OpenDAL Chunked Cache Layer

`CacheLayer` caches aligned chunks of object reads in an OpenDAL operator.
It preserves the source operator's capabilities and leaves conditional reads,
response overrides, and other operations to the source. Mutations invalidate
cached metadata, including when an origin error leaves the mutation outcome
uncertain. Cross-instance invalidation is bounded by `min(ttl, 1s)`: each
`CacheLayer` instance caches the shared recursive-delete generation key for up
to `min(ttl, 1s)`, and a reused `Reader` caches its resolved metadata pointer
locally for up to `min(ttl, 1s)` across range reads. Another process sharing
the same cache backend can therefore observe a recursive delete—or, on an
already-reused `Reader`, a non-recursive mutation—up to `min(ttl, 1s)` later
unless `ttl` is zero. A newly opened `Reader` checks shared cached metadata
immediately for non-recursive mutations.

Attach a source retry layer before adding the cache. Retrying a cached stream
by reopening it after a partial read can mix bytes from different versions.
When the cache detects a change after returning bytes, discard the partial
result and restart the whole read. Reads of large ranges without a verifiable
source identity may stream directly from the source to preserve a single
snapshot. On a native Tokio runtime, shared fills continue when the initiating
reader is canceled. Other runtimes drive fills inline.

The cache operator must support read, write, and delete. Use a backend with
capacity eviction in production. Identity changes can leave old chunks in the
cache until the backend evicts them. Use a dedicated cache root or a separate
cache instance for each tenant, endpoint, or credential boundary. Hot reads
reuse cached metadata and do not re-authorize against the source.

Enable `layers-cache` to use `opendal::layers::CacheLayer` through the facade.
When using `opendal-core` directly, add `opendal-layer-cache`.

```rust
use opendal_core::{Operator, Result};
use opendal_layer_cache::CacheLayer;

fn cached(source: Operator, cache: Operator) -> Result<Operator> {
    Ok(source.layer(CacheLayer::new(cache)?))
}
```

Use `with_policy` to bypass caching or override the metadata TTL for a source
path. The policy receives the normalized path when the reader is created.

```rust
use opendal_core::{Operator, Result};
use opendal_layer_cache::{CacheDecision, CacheLayer, CacheReadRequest};

fn cached(source: Operator, cache: Operator) -> Result<Operator> {
    let layer = CacheLayer::new(cache)?.with_policy(|request: &CacheReadRequest<'_>| {
        if request.path().starts_with("temporary/") {
            CacheDecision::Bypass
        } else {
            CacheDecision::Cache
        }
    });
    Ok(source.layer(layer))
}
```
