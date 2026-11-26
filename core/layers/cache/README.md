<!--
  Licensed to the Apache Software Foundation (ASF) under one
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

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
response overrides, and other operations to the source.

Caching requires a verifiable source identity: an immutable `version` (excluding
non-immutable placeholders such as `"null"`), a strong `etag`, or
`last_modified` + `content_length`. Reads from sources that provide none of
these pass through directly to the source without caching. Note that
`last_modified` + `content_length` is a weak identity: on backends with coarse
timestamp granularity (for example, 1-second mtime precision on SFTP), an
equal-length overwrite with the same timestamp produces the same identity and
can continue serving cached chunks across `ttl` boundaries.

Single-path mutations (`write`, `copy`, `compose`, `rename`, `restore`, and
non-recursive `delete`) invalidate cached metadata for the affected paths on
completion, including when an origin error leaves the outcome uncertain.
Reads explicitly selecting a non-immutable placeholder version such as `"null"`
bypass the cache so local and external overwrites remain immediately visible.
For unversioned reads, external mutations, recursive deletes, cancelled
in-flight mutations, and concurrent cross-instance read/write races are bounded
by `ttl` (subject to weak `last_modified` granularity above). Reads selecting
an explicit immutable `version` skip `ttl` revalidation once cached until
invalidated or evicted, even if that version is deleted externally. A reused `Reader` caches its
resolved metadata pointer locally for up to `min(ttl, 1s)` across range reads,
so a completed cross-instance invalidation becomes visible to an already-reused
`Reader` within `min(ttl, 1s)` and immediately to a newly opened `Reader` (or
immediately when `ttl` is zero).

Attach a source retry layer before adding the cache. Retrying a cached stream
by reopening it after a partial read can mix bytes from different versions.
When the cache detects a change after returning bytes, discard the partial
result and restart the whole read. Shared fills run on the `OperationContext`
executor when configured, fall back to the current Tokio runtime on native
targets, and run inline otherwise.

The cache operator must support read, write, and delete. Use a backend with
capacity eviction in production. Identity changes can leave old chunks in the
cache until the backend evicts them. Use a dedicated cache root or a separate
cache instance for each tenant, endpoint, or credential boundary. Hot reads
reuse cached metadata and do not re-authorize against the source.

The default chunk size is 1 MiB. Use `with_chunk_size` to set an exact size in
bytes, or pass `0` to restore the default. The cache operator's advertised
maximum total write size caps both the default and explicit sizes. The layer
uses this effective size to align source ranges and key cached chunks. The
last chunk of an object can be shorter.

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
