- Proposal Name: `chunked_cache_layer`
- Start Date: 2026-11-21
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#7107](https://github.com/apache/opendal/issues/7107)
- Supersedes: [RFC-6297: Cache Layer](6297_cache_layer.md)

> **Status:** Two defaults remain open for final confirmation before stabilization—default `chunk_size` (provisional `1 MiB`, pending benchmark) and default `ttl` (proposed `60s`). See [Unresolved questions](#unresolved-questions).

# Summary

This RFC supersedes [RFC-6297](6297_cache_layer.md) with a unified, chunked `CacheLayer` (`opendal-layer-cache`, feature `layers-cache`) backed by any OpenDAL `Operator` that supports `read`, `write`, and `delete` with capacity eviction (such as `services::Foyer`, `services::Moka`, or bounded `services::Redis`). Rather than delegating cache layout, key shaping, and per-operation invalidation hooks to user policy code, `CacheLayer` owns fixed-size chunk storage, identity-versioned keys, invalidation, concurrency guards, and in-process singleflight internally. Its public surface narrows to two scalar knobs (`with_chunk_size`, `with_ttl`) and a single-method `CachePolicy` trait (`on_read`) for per-path bypass and TTL overrides.

Applications gain one-line read caching across both small and large objects without maintaining backend-specific cache layers, without cross-version read splicing, and without masking source capabilities. Existing `FoyerLayer` (`layers-foyer`) migrates to `CacheLayer` over `services::Foyer` (`services-foyer`), while `RouteLayer` (`layers-route`) keeps its narrower role of routing distinct path prefixes to separate physical stacks or cache backends.

# Motivation

Three concrete gaps prevent implementing [RFC-6297](6297_cache_layer.md) as written and motivate replacing `FoyerLayer` with a unified layer:

1. **RFC-6297 diverges from OpenDAL's current read architecture and splits ownership unsafely.** After [RFC-7660](7660_move_read_range_to_reader.md), `Service::read(ctx, path, OpRead)` no longer receives `BytesRange`; ranges arrive later on `oio::Read::open` and `oio::Read::read`. RFC-6297's four-hook `CachePolicy` (`on_read`, `on_stat`, `on_write`, `on_delete`) both lacks access to ranges at `Service::read` time and delegates layout and invalidation rules to callers, where mismatched layouts or omitted invalidation hooks silently serve stale or corrupted data.
2. **Whole-object caching duplicates work across backends and penalizes cold reads.** `FoyerLayer` caches whole objects into `foyer::HybridCache` and relies on `with_size_limit` (which requires a pre-read `stat` on cold reads) to avoid loading large files into memory. Extending `FoyerLayer` with chunked caching (as explored in [apache/opendal#7127](https://github.com/apache/opendal/pull/7127)) would duplicate chunk assembly, versioned keys, and invalidation for one backend even though `services::Foyer` already exposes foyer as an OpenDAL `Service`.
3. **Using `RouteLayer` for single-cache path selection misses cross-prefix renames and masks capabilities.** In `core/layers/route/src/lib.rs`, `RouteAccessor::copy` and `RouteAccessor::rename` dispatch by the `from` path. An atomic commit such as `rename("tmp/part", "data/file.parquet")` routes through the `tmp` stack, so a `CacheLayer` mounted only under `data/**` never invalidates the overwritten destination. `RouteAccessor::capability` also clears `write_can_copy_from` and all `compose*` flags. Supporting per-path bypass and TTL overrides inside a single `CacheLayer` lets one layer observe every mutation on the operator without masking capabilities.

# Guide-level explanation

## Mental model and ownership

`CacheLayer` splits caching responsibilities along three clean boundaries:

- **`CacheLayer`** owns chunk layout, versioned key isolation, metadata pointer records, mutation invalidation, concurrency guards, and in-process singleflight.
- **The cache `Operator`** owns storage medium, capacity bounds, eviction, and its own operational layers (for example, `TimeoutLayer` and `MetricsLayer`).
- **Optional `CachePolicy`** owns only per-read admission and freshness overrides (`Bypass`, `Cache`, `CacheFor(Duration)`).

## Basic usage

Most callers attach `CacheLayer` in one line:

```rust
use opendal::{layers::CacheLayer, services, Operator, Result};

async fn build_operator() -> Result<Operator> {
    let cache = Operator::new(services::Foyer::new().memory(64 * 1024 * 1024))?;
    let source = Operator::new(services::S3::default().bucket("my-bucket"))?;

    let op = source.layer(CacheLayer::new(cache)?);
    Ok(op)
}
```

Workloads with known access patterns tune only chunk size and metadata TTL:

```rust
use std::time::Duration;
use opendal::{layers::CacheLayer, Operator, Result};

fn tune_immutable_dataset(source: Operator, cache: Operator) -> Result<Operator> {
    Ok(source.layer(
        CacheLayer::new(cache)?
            .with_chunk_size(4 * 1024 * 1024)
            .with_ttl(Duration::MAX),
    ))
}
```

## Per-path admission and freshness

Callers with mixed workloads—such as lakehouses where data files are immutable while pointer files change frequently—attach a closure or custom `CachePolicy` to a single `CacheLayer`:

```rust
use std::time::Duration;
use opendal::{
    layers::{CacheDecision, CacheLayer, CacheReadRequest},
    Operator, Result,
};

fn build_lakehouse_operator(source: Operator, cache: Operator) -> Result<Operator> {
    Ok(source.layer(
        CacheLayer::new(cache)?.with_policy(|req: &CacheReadRequest<'_>| {
            if req.path().ends_with(".parquet") {
                CacheDecision::CacheFor(Duration::MAX)
            } else if req.path().ends_with("metadata.json") {
                CacheDecision::Bypass
            } else {
                CacheDecision::Cache
            }
        }),
    ))
}
```

## Relationship with `RouteLayer`

`CacheLayer` no longer requires `RouteLayer` for per-path cache filtering or per-path TTLs. Keep `RouteLayer` only when distinct path prefixes require different physical stacks or cache backends (for example, routing `**/*.parquet` to a `services::Foyer` disk cache and `**/*.json` to a `services::Moka` memory cache, or attaching different `TimeoutLayer` / `ThrottleLayer` stacks). When combining `RouteLayer` with `CacheLayer`, remember that `RouteLayer` routes `copy` and `rename` by the `from` path; cross-route overwrites will not reach a `CacheLayer` mounted only on the destination route.

## Migrating from `FoyerLayer`

Replace `FoyerLayer` (`layers-foyer`) with `CacheLayer` (`layers-cache`) over `services::Foyer` (`services-foyer`):

```rust
// Before
let op = source.layer(FoyerLayer::new(hybrid_cache));

// After
let cache = Operator::new(services::Foyer::new().cache(hybrid_cache))?;
let op = source.layer(CacheLayer::new(cache)?);
```

Chunked caching removes the need for `FoyerLayer::with_size_limit`, because large objects populate only the accessed chunks instead of buffering the full object into memory.

# Reference-level explanation

## Crate, feature, and public API

`CacheLayer` lives in a new optional crate `core/layers/cache` (`opendal-layer-cache`), exposed through the `layers-cache` feature on `opendal` and re-exported under `opendal::layers`.

```rust
use std::time::Duration;
use opendal_core::{raw::OpRead, Operator, Result};

#[non_exhaustive]
pub struct CacheReadRequest<'a> {
    // private fields
}

impl<'a> CacheReadRequest<'a> {
    pub fn path(&self) -> &'a str;
    pub fn args(&self) -> &'a OpRead;
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheDecision {
    /// Bypass the cache completely for this reader.
    Bypass,
    /// Use the cache with the layer's configured default TTL.
    Cache,
    /// Use the cache with an entry-specific TTL override.
    /// `Duration::ZERO` revalidates before serving; `Duration::MAX` disables TTL revalidation.
    CacheFor(Duration),
}

pub trait CachePolicy: Send + Sync + 'static {
    /// Called once inside `Service::read`, before any I/O.
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

#[derive(Clone, Debug)]
pub struct CacheLayer {
    // private fields, storing policy as `Arc<dyn CachePolicy>`
}

impl CacheLayer {
    /// Create a `CacheLayer` backed by `cache`.
    ///
    /// Returns `ErrorKind::ConfigInvalid` if `cache.info().full_capability()`
    /// does not support `read`, `write`, and `delete`.
    pub fn new(cache: Operator) -> Result<Self>;

    /// Set the aligned chunk size in bytes.
    ///
    /// Values of `0` are clamped to `1`. When `cache` advertises
    /// `write_total_max_size`, the effective chunk size is capped to that limit.
    pub fn with_chunk_size(self, size: usize) -> Self;

    /// Set the default metadata freshness window.
    ///
    /// `Duration::ZERO` revalidates on every `Reader`.
    /// `Duration::MAX` disables periodic TTL revalidation.
    pub fn with_ttl(self, ttl: Duration) -> Self;

    /// Set a custom per-read admission and TTL policy.
    ///
    /// The default policy returns `CacheDecision::Cache` for all reads.
    pub fn with_policy(self, policy: impl CachePolicy) -> Self;
}
```

Storing the policy as `Arc<dyn CachePolicy>` keeps `CacheLayer` non-generic, `Clone`, and `Debug` even when callers pass capturing closures, while adding one dynamic dispatch per `Service::read` (not per chunk).

`CacheLayer` implements `raw::Layer::apply_service` and returns `self.inner.capability()` unchanged from `Service::capability()`. It does not mask `write_can_copy_from`, `compose`, conditional operations, or recursive delete.

## Cache backend contract and boundaries

- **Minimum capabilities:** `read`, `write`, and `delete`. `CacheLayer` does not require `stat`, `list`, `write_with_expires`, range reads, or conditional writes on the cache `Operator`. `services::Foyer`, `services::Moka`, `services::Redis`, `services::Memcached`, `services::Memory`, and `services::Fs` all satisfy this capability set (verified from current service definitions).
- **Eviction responsibility:** Because updates and version changes leave old chunks unreachable under previous identity keys, the cache backend must enforce its own capacity limits and eviction policy (such as `services::Foyer`, `services::Moka`, or `services::Redis` configured with `maxmemory`). Unbounded backends such as `Memory` or `Fs` will retain orphaned chunks until cleared externally.
- **Operational layers:** Users attach `TimeoutLayer`, `MetricsLayer`, and `TracingLayer` directly to the source and cache `Operator` stacks. Note that `LoggingLayer` logs `ErrorKind::NotFound` at `Warn` level (`core/layers/logging/src/lib.rs`), so attaching default `LoggingLayer` to the cache `Operator` emits warnings on normal cache misses unless filtered.
- **Tenant and credential isolation:** `ServiceInfo` carries `(scheme, root, name)` without endpoints or credentials (`core/core/src/raw/service.rs`). Mixing `scheme`, `name`, and absolute path into cache keys prevents accidental cross-bucket collisions within one process, but operators using different tenants, endpoints, or credentials **must** use separate cache `root` prefixes or separate cache instances because cache hits within `ttl` do not re-authenticate against the source.

## Key layout and object identity (`v1` namespace)

All cache entries live under a versioned `v1/` prefix using lowercase hexadecimal path segments (`[0-9a-f/]`), avoiding path escaping issues, case-insensitive filesystem collisions, and long-key rejections (each segment is at most 64 bytes; full chunk keys are ~140 bytes, well below Memcached's 250-byte key limit and standard 255-byte filename limits):

```text
v1/gen
v1/meta/{gen}/{subject}
v1/data/{gen}/{subject}/{ident}/{chunk_size}/{index}
```

- `gen`: 64-bit generation counter formatted in lowercase hex, persisted in `v1/gen` (missing `v1/gen` equals `0`). Bumped on recursive delete (`OpDelete::recursive()`) to logically invalidate both metadata pointers and data chunks in O(1).
- `subject`: `sha256(len_prefixed(scheme, name, abs_path, version_selector))` (64 hex chars), where `version_selector` is `OpRead::version()`.
- `v1/meta/{gen}/{subject}`: compact versioned binary record containing:
  - the observed complete object `Metadata` (storing only fields returned by the source—never synthesized placeholders),
  - the derived identity tier and `ident` token (32 hex chars), and
  - `observed_at`, captured **before** the source request begins so in-flight latency cannot extend freshness past `ttl`.
- `v1/data/{gen}/{subject}/{ident}/{chunk_size}/{index}`: raw bytes for aligned chunk `index`, covering `[index * chunk_size, min((index + 1) * chunk_size, content_length))`, stored without an extra framing header.

### Automatic identity tiering (`ident`)

`CacheLayer` selects the strongest identity available in the source's observed `Metadata` (contract derived from current `Metadata` and `conditional-operations.md`):

1. **Version (+ strong ETag when present):** `metadata.version()` (paired with non-weak `metadata.etag()` if both exist). When a read explicitly specifies `OpRead::version()`, the targeted version is immutable and skips periodic TTL revalidation.
2. **Strong ETag:** `metadata.etag()` excluding weak ETags (`W/"..."`), matching `core/core/src/docs/specs/conditional-operations.md` and RFC 9110 §13.1.1.
3. **Weak validator:** `(last_modified, content_length)` when neither `version` nor strong `etag` is available. This enables detecting external modifications on services like `Fs` (bounded by filesystem mtime precision).
4. **Local nonce:** random 128-bit nonce when the source exposes none of the above (for example, `services::Memory`, whose `RpRead` only reports `content_length`). Invalidated on local mutations and expired when `ttl` elapses.

## Read path semantics

1. **Per-reader policy decision (`Service::read`):** `CachePolicy::on_read` executes once before any I/O. If it returns `CacheDecision::Bypass`, `CacheLayer` returns `inner.read(ctx, path, args)` directly.
2. **Pass-through conditions:** Reads where `args.is_conditional()` is true or any `override_*` option is set (`override_content_type`, `override_cache_control`, `override_content_disposition`) bypass the cache and delegate directly to the source so preconditions and response overrides are evaluated natively. `OpRead::version()` is part of `subject`, not a precondition, and participates in caching.
3. **Metadata resolution:** On the reader's first `open`/`read`, `CacheLayer` resolves `v1/meta/{gen}/{subject}`:
   - **Fresh pointer** (`effective_ttl == Duration::MAX`, explicit `OpRead::version()`, or `now - observed_at < effective_ttl`): reuse cached `Metadata` and `ident` with 0 source RTT.
   - **Expired pointer** (`effective_ttl == Duration::ZERO` or age `>= effective_ttl`):
     - For verifiable tiers (1–3), issue `inner.stat` (1 RTT, no body transfer). If the derived `ident` matches, refresh `observed_at` and reuse cached chunks; otherwise update or delete the pointer.
     - For Tier 4 (nonce), delete the pointer and proceed as a cold miss.
   - **Cold miss (no pointer):**
     - For bounded forward ranges or unbounded `0..`, issue the aligned source read directly via `oio::Read::open` without a preceding `stat` (1 RTT on object stores such as S3, GCS, and Azblob that return complete `Metadata` in `RpRead`).
     - Verified codebase reality: not all services populate complete `Metadata` in `RpRead`—`services::Memory` (`core/core/src/services/memory/backend.rs`) populates only `content_length`, while `Fs` and all `PositionReader`-backed services (`core/core/src/raw/oio/read/position_read.rs`) return `RpRead::default()` (`None`). When `RpRead` lacks total `content_length` or a validator needed to form a pointer, or on a cold `Suffix` read whose source lacks `Capability::read_with_suffix` or `RpRead` metadata, `CacheLayer` falls back to one `inner.stat` before writing `v1/meta/{gen}/{subject}`.
4. **Range normalization, alignment, and EOF handling:**
   - Normalize `BytesRange` (`offset..`, `offset..end`, or `Suffix(n)`) against known `content_length`.
   - Forward requests starting at or beyond `content_length` (including empty objects) to the source so backend boundary semantics (such as `ErrorKind::RangeNotSatisfied`) remain authoritative.
   - Check cache for all chunks covering the normalized range. Treat any cached chunk whose byte length does not match the expected chunk length as a miss.
   - Coalesce contiguous missing chunks into a single aligned source request using `oio::Read::open` (never `oio::Read::read`, because the final chunk of an object is naturally short while `StreamReader::read` and `PositionReader::read` fail short reads with `Unexpected` or `RangeNotSatisfied`).
   - Return `RpRead::new(metadata)` carrying the real `Metadata` from the resolved pointer so `ReadContext::metadata()` observes genuine metadata.
5. **Single-version read invariant (no cross-version splicing):**
   - Every chunk-fill request pins source identity whenever supported (`if_match` for strong ETag, `version` for versioned reads) and checks the response `RpRead` metadata (`version`, `etag`, `content_length`) against the resolved `ident`.
   - If a mismatch occurs before any bytes are yielded to the caller, `CacheLayer` deletes the stale pointer and retries once against the new version.
   - If bytes have already been yielded on the stream, `CacheLayer` fails the stream with a temporary error rather than splicing bytes from two versions. New-version bytes are never written under the old `ident` key.
6. **In-process singleflight:**
   - Concurrent misses for the same chunk key coalesce inside an in-process singleflight table keyed by chunk key.
   - Singleflight runs bounded aligned fill units independently of caller `ReadStream` polling so a slow consumer or dropped caller future cannot stall or cancel sibling waiters.

## Mutation invalidation and concurrency invariants

`CacheLayer` invalidates affected `v1/meta/{gen}/{subject}` entries after every local mutation completes—**regardless of whether the inner mutation returns `Ok` or `Err`**, because remote timeouts or ambiguous failures can still commit server-side:

- `write` (including append, conditional writes, and `Writer::copy_from`), `copy`, and `compose`: invalidate the destination path after the stateful operation closes (or errors).
- `rename`: invalidate both `from` and `to`.
- `restore`: invalidate the target path.
- `delete`: track each accepted path (and optional version) on the `Deleter` and invalidate after `Deleter::close()`. A versioned delete invalidates both the versioned subject and the unversioned current subject.

Three additional internal invariants close concrete races verified against the current core codebase:

1. **Sharded epoch guard against fill-vs-mutation resurrection:** `CacheLayer` maintains a fixed-size sharded epoch table indexed by `hash(subject)`. Readers snapshot the shard epoch before issuing a source read or `stat`. Mutations increment the shard epoch before deleting `v1/meta/{gen}/{subject}` (and again after completion). Before writing a newly populated pointer back to cache, a reader verifies under the shard lock that the epoch has not advanced; if it has, the reader skips storing the pointer while still returning the fetched bytes. Residual cross-process races on shared caches are bounded by `ttl`.
2. **O(1) recursive delete invalidation via `v1/gen`:** `Operator::new` places `SimulateLayer` below user layers (`core/core/src/types/operator/builder.rs`), and backends such as `Fs`, `Hdfs`, `Azdls`, and `Gdrive` implement recursive delete natively. Consequently, `OpDelete::recursive()` reaches `CacheLayer` as a single directory path that cannot be prefix-matched against hashed keys without `list`. On `OpDelete::recursive()`, `CacheLayer` increments `v1/gen` in O(1), logically invalidating existing pointers and chunks without scanning the cache.
3. **Aligning pass-through `stat` with cached `read`:** `CacheLayer` passes `stat` through to the source in v1 (avoiding serving incomplete `RpRead` metadata as a `stat` result). Whenever a pass-through `stat` returns `NotFound` or metadata whose derived `ident` differs from the cached pointer, `CacheLayer` deletes the stale pointer. This prevents `ReadContext::content_length()` (which calls `stat` for chunked/concurrent unbounded reads) from planning reads using a new length while reading stale cached chunks.

## Failure semantics and guarantees

**Guaranteed:**
- Every byte returned by a single `open` or `read` comes from one object identity matching its `RpRead` metadata (no cross-version splicing).
- Every `Reader` and `stat` observes source state no older than the effective `ttl` (within clock skew on shared caches).
- Cache backend errors never fail source operations: cache read errors degrade to misses, and cache write/delete errors are ignored (and remain observable through layers attached to the cache `Operator`).

**Not guaranteed (explicit boundaries):**
- Strict read-your-own-writes across processes or after cache delete failures when `ttl > 0` (bounded by `ttl`).
- Source re-authorization on hot cache hits within `ttl`.
- Caching `list` results.
- Snapshot isolation across separate `open`/`read` calls on the same `Reader` (matching uncached OpenDAL behavior).

## Compatibility and migration

- **RFC-6297 and PR #7127:** This RFC supersedes [RFC-6297](6297_cache_layer.md) before `core/layers/cache` has shipped, so there is no breaking change to `CacheLayer` itself. Chunked caching lands once in `CacheLayer`, superseding [apache/opendal#7127](https://github.com/apache/opendal/pull/7127).
- **`FoyerLayer` (`layers-foyer`):** Remains untouched while `CacheLayer` and `services::Foyer` hardening land and pass parity tests (including conditional read bypass and copy/rename/delete invalidation coverage). Once parity is verified, `FoyerLayer` will be deprecated and subsequently removed in a breaking release with migration notes pointing to `CacheLayer` + `services::Foyer`.
- **`RouteLayer` (`layers-route`):** Unchanged in API; documentation narrows its caching guidance to multi-backend routing and calls out `from`-path routing on `copy`/`rename`.

## Validation criteria

Before stabilizing `CacheLayer` and deprecating `FoyerLayer`, implementation tests must verify:

- Single-version integrity under concurrent overwrites during multi-chunk reads (retry before first byte; temporary error rather than splicing after first byte).
- Invalidation across `write`, `append`, `Writer::copy_from`, `copy`, `rename`, `compose`, `restore`, batch `delete`, versioned `delete`, and `OpDelete::recursive()`, including when a mutation fails or times out.
- In-process epoch guard preventing an in-flight slow read from resurrecting a deleted pointer.
- Pass-through correctness for conditional reads (`if_match`, `if_none_match`, etc.) and `override_*` headers, plus cached immutable `OpRead::version()` reads.
- Cross-service behavior on services with full `RpRead` metadata (e.g., S3), length-only `RpRead` (`Memory`), and empty `RpRead` (`Fs`), including `Suffix`, unbounded, and out-of-bounds ranges.
- Degraded operation when the cache backend injects read/write/delete errors.
- Benchmark measurement of `chunk_size` (`1 MiB`–`4 MiB`) on S3 + `services::Foyer` to finalize the default.

# Drawbacks

- **Cold-read first-byte latency:** Decoupling singleflight fill units from consumer streams means the first byte of a cold read waits for the first aligned chunk fill unit rather than streaming byte-by-byte immediately.
- **Coarse recursive-delete invalidation:** Bumping `v1/gen` on `OpDelete::recursive()` invalidates the entire cache namespace rather than only the deleted subtree. This trades rare recursive-delete cache warmth for O(1) invalidation without requiring `list`.
- **Orphaned chunks require backend eviction:** Because updates delete only `v1/meta/{gen}/{subject}`, cache backends without capacity eviction accumulate orphaned data chunks over time.
- **Periodic revalidation under finite default TTL:** A finite default `ttl` (proposed `60s`) adds one `stat` RTT per object per TTL window on immutable workloads unless callers opt into `.with_ttl(Duration::MAX)`.

# Rationale and alternatives

- **Unified chunking vs. RFC-6297's `WholeCachePolicy` / `ChunkedCachePolicy` split:** Small objects occupy a single chunk with no extra amplification, while large objects cache only accessed ranges without needing `max_object_size` or pre-read `stat`. Keeping one internal layout eliminates layout-mismatch bugs and halves maintenance cost.
- **Minimal `CachePolicy` (`on_read` with 3 variants) vs. richer policy hooks (`ReadOnly`, `should_fill`, `max_object_size`, 4 lifecycle hooks):** Removing `max_object_size`, `ReadOnly`, and `should_fill` eliminates three overlapping ways to skip filling and keeps every `Reader` consistent with a single pre-I/O decision matching post-RFC-7660 `Service::read`. If real workloads later require metadata-based fill filtering or read-without-fill, defaulted methods on `CachePolicy` and new variants on `#[non_exhaustive] CacheDecision` can be added without breaking changes.
- **No `CacheInterceptor`:** Users already compose `MetricsLayer`, `LoggingLayer`, and `TracingLayer` on the source and cache `Operator`s. Adding a bespoke observability trait would duplicate existing layer abstractions.
- **General `CacheLayer` + `services::Foyer` vs. extending `FoyerLayer`:** Implementing chunked caching once in `CacheLayer` benefits every eviction-capable OpenDAL cache service (`foyer`, `moka`, `redis`, `memcached`) and avoids maintaining two parallel caching stacks.

# Prior art

- **OpenDAL `FoyerLayer` (RFC-6370) and `RouteLayer` (RFC-7130):** Established `foyer` integration, conditional-read bypass, and copy/rename invalidation lessons, and revealed the `RouteLayer` `from`-path rename gap and whole-object memory pressure.
- **Mountpoint for Amazon S3 data cache:** Uses fixed-size block caching paired with a finite metadata TTL to keep hot reads 0-RTT while bounding staleness from external writers.
- **Client-side distributed filesystem caches (e.g., JuiceFS, Alluxio):** Separate mutable path-to-metadata pointers from immutable version/content-addressed data blocks so updates invalidate metadata in O(1) while leaving block eviction to cache storage.

# Unresolved questions

1. **Default `chunk_size` (provisional recommendation: `1 MiB`):** `1 MiB` fits Memcached's default item size limit and keeps singleflight chunk-fill latency low, at the cost of more cache entries for multi-gigabyte sequential scans compared to `2 MiB` or `4 MiB`. Final default will be chosen after benchmarking `1 MiB`, `2 MiB`, and `4 MiB` against S3 + `services::Foyer`.
2. **Default `ttl` (proposed recommendation: `Duration::from_secs(60)`):**
   - `60s` keeps hot reads 0-RTT within the window while bounding staleness from external writers, persistent disk caches across restarts, failed cache deletes, and cross-process races; immutable datasets can disable revalidation with `.with_ttl(Duration::MAX)`.
   - `Duration::MAX` maximizes speed by default, but turns all residual staleness windows into permanent silent stale reads.
   - `Duration::ZERO` maximizes freshness, but adds a `stat` RTT to every hot small-object `Reader`, making caching appear ineffective out of the box.

# Future possibilities

- Serving unconditional `stat` from fresh pointer records when the source guarantees complete `stat` metadata in cached pointers.
- `If-None-Match` (`304 Not Modified`) revalidation on expired reads to avoid a separate `stat` RTT when the object has not changed.
- Additional defaulted methods on `CachePolicy` (for example, metadata-based fill admission) if concrete workloads need them.
- Asynchronous background fill via `OperationContext::executor()` or optional write-through caching.
