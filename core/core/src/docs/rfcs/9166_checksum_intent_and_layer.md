- Proposal Name: `checksum_intent_and_layer`
- Start Date: 2026-11-26
- RFC PR: [apache/opendal#9166](https://github.com/apache/opendal/pull/9166)
- Tracking Issue: [apache/opendal#5549](https://github.com/apache/opendal/issues/5549)
- Supersedes: `Capability` and `ChecksumLayer` sections of [RFC-6817: Checksum](6817_checksum.md)

# Summary

Replace the unreleased per-algorithm `Capability::checksum_*` booleans with a single `Capability::write_with_checksum: bool`, generalize `WriteOptions::checksum` to a four-variant `WriteChecksum` intent (`Any`, `Fast`, `Algo(ChecksumAlgo)`, `Expected(Checksum)`), add an opt-in `checksum: bool` metadata hint on `StatOptions`/`ReadOptions`/`ReaderOptions`, move zero-buffered streaming checksum calculation and synchronous validation into each `Service`, and scope `ChecksumLayer` to operator-level write policy injection (`WriteChecksumPolicy`) and full-object streaming read verification.

This gives applications a portable way to enable native upload integrity across services (avoiding new per-service `checksum_algorithm` configs) without bloating `Capability` or binding FFI structs, without full-file buffering, and without fabricating local verification when a backend lacks checksum support.

# Motivation

PR #7830 (`a22829178`, unreleased) landed the first slice of [RFC-6817](6817_checksum.md): `ChecksumAlgo`, `Checksum`, `ErrorKind::ChecksumMismatch`, `WriteOptions::checksum(Checksum)`, `Metadata::checksum()`, and AWS S3 write verification for caller-supplied `Crc32c` and `Crc64Nvme`. Extending that baseline across services and adding `ChecksumLayer` exposes three contract problems:

1. **Per-algorithm `Capability` fields do not scale.** Native fast checksums rarely overlap across cloud providers: AWS S3 uses `CRC64NVME`/`CRC32C`, GCS uses `CRC32C`, OSS uses `CRC64-ECMA`, Azure Blob uses per-block `CRC64` (transport-only), and B2 uses `SHA-1`. Adding a boolean per algorithm expands `Capability`, its Java/.NET FFI mirrors, and `website/data/capabilities.json` for provider-specific details most callers do not want to branch on.
2. **Callers without a precomputed digest lack a portable write contract.** Workloads such as data lakes and general object pipelines want native streaming upload protection ("use a service-supported CRC" or "use any native checksum") without precomputing digests in application code or wiring per-service builder options.
3. **RFC-6817's `ChecksumLayer` design conflicts with streaming I/O and honest guarantees.** Computing a full-file digest in a layer before `Service::write` requires buffering the entire file; computing a local checksum when a backend lacks write verification gives zero wire or storage protection while hiding `Unsupported`; computing a missing checksum on `stat()` turns a `HEAD` into a full download; and OpenDAL's `oio::Read` is range-oriented, where full-file digests cannot verify partial ranges.

This proposal supersedes the `Capability` and `ChecksumLayer` sections of [RFC-6817](6817_checksum.md) before PR #7830's unreleased capability fields ship in a release.

# Guide-level explanation

## Delta from current `main` (unreleased #7830)

| Area | Current `main` (unreleased #7830) | Proposed |
| --- | --- | --- |
| `Capability` | 4 booleans: `checksum_crc64_nvme`, `checksum_crc32c`, `checksum_md5`, `checksum_sha256` | 1 boolean: `write_with_checksum` |
| `WriteOptions::checksum` | `Option<Checksum>` (caller-supplied digest only) | `Option<WriteChecksum>` (`Any`, `Fast`, `Algo`, `Expected`) |
| `Service::write` | Assumes `OpWrite::check` already checked exact algorithm | Synchronously validates class/algorithm support across all write paths and returns `Unsupported` before I/O |
| `StatOptions` / `ReadOptions` / `ReaderOptions` | No checksum retrieval opt-in; `stat`/`read` do not populate `Metadata::checksum()` | Add `checksum: bool` (default `false`) hint to request backend full-file checksum metadata when extra headers/permissions are required (e.g., S3 `x-amz-checksum-mode: ENABLED`); zero-cost services may always populate `Metadata::checksum()` |
| `WriteState` streaming `copy_from` | Resumes on read errors; does not poison writer on source `ChecksumMismatch` | Poisons writer (`requires_abort`) if source fails with `ChecksumMismatch` after writing bytes to destination |
| `ChecksumLayer` | Unimplemented (RFC-6817 specifies local fallback + `stat` read-compute) | Opt-in write policy injection (`WriteChecksumPolicy`, best-effort or strict) + full-object streaming read verification |

## New and updated concepts

- **`WriteChecksum`** describes what integrity check a write requests:
  - `WriteChecksum::Any`: use any native write checksum the service supports (services prefer CRC-family algorithms over cryptographic hashes). May use transport-level per-part/per-block verification; populates `Metadata::checksum()` when the service produces a verified full-file digest, or leaves it `None` for transport-only paths (such as Azure Blob per-block CRC64 or B2 large-file per-part SHA-1).
  - `WriteChecksum::Fast`: use a service-supported CRC-family checksum (e.g., CRC32, CRC32C, CRC64-NVME, CRC64-ECMA, Azure CRC64). Rejects services that only offer cryptographic hashes (MD5/SHA-1/SHA-256). Same `Metadata::checksum()` rule as `Any`.
  - `WriteChecksum::Algo(ChecksumAlgo)`: compute and verify a **full-file** checksum with the specified algorithm before the object commits (across every supported non-append write path), and return it in `Metadata::checksum()`.
  - `WriteChecksum::Expected(Checksum)`: verify the written bytes against a caller-supplied **full-file** digest before the object commits (across every supported non-append write path), and return it in `Metadata::checksum()`.
- **`WriteChecksumPolicy`** is the subset valid for operator-wide injection in `ChecksumLayer`: `Any`, `Fast`, `Algo(ChecksumAlgo)` (a fixed `Expected(Checksum)` belongs to a single object and is only accepted per write).
- **`Capability::write_with_checksum`** indicates whether the service supports native write checksums (at minimum `WriteChecksum::Any` on non-append writes).
- **`StatOptions::checksum` / `ReadOptions::checksum` / `ReaderOptions::checksum`** (default `false`) is an opt-in metadata hint requesting backend full-file checksum headers when retrieving them requires extra headers or permissions (such as AWS S3 `x-amz-checksum-mode: ENABLED` on SSE-KMS objects).
- **`ChecksumLayer`** (`opendal-layer-checksum`) injects a `WriteChecksumPolicy` on writes that did not specify a checksum and verifies recognizable single-response full-object reads against backend-returned full-file checksums.

## Examples

### Per-write integrity (`Into<WriteChecksum>`) and opt-in stat checksum

```rust,ignore
use opendal::{Checksum, ChecksumAlgo, Operator, Result, WriteChecksum};

async fn example(op: Operator, data: Vec<u8>, expected: Checksum) -> Result<()> {
    // 1. Caller-supplied full-file digest (source-compatible with PR #7830)
    let meta = op.write_with("a.bin", data.clone()).checksum(expected).await?;
    assert!(meta.checksum().is_some());

    // 2. Service-computed specific full-file algorithm
    let meta = op.write_with("b.bin", data.clone()).checksum(ChecksumAlgo::Crc32c).await?;
    assert_eq!(meta.checksum().map(|c| c.algo), Some(ChecksumAlgo::Crc32c));

    // 3. Service-supported CRC-family checksum (full-file on S3/GCS/OSS; per-block on Azblob)
    let _meta = op.write_with("c.bin", data.clone()).checksum(WriteChecksum::Fast).await?;

    // 4. Any native checksum (also covers SHA-1-only services such as B2)
    let _meta = op.write_with("d.bin", data).checksum(WriteChecksum::Any).await?;

    // 5. Opt into retrieving full-file checksum metadata on stat (e.g., S3 checksum mode)
    let stat = op.stat_with("a.bin").checksum(true).await?;
    let _digest = stat.checksum();
    Ok(())
}
```

### Operator-wide protection via `ChecksumLayer`

```rust,ignore
use opendal::layers::{ChecksumLayer, RetryLayer};
use opendal::{Operator, WriteChecksumPolicy};

fn build(op: Operator) -> Operator {
    // Place ChecksumLayer outside RetryLayer so mid-stream read retries resume
    // transparently under the same digest state.
    op.layer(RetryLayer::new()).layer(
        ChecksumLayer::new()
            .write(WriteChecksumPolicy::Fast) // default; best-effort when unsupported
            .verify_read(true),               // default; verifies full-object reads when metadata has checksum
    )
}
```

# Reference-level explanation

## Core types and options (`opendal-core`)

### `WriteChecksum` and `WriteChecksumPolicy`

```rust,ignore
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WriteChecksum {
    Any,
    Fast,
    Algo(ChecksumAlgo),
    Expected(Checksum),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WriteChecksumPolicy {
    Any,
    Fast,
    Algo(ChecksumAlgo),
}

impl From<Checksum> for WriteChecksum {
    fn from(v: Checksum) -> Self {
        Self::Expected(v)
    }
}

impl From<ChecksumAlgo> for WriteChecksum {
    fn from(v: ChecksumAlgo) -> Self {
        Self::Algo(v)
    }
}

impl From<WriteChecksumPolicy> for WriteChecksum {
    fn from(v: WriteChecksumPolicy) -> Self {
        match v {
            WriteChecksumPolicy::Any => Self::Any,
            WriteChecksumPolicy::Fast => Self::Fast,
            WriteChecksumPolicy::Algo(algo) => Self::Algo(algo),
        }
    }
}
```

- `WriteOptions::checksum` changes from `Option<Checksum>` to `Option<WriteChecksum>`. A single field makes conflicting intent (e.g., setting both `Algo` and `Expected`) unrepresentable.
- `FutureWrite::checksum` and `FutureWriter::checksum` accept `impl Into<WriteChecksum>`.
- `OpWrite` exposes `checksum(&self) -> Option<WriteChecksum>` and a validated helper `with_checksum(self, checksum: WriteChecksum, capability: &Capability) -> Result<Self>` so external layers (such as `ChecksumLayer`) inject write checksums through the exact same validation path as `OpWrite::from_options`.
  - Internally, `WriteField::Checksum` stores an explicit leading variant byte followed by an optional algorithm tag byte and optional raw digest bytes, while `Metadata` retains its pure `Checksum` encoding.
- `ChecksumAlgo` remains `#[non_exhaustive]` (`Crc64Nvme`, `Crc32c`, `Md5`, `Sha256`), extensible with additional algorithms (e.g., `Crc64Ecma`, `Crc32`, `Sha1`) without touching `Capability`.

### `StatOptions`, `ReadOptions`, and `ReaderOptions` checksum metadata hint

- Add `pub checksum: bool` (default `false`) to `StatOptions`, `ReadOptions`, and `ReaderOptions`, with `.checksum(bool)` builders on `FutureStat`, `FutureRead`, and `FutureReader`.
- Expose `OpStat::checksum(&self) -> bool`, `OpRead::checksum(&self) -> bool`, and `OpRead::with_checksum(self, checksum: bool) -> Self`.
- **Contract:** `checksum: true` is a metadata-retrieval hint instructing the service to request full-file checksum metadata when doing so requires extra protocol headers or permissions (such as AWS S3 `x-amz-checksum-mode: ENABLED`).
  - It never returns `ErrorKind::Unsupported` on services that do not carry stored checksums.
  - It never causes `stat()` to read object bytes.
  - Services whose ordinary `stat`/`read` responses already carry a verified full-file checksum at zero extra cost or permission (e.g., GCS) may populate `Metadata::checksum()` regardless of this flag.
  - `ChecksumLayer` sets `OpRead::with_checksum(true)` automatically on eligible full-object reads when `verify_read` is enabled.

### `Capability`

- Remove the four unreleased fields (`checksum_crc64_nvme`, `checksum_crc32c`, `checksum_md5`, `checksum_sha256`) from `Capability`, `bindings/java`, `bindings/dotnet`, `dev/src/generate/capabilities_docs.rs`, and `website/data/capabilities.json`.
- Add `pub write_with_checksum: bool` to `Capability` (default `false`).
- **Contract of `write_with_checksum: true`:** The service natively verifies writes for at least `WriteChecksum::Any` on non-append writes and synchronously validates all `WriteChecksum` variants in `Service::write`.
- **No `stat_has_checksum` / `read_has_checksum` flag:** Consistent with every other optional `Metadata` field (`etag`, `version`, `content_md5`), callers inspect `Metadata::checksum() -> Option<Checksum>`.

### Validation split (`OpWrite::check` vs `Service::write`)

1. **`OpWrite::from_options` / `OpWrite::check(&self, capability)`** (verified in `core/core/src/raw/ops.rs`):
   - `Expected(cs)` with `cs.value.len() != cs.algo.digest_len()` -> `ErrorKind::ConfigInvalid`.
   - Any `Some(WriteChecksum)` when `!capability.write_with_checksum` -> `ErrorKind::Unsupported`.
   - `Algo(_)` or `Expected(_)` combined with `append == true` -> `ErrorKind::Unsupported`.
   - `Any` or `Fast` combined with `append == true` passes `OpWrite::check`; each service decides in `Service::write` whether its append path supports per-request transport checksums (otherwise returns `Unsupported`).
2. **`Service::write(&self, ctx, path, args) -> Result<Self::Writer>`** (synchronous in `core/core/src/raw/service.rs`):
   - **Invariant 1 (Zero-I/O rejection):** `Service::write` must validate `args.checksum()` against its supported classes, algorithms, and `args.append()` synchronously and return `ErrorKind::Unsupported` before performing network I/O or side effects.
   - **Invariant 2 (All-write-path fulfillment):** Because `Operator::write_with` and `Operator::writer_with` share `writer_inner` (`core/core/src/types/operator/operator.rs#L1021`), `Service::write` has neither payload length nor a single-shot guarantee. A service must only accept `Algo(a)` or `Expected(cs)` synchronously when **every supported non-append write path** (single-shot and multi-chunk/multipart) can fulfill the pre-commit full-file verification contract for `a`. Services that only verify single-PUT uploads for an algorithm must reject `Algo`/`Expected` for that algorithm in `Service::write`.

## Ownership, invariants, and boundaries

### Full-file vs transport-level checksum invariant

- `Metadata::checksum()` **must only** contain a backend-verified **full-file** digest. Composite digests (e.g., S3 `ChecksumType=COMPOSITE` values suffixed with `-N`), per-part/per-block digests, or unverified local computations must never be stored in `Metadata::checksum()`.
- Outcome matrix by `WriteChecksum` variant:

| Variant | Pre-commit full-file verification required? | Transport-only per-part/block verification allowed? | `Metadata::checksum()` on success |
| --- | --- | --- | --- |
| `Expected(cs)` | Yes (against `cs`, all write paths) | No | `Some(cs)` (echoed/confirmed by backend) |
| `Algo(algo)` | Yes (service computes streaming, all write paths) | No | `Some(Checksum { algo, .. })` (confirmed by backend) |
| `Fast` | Preferred when native | Yes (CRC-family only) | `Some(full_file_cs)` if native full-file; `None` if transport-only |
| `Any` | Preferred when native | Yes (any native algorithm) | `Some(full_file_cs)` if native full-file; `None` if transport-only |

### Service-layer streaming execution (zero full-file buffering)

Services compute checksums directly over each in-flight `Buffer` (single-shot or multipart/block chunk) and combine part digests natively when their protocol supports full-object verification:

- **AWS S3 (`scheme == "s3" && !is_s3_express`)**:
  - *Verified on live AWS S3 in #7830 (`a22829178`):* `Expected(Crc32c)` and `Expected(Crc64Nvme)` on single `PutObject` and uneven-part multipart uploads with `FULL_OBJECT`.
  - *Expected (to be live-verified on AWS S3):*
    - `Any` / `Fast` select `CRC64NVME`; `Algo(Crc64Nvme | Crc32c)` uses the requested algorithm; `Algo`/`Expected` for `Md5`/`Sha256` synchronously return `Unsupported`.
    - Multipart uploads populate `oio::MultipartPart::size` (currently `None` in `core/services/s3/src/writer.rs#L219`) alongside `checksum`, combine per-part CRCs in `part_number` order using `crc_fast::checksum_combine`, send the full-object digest on `CompleteMultipartUpload` with `x-amz-checksum-type: FULL_OBJECT`, verify the echoed digest, and populate `Metadata::checksum()`.
    - **Read / Stat checksum mode (permission-safe opt-in):** AWS `HeadObject` / `GetObject` with `x-amz-checksum-mode: ENABLED` requires KMS permissions on SSE-KMS encrypted objects and interacts with custom `s3://` endpoints and presigned requests (which share request builders in `core/services/s3/src/backend.rs#L1596`). Therefore, S3 sends `x-amz-checksum-mode: ENABLED` **only** when `args.checksum()` is `true` on `OpStat`/`OpRead` (never on presigned requests), and populates `Metadata::checksum()` only when `x-amz-checksum-type` is `FULL_OBJECT` (or an unsuffixed single-part checksum), ignoring `COMPOSITE`.
    - S3-compatible presets (`minio`, `r2`, etc.) keep `write_with_checksum = false` until verified not to silently ignore checksum headers.
- **Other services (protocol mapping; all unverified — each requires live-service verification before enabling `write_with_checksum = true`):**
  - **GCS:** Official data validation supports full-object `X-Goog-Hash: crc32c=...` on single upload and on the **final** request of a JSON resumable upload (`GcsConditionalWriter`, `core/services/gcs/src/writer.rs#L422`). Both the final upload response and the session-query recovery exit (`Complete(metadata)`) must verify the same digest. XML multipart validates parts and returns whole-object `crc32c` on completion, so `Algo(Crc32c)`/`Expected(Crc32c)` must route through JSON resumable upload. On read (`core/services/gcs/src/reader.rs#L74`), stored-object `crc32c` applies only when the response body matches the stored byte representation (not transcoded gzip).
  - **Azure Blob:** `Any` / `Fast` send per-request `x-ms-content-crc64` on `PutBlob`, `PutBlock`, and `AppendBlock` for transport integrity and leave `Metadata::checksum()` as `None`. `Algo` and `Expected` return `Unsupported` synchronously in `Service::write` because `PutBlockList` does not verify full-blob CRC64 or MD5 across concatenated blocks.
  - **Backblaze B2:** `Any` sends `X-Bz-Content-Sha1` on single upload and per-part on large files (`b2_finish_large_file` accepts `partSha1Array`, not a backend-verified full-file SHA-1). `Fast`, `Algo`, and `Expected` return `Unsupported` synchronously in `Service::write`.
  - **Alibaba Cloud OSS:** `Any` / `Fast` compute streaming `CRC64-ECMA` per part and combine on completion. Because OSS reports `x-oss-hash-crc64ecma` in the `CompleteMultipartUpload` response (after completion rather than rejecting pre-commit), a mismatch on completion is a **post-commit** verification failure (not pre-commit atomic rollback), so OSS cannot claim pre-commit `Expected`/`Algo` unless the protocol supports pre-commit rejection. (COS and TOS require separate protocol verification before enabling `write_with_checksum`.)

### `Writer::copy_from` interaction (both native and streaming)

1. **Disabling native `copy_from`:**
   - Verified in `core/core/src/types/context/write.rs#L176`: `WriteState::create` disables native `copy_from` whenever `ctx.args().checksum().is_some()`.
   - Because `WriteState::create` runs outside `Layer::apply_service`, `ChecksumService::capability()` **must** clear `write_can_copy_from = false` whenever write checksum injection is active and `inner.capability().write_with_checksum` is `true`.
   - As defense in depth, any service writer created with `args.checksum().is_some()` must reject `oio::Write::copy_from` with `ErrorKind::Unsupported`.
2. **Preventing silent bypass of source `ChecksumMismatch` during streaming `copy_from`:**
   - Verified in `core/core/src/types/context/write.rs#L322, L503`: during streaming `copy_from`, `WriteState` advances the source offset as chunks are written to the destination. If the source stream fails with `ChecksumMismatch` on the final chunk after all bytes have already been accepted by the destination (`remaining == 0`), a caller retrying `copy_from` per `Writer::copy_from`'s retry contract would hit the `remaining == 0` branch, clear pending state, and commit corrupted bytes on `close()`.
   - **Invariant:** When streaming `copy_from` receives `ErrorKind::ChecksumMismatch` from the source **after any bytes have been written to the destination writer**, `WriteState` must permanently transition the writer into `requires_abort = true` (rejecting further `write`/`copy_from`/`close` and allowing only `abort()`), while preserving resumable retry for ordinary temporary read errors.

### `ChecksumLayer` (`opendal-layer-checksum`)

`ChecksumLayer` has two independent, configurable responsibilities and **no local fake write fallback**:

```rust,ignore
pub struct ChecksumLayer {
    write: Option<WriteChecksumPolicy>, // default: Some(WriteChecksumPolicy::Fast)
    require_write: bool,                // default: false (best-effort)
    verify_read: bool,                  // default: true
    require_read: bool,                 // default: false (best-effort)
}
```

#### Write path
1. If `args.checksum().is_some()`, pass through untouched (caller's explicit per-write option always takes precedence; `ChecksumLayer` never downgrades caller-supplied `Expected` or `Algo`).
2. If `self.write` is `None`, pass through untouched.
3. If `self.write` is `Some(policy)`:
   - If `!self.inner.capability().write_with_checksum`:
     - `require_write == false`: pass `args` through unchanged.
     - `require_write == true`: return `ErrorKind::Unsupported`.
   - If `self.inner.capability().write_with_checksum`:
     - Build `injected = args.clone().with_checksum(policy.into(), &self.inner.capability())?` and call `self.inner.write(ctx, path, injected)`.
     - If `with_checksum` or `inner.write` synchronously returns `Err(e)` with `e.kind() == ErrorKind::Unsupported` and `!self.require_write`: retry `self.inner.write(ctx, path, args)` once without the injected checksum. Because `Service::write` is synchronous and validates before I/O, this has zero network cost, and any unrelated `Unsupported` (e.g., `append` or `CapabilityCheckLayer` rejecting `content_type`) still fails identically on `args`.
     - Otherwise return the result directly.

#### Read path
1. If `!self.verify_read`, pass through untouched.
2. Verification (and `require_read(true)`) applies **only to recognizable single-response full-object reads covering `[0, content_length)`**:
   - `BytesRange::Range { offset: 0, size: None }`, or
   - `BytesRange::Range { offset: 0, size: Some(n) }` when `rp.metadata()` reports `content_length() <= n`.
   - Partial range reads and logical full-object reads that set `chunk` (which `BufferStream` splits into partial range requests at `core/core/src/types/read/buffer_stream.rs#L548`) pass through unverified even when `require_read(true)` is enabled, and are **never** upgraded to full-object downloads. This allows mixed full-object and range workloads (such as Parquet or Lance) to share a single operator.
3. On an eligible full-object read, `ChecksumLayer` sets `OpRead::with_checksum(true)` (enabling opt-in backend checksum headers such as S3 `x-amz-checksum-mode: ENABLED` without affecting operators without `ChecksumLayer`) and inspects `rp.metadata().and_then(Metadata::checksum)`:
   - **Checksum present and algorithm supported by `ChecksumLayer`:**
     - `oio::Read::read`: verify over the returned `Buffer`; on mismatch return `Err(ErrorKind::ChecksumMismatch)` instead of the buffer.
     - `oio::Read::open`: wrap `oio::ReadStreamDyn` in a streaming hasher (zero full-file buffering).
       - **Exact-length early finalization invariant:** When `rp.metadata()` reports `content_length = n`, `BufferStream` stops polling immediately after receiving the `n`-th byte and does **not** poll for an empty EOF buffer (`core/core/src/types/read/buffer_stream.rs#L657`). Therefore, the streaming hasher **must** finalize and compare the digest as soon as `bytes_read == n` **before** yielding that final chunk (failing with `ChecksumMismatch` instead of delivering the final chunk). If `bytes_read > n`, fail immediately.
       - When `content_length` is unknown (or `n == 0`), finalize and compare upon observing the empty EOF buffer.
       - **Terminal error latch:** Once `ChecksumMismatch` is returned, latch the failed state so subsequent polls cannot return `Ok(Buffer::new())`.
   - **Checksum absent or algorithm unknown to `ChecksumLayer`:**
     - `require_read == false`: pass through without hashing.
     - `require_read == true`: return `Err(ErrorKind::Unsupported)` before yielding bytes.
4. `stat()` is never intercepted to read object data.

#### Layer composition (`RetryLayer`, `RouteLayer`, `CacheLayer`, `CapabilityCheckLayer`)
- **`RetryLayer`:** Place `ChecksumLayer` **outside** `RetryLayer` (`op.layer(RetryLayer::new()).layer(ChecksumLayer::new())`). When `RetryLayer` resumes a dropped full-object read from `offset > 0` (`core/layers/retry/src/lib.rs#L950`), `ChecksumLayer` outside it sees the continuous logical `0..EOF` stream and keeps one digest state. Write retries depend on the inner writer's replayability (see [Failure semantics](#failure-semantics)).
- **`RouteLayer`:** `RouteLayer` advertises the default route's `Capability` and re-runs `args.check(&target_cap)` on the matched route (`core/layers/route/src/lib.rs#L35, L240`). Install `ChecksumLayer` inside each routed operator when routes have different `write_with_checksum` capabilities; note that explicit per-write `WriteChecksum` on the outer routed operator is still gated by `RouteLayer`'s advertised capability.
- **`CacheLayer`:** `CacheLayer` v1 persistent pointers (`core/layers/cache/src/record.rs#L33`) do not persist `Metadata::checksum()`. Placing `ChecksumLayer` outside `CacheLayer` verifies final bytes delivered to callers when metadata contains a checksum, but persisted v1 cache hits return `checksum() == None` (skipped in best-effort mode; `Unsupported` under `require_read(true)` until a future cache pointer version stores checksums). Placing `ChecksumLayer` inside `CacheLayer` verifies origin fills into the cache but does not re-verify cache hits.

## Failure semantics

### Visibility and commit outcomes

Services and callers must distinguish three distinct write failure outcomes:

1. **Pre-commit rejection:** The backend rejects a request before committing the object (e.g., S3 `BadDigest` on `PutObject` or `CompleteMultipartUpload`; GCS final resumable `PUT` mismatch). The previous object stays intact (or a new key stays absent). `Operator::write_with` attempts best-effort `abort()` and preserves the original error (`core/core/src/types/operator/operator.rs#L1021`); direct `Writer` callers remain responsible for calling `abort()` to clean up uncommitted multipart uploads.
2. **Post-commit unconfirmed / verification failure:** The backend returns HTTP 200 OK, and subsequent verification fails (e.g., S3 echoes a missing or mismatched checksum header -> `ErrorKind::Unexpected`, `core/services/s3/src/writer.rs#L97`; or OSS returns a mismatched `x-oss-hash-crc64ecma` after `CompleteMultipartUpload`). The object **may already be visible or replaced**; OpenDAL cannot promise the previous object stayed intact.
3. **Append partial commit:** For chunked append paths under `Any`/`Fast` (e.g., Azblob `AppendBlock`), each request commits independently. If chunk 1 succeeds and chunk 2 fails with `ChecksumMismatch`, chunk 1 remains visible and `AppendWriter::abort()` does not roll it back (`core/core/src/raw/oio/write/append_write.rs#L102`).

### Error classification and retryability (`is_temporary`)

Retryability is determined by the **service at the failing protocol step** — never blindly by the `WriteChecksum` variant:

| Failure case | `ErrorKind` | Temporary (`is_temporary`)? | Rationale / caller impact |
| --- | --- | --- | --- |
| `Expected(cs)` wrong digest length | `ConfigInvalid` | No | Fails in `OpWrite::from_options` before I/O |
| Unsupported capability, algorithm, class, or `append` + `Algo`/`Expected` | `Unsupported` | No | Fails in `OpWrite::check` or synchronously in `Service::write` before I/O |
| Single replayable upload request with client-computed buffer digest rejected (e.g., `PutObject` for `Any`/`Fast`/`Algo`, or `UploadPart` / `PutBlock` per-part checksum even during an `Expected` multipart upload) | `ChecksumMismatch` | **Yes** (only when the failing step retains its input buffer and can be safely replayed) | Client computed the digest over the exact transmitted buffer; server rejection indicates in-transit corruption and replay can succeed |
| Final full-object comparison mismatch against caller `Expected(cs)` (`PutObject` or `CompleteMultipartUpload`), or `CompleteMultipartUpload` rejection where replaying `close()` submits identical parts | `ChecksumMismatch` | **No** | Replaying the same request or parts cannot fix a wrong caller digest or deterministic combine mismatch (`RetryLayer` retrying `close()` would loop uselessly) |
| Backend commits `Algo`/`Expected` write without echoing matching full-file checksum | `Unexpected` | **No** | Post-commit ambiguity; object may already be visible |
| Full-object read mismatch in `ChecksumLayer` | `ChecksumMismatch` | **No** | On `oio::Read::read`, corrupted buffer is discarded. On `oio::Read::open` with known `content_length`, error surfaces before yielding the final chunk; earlier chunks already yielded on a stream cannot be recalled |

# Compatibility and Migration

1. **Unreleased PR #7830 (`a22829178`):** `Capability::checksum_{crc64_nvme,crc32c,md5,sha256}` has not shipped in any release. Replacing it with `Capability::write_with_checksum` before the next release avoids any semver break for released users. `.checksum(Checksum)` call sites remain source-compatible via `From<Checksum> for WriteChecksum`.
2. **Bindings (`Java`, `.NET`, etc.):** Replacing the four unreleased capability fields updates the Java/.NET capability mirrors and `website/data/capabilities.json`. Note that Java (`bindings/java/src/error.rs#L52`) and .NET (`bindings/dotnet/src/error.rs#L82`) currently map `ErrorKind::ChecksumMismatch` to `Unexpected`; error-kind mapping is a separate deliverable required whenever a binding exposes checksum options or `ChecksumLayer`.
3. **`Metadata::content_md5()`:** Preserved unchanged.
4. **Per-service `checksum_algorithm` configs:**
   - **New services (GCS, OSS, Azblob, B2, COS, TOS, etc.):** Must not add per-service `checksum_algorithm` config fields; implement `OpWrite::checksum()` instead.
   - **Existing `S3Config::checksum_algorithm`:** Currently accepts `"crc32c"` and `"md5"` (`core/services/s3/src/backend.rs#L1041`); `"md5"` disables `write_can_multi` and any configured value disables `write_can_copy_from` at build time (`backend.rs#L1191, L1228`). Retained **without immediate deprecation** until equivalent migration paths exist for `Content-MD5` single-PUT, explicit headers on custom endpoints, and declarative config / language-binding config maps (`ServiceConfig`), while updating S3 docs to recommend `WriteChecksum` / `ChecksumLayer` for AWS S3. When both are set, per-operation `OpWrite::checksum()` selects the wire checksum header, while build-time capability restrictions from `S3Config::checksum_algorithm` remain in effect.
5. **RFC / Specification lifecycle:** [RFC-6817](6817_checksum.md) remains immutable; upon implementation, this contract is codified in `core/core/src/docs/specs/checksum.md`.

# Drawbacks

- **No static per-algorithm discovery on `Capability`:** Callers cannot inspect `Capability` to choose which algorithm to precompute locally before calling `write` (though callers without a digest should use streaming `WriteChecksum::Fast`/`Any`/`Algo`, and callers holding an external digest receive synchronous zero-I/O `Unsupported` from `Service::write`).
- **Chunked `Reader` reads bypass single-response `ChecksumLayer` verification:** Logical full-object reads that set `chunk` issue partial range requests and are not verified at the `oio::Read` boundary.
- **Conservative `write_can_copy_from` clearing under `ChecksumLayer`:** When `ChecksumLayer` runs in best-effort mode on a service with `write_with_checksum = true` that rejects the layer's policy (e.g., `Fast` on B2), `ChecksumService::capability()` still clears `write_can_copy_from = false`.

# Rationale and alternatives

- **Per-algorithm booleans or bitmask in `Capability` (rejected):** Forces `Capability` and Java/.NET `#[repr(C)]` mirrors to track every provider-specific algorithm (`CRC64-ECMA`, Azure `CRC64`, `CRC32`, `SHA-1`, etc.) and cannot express `Any`/`Fast` or transport-only paths cleanly. A single `write_with_checksum: bool` plus synchronous `Service::write` validation delivers the same zero-I/O rejection with one boolean.
- **Separate `WriteOptions` fields for `checksum` and `checksum_algo` (rejected):** Allows mutually exclusive combinations to be constructed at compile time. A single `Option<WriteChecksum>` enum plus `WriteChecksumPolicy` for `ChecksumLayer` makes illegal states unrepresentable.
- **Require `Any`/`Fast` to always produce full-file `Metadata::checksum()` (rejected):** Excludes Azure Blob per-block CRC64 and B2 large-file per-part SHA-1 from streaming transport protection.
- **Unconditional `x-amz-checksum-mode: ENABLED` on S3 `stat`/`read` (rejected):** Breaks AWS SSE-KMS `HeadObject`/`GetObject` IAM permissions for callers without KMS access and leaks headers into custom `s3://` endpoints and presigned URLs. An explicit `checksum: bool` hint on `StatOptions`/`ReadOptions`/`ReaderOptions` keeps default operations safe while letting callers and `ChecksumLayer` opt in cleanly.
- **Operator-wide rejection of partial range reads under `ChecksumLayer::require_read(true)` (rejected):** Makes `require_read(true)` unusable for Parquet/Lance-style workloads that mix full-object and partial range reads on one operator.
- **Local compute fallback in `ChecksumLayer` (rejected from RFC-6817):** Hashing in-process without backend verification provides zero wire or storage protection and hides `Unsupported`.

# Prior art

- **AWS SDK Flexible Checksums:** Defaults to streaming `CRC64NVME` (`WHEN_SUPPORTED`) with `FULL_OBJECT` multipart combination and opt-in `ChecksumMode=ENABLED` on reads. This proposal adapts that model to multi-cloud storage via `WriteChecksum::Fast`/`Any` and `ChecksumLayer` while preserving explicit per-operation `Unsupported` semantics and KMS-safe opt-in read checksum mode.
- **Google Cloud Storage / Alibaba Cloud OSS clients:** Verify streaming `CRC32C` / `CRC64-ECMA` during upload and download without requiring application-side precomputation.

# Validation

Observable acceptance contracts (implementation plan tracked separately in #5549):

1. **Core, `OpWrite`/`OpRead`/`OpStat`, and `WriteState` unit tests:** `Capability::write_with_checksum` gates all `WriteChecksum` variants; `Expected` wrong digest length returns `ConfigInvalid`; `append` rejects `Algo`/`Expected` and delegates `Any`/`Fast` to `Service::write`; `OpWrite::with_checksum` enforces identical checks for layer injection; `StatOptions`/`ReadOptions`/`ReaderOptions` propagate `checksum: bool` without returning `Unsupported` on services without checksum metadata; `WriteState::create` disables native `copy_from` for direct `OpWrite::checksum()` and `ChecksumLayer`-injected writes; streaming `copy_from` receiving source `ChecksumMismatch` after writing bytes poisons the writer so retrying `copy_from` and calling `close()` both fail (only `abort()` succeeds).
2. **AWS S3 live behavior tests:** Single PUT and concurrent uneven-part multipart uploads for `Any`, `Fast`, `Algo(Crc64Nvme|Crc32c)`, and `Expected(Crc64Nvme|Crc32c)` return verified `FULL_OBJECT` checksums matching independent computation; opt-in `stat_with(...).checksum(true)` and `read_with(...).checksum(true)` return `Metadata::checksum()` for `FULL_OBJECT` and `None` for `COMPOSITE`; default `stat`/`read` without `checksum(true)` and presigned requests send no `x-amz-checksum-mode` header (preserving SSE-KMS IAM permissions and custom `s3://` endpoints); `Expected` mismatch returns non-temporary `ChecksumMismatch` without committing; `Algo(Sha256)`/`Expected(Sha256)` fails synchronously with `Unsupported` before I/O. Rust behavior tests retain explicit S3 `Crc32c`/`Crc64Nvme` coverage plus generic `WriteChecksum::Any` tests for any service with `write_with_checksum = true`.
3. **`ChecksumLayer` tests:** Best-effort fallback on `write_with_checksum = false` and on synchronous service `Unsupported` (zero I/O, non-checksum `Unsupported` preserved); strict mode returns `Unsupported`; full-object `op.read(path)` AND `op.read_with(path).limit(n)` (exact file size) detect corrupted payloads with `ChecksumMismatch` before delivering the final chunk; terminal mismatch stays latched on repeated poll; partial range reads and chunked `Reader` reads pass through unverified even under `require_read(true)` without full-file download; `stat` never triggers `read`; `ChecksumLayer` outside `RetryLayer` verifies resumed streams; `RouteLayer` tested in both directions (default lacks checksum -> routed supports, and vice versa).
4. **Bindings & follow-up services:** Any binding exposing checksum options or `ChecksumLayer` verifies `ErrorKind::ChecksumMismatch` error mapping (not `Unexpected`). Each follow-up service (GCS, Azblob, B2, OSS, etc.) must verify its claimed `WriteChecksum` paths against the actual service before setting `write_with_checksum = true`.

# Unresolved questions

None.

# Future possibilities

- **Chunked/concurrent full-object read verification:** Verify logical full-object reads performed via chunked `Reader` when the backend checksum algorithm is combinable (CRC32C, CRC64-NVME, CRC64-ECMA).
- **CacheLayer checksum persistence:** Extend `CacheLayer`'s persistent pointer format (v2) to store `Metadata::checksum()` so persisted cache hits can be verified by an outer `ChecksumLayer`.
- **Native `copy_from` with checksum:** Support `UploadPartCopy` / server-side copy when source and destination preserve combinable part checksums.
- **Explicit preset opt-in for S3-compatible services:** Allow user-verified S3-compatible endpoints to opt into `write_with_checksum = true` once echo verification guarantees are confirmed against those services.
