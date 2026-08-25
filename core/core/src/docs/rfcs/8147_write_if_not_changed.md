- Proposal Name: `write_if_not_changed`
- Start Date: 2026-08-25
- RFC PR: [apache/opendal#8147](https://github.com/apache/opendal/pull/8147)
- Tracking Issue: [apache/opendal#7889](https://github.com/apache/opendal/issues/7889)

# Summary

Add version match and non-match preconditions to OpenDAL's target-object
operations, and add `if_not_changed(&Metadata)` as the portable
optimistic-concurrency API. A service evaluates every advertised mutation
precondition atomically with the operation that changes the target and returns
`ErrorKind::ConditionNotMatch` when the condition fails.

ETags and versions remain separate identity axes. S3 and Azure implement
`if_not_changed` with an ETag, while GCS uses the object generation exposed as
`Metadata::version()`. The metadata represents caller-provided expected state
and may come from OpenDAL or another source.

# Problem

OpenDAL exposes ETag conditions and historical version selectors, but it lacks
one portable read-modify-write condition. S3 guards replacement with the
current ETag, while GCS uses the current generation. Applications must
currently select a service-specific condition, and an unconditional mutation
after `stat` cannot prevent a concurrent update.

# Proposed design

## Public API

Add `if_version_match` and `if_version_not_match` to stat, read, write, delete,
and copy options. Add the missing `if_none_match` to delete and copy options.
Write, delete, and copy options also accept expected metadata:

```rust
pub if_version_match: Option<String>,
pub if_version_not_match: Option<String>,
pub if_not_changed: Option<Metadata>,

pub fn if_not_changed(self, metadata: &Metadata) -> Self;
```

Operator futures, reader and writer builders expose the corresponding fluent
methods. Blocking APIs accept the same options structs, and `DeleteInput`
accepts the same target conditions as `DeleteOptions`.

`if_not_changed` supports a read-modify-write flow without exposing the
service's identity choice:

```rust,ignore
let mut stream = op.reader(path).await?.into_bytes_stream(..).await?;
let expected = stream.metadata().await?;
let content = stream.try_collect::<Vec<_>>().await?;

op.write_with(path, modify(content))
    .if_not_changed(&expected)
    .await?;
```

Callers may also construct expected metadata from identities obtained through
another trusted channel:

```rust,ignore
let expected = Metadata::default()
    .with_etag(saved_etag)
    .with_version(saved_version);
```

Preserving both fields keeps externally stored metadata portable across
services. Metadata containing only one field is service-aware and returns
`ConfigInvalid` when the service selects the other identity.

The new conditions are not supported by presign operations. Supplying them to
a presign options API returns `Unsupported` before service dispatch.

## Condition semantics

All token values are opaque. A `version` option selects a historical object;
an `if_version_*` option compares the current live target without selecting a
historical version.

| Condition | Successful when |
| --- | --- |
| `if_match(e)` | The current ETag equals `e`. |
| `if_none_match(e)` | The current ETag does not equal `e`. |
| `if_version_match(v)` | The current version equals `v`. |
| `if_version_not_match(v)` | A live target exists and its current version differs from `v`. |
| `if_not_exists` | No live target exists. |
| `if_not_changed(meta)` | The service-native identity in `meta` still matches the target. |

For `if_not_changed`, each service selects one identity it can enforce
atomically. S3 and Azure consume `Metadata::etag()`; GCS consumes
`Metadata::version()`. Other metadata fields do not participate. OpenDAL does
not validate provenance, path, or storage namespace, so the caller must
associate externally constructed metadata with the correct target. Missing the
selected identity returns `ConfigInvalid`.

`if_not_changed` is exclusive with every other target condition and, on
delete, with the target `version` selector. Version match and non-match are
mutually exclusive and cannot be combined with another target condition or a
target version selector. Copy's `source_version` selects a different object
and may be combined with destination conditions. Append writes cannot use
version conditions or `if_not_changed` because append has no portable final
replacement commit point. Invalid combinations return `ConfigInvalid`.

Missing-target results are part of the portable contract:

| Condition | Stat or read | Mutation where supported |
| --- | --- | --- |
| `if_match` | `NotFound` | `ConditionNotMatch` |
| `if_none_match` | `NotFound` | The condition succeeds. Delete is an idempotent no-op. |
| `if_version_match` or `if_version_not_match` | `ConditionNotMatch` | `ConditionNotMatch` |
| `if_not_exists` | N/A | The condition succeeds. |
| `if_not_changed` | N/A | `ConditionNotMatch` |

OpenDAL normalizes native condition failures, including HTTP 304 and 412, to
`ConditionNotMatch`. A successful condition only proves equality under the
selected identity. In particular, ETag-backed services do not provide total
ordering, fencing, or protection from ABA changes that return to the same
ETag.

## Atomicity and capabilities

A service advertises a mutation condition only when it can evaluate the
condition in the native operation that makes the mutation visible. It must not
simulate a mutation precondition with `stat` followed by an unconditional
operation. Staged and multipart writes evaluate the condition at the final
commit; a failure leaves the previously visible target unchanged. Copy
conditions guard the destination.

Conditional deletes may use a native batch only when it preserves each
condition and reports per-object condition failures. Otherwise, OpenDAL routes
conditional entries through a conforming single-object path. Existing
`Deleter` partial-success and retry semantics remain unchanged.

Capabilities follow the existing per-operation naming scheme:

- `{stat,read,write,delete,copy}_with_if_version_{match,not_match}`.
- `{delete,copy}_with_if_none_match`.
- `{write,delete,copy}_with_if_not_changed`.

All new capabilities default to `false`. A service advertises
`*_with_if_not_changed` only when stat and read return metadata containing its
selected identity and every reachable mutation path preserves the condition.
Other metadata-producing paths preserve the identity whenever the native
response provides it. After an effective public capability check succeeds,
the operation must not later degrade to an unconditional request or return
`Unsupported` because of its execution path.

## GCS mapping

GCS maps version conditions to JSON API generation parameters:

| OpenDAL | GCS JSON API |
| --- | --- |
| `if_version_match(v)` | `ifGenerationMatch=v` |
| `if_version_not_match(v)` | `ifGenerationNotMatch=v` |
| `if_not_exists` | `ifGenerationMatch=0` |
| `if_not_changed(meta)` | `ifGenerationMatch=meta.version()` |

GCS generation `0` is reserved for the absence condition and is never an
object generation. GCS rejects `if_version_match("0")` and expected metadata
whose selected version is `"0"` with `ConfigInvalid`.
`if_version_not_match("0")` remains valid and succeeds only when a live object
exists.

GCS applies these parameters to JSON API get, insert, delete, and destination
rewrite requests. `ifGenerationNotMatch` fails when no live object exists, and
OpenDAL maps that failure to `ConditionNotMatch`. Multi-request rewrite keeps
the destination condition across requests.

GCS must use a write path that can preserve generation conditions, such as
JSON resumable upload, before advertising a conditional write capability. It
must also populate `Metadata::version()` from generation values returned by
stat, read, write, copy, and list so OpenDAL-produced metadata can round-trip
through `if_not_changed`.

# Compatibility and migration

The new options and capabilities are additive and default to disabled. Existing
version selectors, ETag conditions, and `if_not_exists` retain their behavior
except for conditional delete when its required live target is absent.

Conditional delete currently inherits unconditional delete's idempotent 404
handling on some services. After this RFC, a delete guarded by `if_match`, a
version condition, or `if_not_changed` returns `ConditionNotMatch` when the
target disappears before commit. This user-visible correction applies to every
binding that exposes the condition.

GCS currently advertises `write_with_if_not_exists` while one multipart path
cannot preserve the condition. The implementation must route that case through
a conforming API or disable the capability until it can honor the contract.

# Rationale and tradeoffs

A single `revision` field would duplicate GCS version and S3 ETag while
suggesting ordering, per-write uniqueness, or ABA resistance that ETags do not
provide. Keeping the axes explicit preserves their native meaning.

Applications could choose `if_match` or `if_version_match` after inspecting
capabilities, but that would expose service differences at every call site.
`if_not_changed` centralizes the choice and reuses `Metadata`, which is already
returned by object operations and can be constructed by callers. Carrying the
metadata costs more option space than carrying one token, but avoids another
public identity type and keeps the explicit conditions available when callers
need a specific axis.
