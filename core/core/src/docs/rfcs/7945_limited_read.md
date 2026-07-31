- Proposal Name: `limited_read`
- Start Date: 2026-07-24
- RFC PR: [apache/opendal#7945](https://github.com/apache/opendal/pull/7945)
- Tracking Issue: [apache/opendal#7938](https://github.com/apache/opendal/issues/7938)

# Summary

Add `limit` to `Operator::read_with` and `ReadOptions`.

`range` continues to request an exact bounded range. `limit` caps the amount of
data returned but accepts a clean end of file before the cap. This supports
small probes, such as reading up to 16 KiB from an object whose size is unknown,
without a preceding `stat`.

The implementation reuses the existing read path. It adds no raw operation or
new planning type. Core only carries an `exact` boolean so the component that
collects a stream knows whether a clean short read is an error.

# Motivation

Callers often need only the beginning of an object:

- inspect a file signature or header;
- detect a format;
- parse metadata stored near the beginning;
- sample a small object without first knowing its size.

Today a caller can use `range(0..N)`, but a bounded range is exact. Reading an
object shorter than `N` fails even though the bytes that do exist are sufficient
for these use cases. The caller can avoid the error by calling `stat` first and
then choosing a smaller range, but that adds a request and introduces a race
between metadata lookup and data access.

OpenDAL needs an at-most read alongside its existing exact range read.

# Guide-level explanation

Use `limit` when any number of bytes from zero through the limit is a successful
result:

```rust
let header = op
    .read_with("path/to/file")
    .limit(16 * 1024)
    .await?;
```

If the object contains at least 16 KiB, this returns 16 KiB. If it contains less,
this returns the whole object. OpenDAL does not perform a `stat` before reading.

`limit` can start at an offset:

```rust
let data = op
    .read_with("path/to/file")
    .range(4096..)
    .limit(1024)
    .await?;
```

This returns at most 1024 bytes starting at offset 4096. If the offset is at or
beyond the end of the object, the result is empty.

`range` without `limit` keeps its current behavior:

```rust
let data = op
    .read_with("path/to/file")
    .range(0..16 * 1024)
    .await?;
```

This succeeds only when the complete 16 KiB range is available. A clean end of
file before the requested end remains an error.

For a non-empty limit, `limit` changes only clean end-of-file handling. A
missing object, a failed condition, a permission failure, or a transport error
still fails the read.

# Reference-level explanation

## Public API

`ReadOptions` gains one field:

```rust,ignore
pub struct ReadOptions {
    pub range: BytesRange,
    pub limit: Option<u64>,
    // Existing fields.
}
```

`FutureRead` gains the matching builder:

```rust,ignore
pub fn limit(mut self, limit: u64) -> Self {
    self.args.limit = Some(limit);
    self
}
```

`limit` applies after `range`. Core converts the two options into one physical
`BytesRange`:

| Options | Physical range | Completion |
| --- | --- | --- |
| no `range`, `limit(n)` | offset 0, size `n` | at most `n` |
| `range(offset..)`, `limit(n)` | offset `offset`, size `n` | at most `n` |
| `range(start..end)`, `limit(n)` | offset `start`, size `min(end - start, n)` | at most that size |
| bounded `range` without `limit` | unchanged | exact |

The initial API rejects combining a suffix range with `limit`. A suffix is
relative to the object end, so resolving it without metadata would defeat the
no-`stat` property.

`limit(0)` returns an empty buffer without checking the object, consistent with
an empty range.

Combining `limit` with a suffix range, an explicit `chunk` size, or a
`concurrent` value greater than one returns `ErrorKind::ConfigInvalid` before
storage I/O.

## Exact completion

Core carries a private boolean named `exact` with the normalized range. It does
not introduce a public type or a new raw abstraction.

- A regular bounded, non-suffix range sets `exact` to `true`.
- A read with `limit` sets `exact` to `false`.
- An open-ended or suffix range sets `exact` to `false`.

The buffer stream tracks the number of bytes it yields. At clean end of file, it
compares that count with the bounded range size only when `exact` is `true`.
This preserves current exact range behavior while allowing limited reads to
finish early.

The `exact` flag belongs to the public read execution path. It is not passed to
services because services should not decide whether a caller accepts a short
result.

## Raw read contract

This proposal keeps the raw `oio::Read` API unchanged:

```rust,ignore
pub trait Read {
    fn open(&self, range: BytesRange) -> ...;
    fn read(&self, range: BytesRange) -> ...;
}
```

The two existing methods already provide the required split:

- `open(range)` returns the bytes available inside the range, stops at the range
  boundary, and treats a clean end of file as normal stream completion.
- `read(range)` remains an exact bounded read for chunked and concurrent
  planning. It returns the complete range or an error.

`PositionReadStream` therefore treats an empty positioned read as clean EOF,
while `PositionReader::read` keeps rejecting EOF before its exact bounded read
is complete. Stream-based services follow the same contract.

When a service reports that a range starts at or beyond EOF, its `open` path
normalizes that response to an empty stream if the object is known to exist.
The core exactness check then rejects the result for an exact range and accepts
it for a limited read. A missing object remains `NotFound`.

`CompleteLayer` also separates these responsibilities. Its `read(range)` path
continues to require the exact bounded size. Its `open(range)` path validates
the service stream rather than imposing public exactness:

- it always rejects bytes beyond the requested range;
- when `RpRead` contains the full object length, it requires exactly the bytes
  available in the requested range;
- without that metadata, it relies on the service to distinguish clean EOF from
  a truncated response.

HTTP services still validate the response body's `Content-Length`, so accepting
object EOF does not turn a truncated network response into success.

## Execution

A limited read opens one bounded stream and collects it. It does not split the
limit into speculative exact reads because the final chunk may legitimately
cross the object end.

The initial implementation rejects `limit` together with an explicit `chunk`
size or `concurrent` value greater than one. This keeps their exact chunk
contract intact instead of silently changing or ignoring execution options.
Limited reads target small probes; support for parallel limited reads can be
added later if a real workload requires it.

No capability flag is needed. Every readable service already supports the
`open(range)` path needed by this API.

`presign_read_options` rejects `limit`. A presigned request is executed by the
caller, so OpenDAL cannot apply its completion check to the response.

## Compatibility and validation

Existing reads do not set `limit`, so their normalized ranges and behavior
remain unchanged. As with other additions to `ReadOptions`, callers that
construct the public struct without `..Default::default()` must initialize the
new field.

Implementation tests should cover:

- objects shorter than, equal to, and longer than the limit;
- `range(offset..).limit(n)` before, at, and beyond EOF;
- unchanged failure for a short exact range;
- unchanged propagation of non-EOF errors;
- rejection of suffix, chunked, and concurrent combinations;
- detection of a truncated HTTP body;
- both stream-based and positioned-read services.

# Drawbacks

`range` and `limit` are similar size controls with deliberately different EOF
semantics. Documentation must make the exact-versus-at-most distinction clear.

The first implementation does not combine limited reads with suffix ranges or
parallel chunk planning.

# Rationale and alternatives

## Add a separate operation

A method such as `read_up_to` would make the semantic difference obvious, but it
would duplicate all existing read options and their builder surface. `limit`
fits the current `read_with` model and composes naturally with an offset range.

## Change bounded ranges to accept EOF

Rejected. Exact bounded ranges are useful for validating file structure and for
safe concurrent chunk planning. Changing their behavior would weaken an
existing contract.

## Call `stat` before a range read

Rejected. It adds latency and cannot make the subsequent read atomic with the
metadata result. Services can already report the available range through the
read response.

## Add a raw method or planning type

Rejected. The existing `open` and `read` methods already distinguish streaming
from exact materialization. A local `exact` boolean expresses the only missing
decision without adding `ReadPlan`, `ReadCompletion`, or another service-facing
contract.

# Prior art

Rust I/O adapters commonly use a limit to cap bytes while treating EOF before
that limit as normal. This proposal applies the same expectation to OpenDAL's
read request construction.

RFC-0090 introduced limited readers to prevent over-reading, while RFC-7660
separated range streams from exact bounded raw reads. This proposal builds on
those boundaries and adds the missing public at-most completion semantics.

# Unresolved questions

None.

# Future possibilities

OpenDAL can support suffix ranges or parallel planning with `limit` later if it
can preserve at-most completion without a metadata request or speculative
requests beyond EOF.
