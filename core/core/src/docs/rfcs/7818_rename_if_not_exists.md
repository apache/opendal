- Proposal Name: `rename_if_not_exists`
- Start Date: 2026-06-24
- RFC PR: [apache/opendal#7818](https://github.com/apache/opendal/pull/7818)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

# Summary

Extend rename with an `if_not_exists` option:

```rust
op.rename_with("staging/file", "published/file")
    .if_not_exists(true)
    .await?;
```

The existing `rename` API keeps its overwrite semantics. When
`if_not_exists` is enabled, rename succeeds only if the destination does not
exist. A destination conflict returns `ConditionNotMatch` without modifying the
source or destination.

# Motivation

OpenDAL defines `rename` as an overwrite operation. Some applications also need
an atomic publish primitive: move a completed staging file into place only when
no other writer has already published that destination.

A caller cannot implement this safely with `stat` followed by `rename`. Another
writer can create the destination after `stat` reports that it is absent but
before rename runs. A service configuration flag is also unsuitable because it
would make the meaning of the same `rename` call depend on backend construction
rather than an explicit call-site condition.

OpenDAL already models the equivalent destination condition for write and copy
through options:

```rust
op.write_with("path", content)
    .if_not_exists(true)
    .await?;

op.copy_with("source", "target")
    .if_not_exists(true)
    .await?;
```

Rename should follow the same public API and error model.

# Guide-level explanation

Use `rename` when the destination may be replaced:

```rust
use opendal::{Operator, Result};

async fn replace(op: Operator) -> Result<()> {
    op.rename("staging/file", "published/file").await?;
    Ok(())
}
```

Use `rename_with(...).if_not_exists(true)` when an existing destination must be
preserved:

```rust
use opendal::{ErrorKind, Operator, Result};

async fn publish(op: Operator) -> Result<()> {
    match op
        .rename_with("staging/file", "published/file")
        .if_not_exists(true)
        .await
    {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::ConditionNotMatch => Err(err),
        Err(err) => Err(err),
    }
}
```

The conditional operation has the following outcomes:

- If the destination does not exist, the source is renamed to the destination.
- If the destination exists, the operation returns `ConditionNotMatch` and
  leaves both paths unchanged.
- If the service cannot enforce the destination condition atomically, the
  operation returns `Unsupported`.
- If source and destination are the same path, the operation returns
  `IsSameFile`, matching normal rename.

Users can inspect `Capability::rename_with_if_not_exists` before enabling the
option.

Blocking users configure the same condition through `RenameOptions`:

```rust
use opendal::blocking;
use opendal::options::RenameOptions;
use opendal::Result;

fn publish(op: blocking::Operator) -> Result<()> {
    let mut options = RenameOptions::default();
    options.if_not_exists = true;
    op.rename_options("staging/file", "published/file", options)?;
    Ok(())
}
```

# Reference-level explanation

## Public API

Add `RenameOptions`:

```rust
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct RenameOptions {
    pub if_not_exists: bool,
}
```

The asynchronous operator exposes:

```rust
impl Operator {
    pub async fn rename(&self, from: &str, to: &str) -> Result<()>;

    pub fn rename_with(
        &self,
        from: &str,
        to: &str,
    ) -> FutureRename<impl Future<Output = Result<()>>>;

    pub async fn rename_options(
        &self,
        from: &str,
        to: &str,
        options: impl Into<RenameOptions>,
    ) -> Result<()>;
}
```

`rename` delegates to `rename_options` with default options. `FutureRename`
provides:

```rust
impl<F: Future<Output = Result<()>>> FutureRename<F> {
    pub fn if_not_exists(self, value: bool) -> Self;
}
```

The blocking operator follows existing blocking options APIs:

```rust
impl blocking::Operator {
    pub fn rename_options(
        &self,
        from: &str,
        to: &str,
        options: RenameOptions,
    ) -> Result<()>;
}
```

No standalone `rename_if_not_exists` method is added. The options API matches
write and copy and leaves room for future composable rename conditions.

## Service API

`RenameOptions` converts into the raw `OpRename`:

```rust
pub struct OpRename {
    if_not_exists: bool,
}
```

The `Service::rename` signature does not change. Services inspect
`OpRename::if_not_exists()` to select their native overwrite or no-overwrite
operation.

Add a capability field:

```rust
pub struct Capability {
    pub rename: bool,
    pub rename_with_if_not_exists: bool,
}
```

`rename_with_if_not_exists` is meaningful only when `rename` is also supported.
The correctness check returns `Unsupported` before dispatch when the option is
enabled but the service does not advertise the capability.

## Error semantics

`if_not_exists` is a destination precondition, so a destination conflict maps
to `ConditionNotMatch`. This is consistent with existing write and copy
behavior.

Backend errors caused by the native no-replace condition must be translated to
`ConditionNotMatch`, even when the backend or operating system reports a native
error such as `AlreadyExists`.

Other errors retain their normal meanings. For example, a missing source
returns `NotFound`, a directory passed where a file is required returns the
corresponding directory error, and an unsupported condition returns
`Unsupported`.

## Atomicity requirement

A service must advertise `rename_with_if_not_exists` only when the destination
condition and rename are enforced by one native operation. Performing a
destination check followed by an overwriting rename has a time-of-check to
time-of-use race and does not satisfy the capability.

When the destination condition fails:

- The source remains at its original path.
- The destination remains unchanged.

When the operation succeeds:

- The source no longer exists at its original path.
- The destination contains the source data.

This RFC does not require rename to be crash-atomic beyond the guarantees of the
underlying service. It requires the no-replace decision to be atomic with
respect to competing destination creation.

## Service analysis

### HDFS

HDFS exposes rename options through `Options.Rename`. libhdfs `hdfsRename`
invokes rename with `Options.Rename.NONE`, which rejects an existing
destination. The HDFS service can therefore advertise
`rename_with_if_not_exists` and use that native operation directly.

Normal OpenDAL rename continues to provide overwrite behavior. Where the
libhdfs operation does not overwrite, the HDFS service removes an existing file
destination before invoking native rename.

The HDFS implementation must translate both an existing destination discovered
before the call and a native conflict reported by the final rename into
`ConditionNotMatch`. The final native operation remains authoritative so a
concurrent creator cannot be overwritten.

### FS

The FS service currently uses `tokio::fs::rename`. Normal rename behavior and
destination replacement differ across operating systems, and this API does not
offer a portable no-replace option.

The FS service must not advertise `rename_with_if_not_exists` with a
`metadata`-then-`rename` implementation. A future implementation may use native
primitives such as Linux `renameat2(RENAME_NOREPLACE)` and equivalent supported
operations on other platforms, with a clearly defined portability policy.

Any existing cross-platform differences in default FS overwrite behavior are
outside this RFC and should be handled separately.

### S3 and object stores without rename

S3 does not expose a native rename operation, and the S3 service does not
advertise `rename`. S3 supports conditional copy, but conditional copy followed
by delete is not an atomic rename:

- The copy may succeed while deleting the source fails.
- Other clients can observe both source and destination between operations.
- Retrying after partial completion has different semantics from native rename.

Therefore, `copy_with(...).if_not_exists(true)` plus delete must not be used to
advertise either rename capability. Other object stores with the same operation
model follow the same rule.

### Other services

Services can adopt `rename_with_if_not_exists` independently when they have a
native rename primitive that rejects destination replacement atomically. A
service-specific implementation may translate native error codes, but it must
preserve the common OpenDAL result and path-state contract.

# Drawbacks

The public options and capability surfaces gain new fields and a new
`FutureRename` alias.

Only a subset of rename-capable services can initially support the option.
Callers that operate across arbitrary services must inspect capability or handle
`Unsupported`.

The common error kind hides backend-specific conflict errors. This is
intentional for portable conditional-operation handling but removes some native
detail from the primary error kind.

# Rationale and alternatives

## Add standalone `rename_if_not_exists`

A separate method is easy to discover but diverges from OpenDAL's write and
copy APIs. Rename conditions belong in `RenameOptions`, and `rename_with`
provides the established fluent API.

## Change default rename behavior

Changing `rename` to reject an existing destination would break the existing
overwrite contract and callers that rely on replacement.

## Add an HDFS configuration flag

A backend configuration flag would make the meaning of a standard operation
depend on service construction. The destination condition is a per-call
requirement and should be explicit at the call site.

## Simulate with stat then rename

This cannot enforce the condition under concurrent destination creation and
must not be exposed as the native capability.

## Simulate with conditional copy then delete

This can be useful in application-specific workflows, but it is not an atomic
rename and has partial-success states. OpenDAL should not expose it under the
same contract.

# Prior art

OpenDAL already provides `if_not_exists` options for write and copy, with
`ConditionNotMatch` representing a failed destination condition.

HDFS provides `Options.Rename.NONE` and `Options.Rename.OVERWRITE`. Linux
provides `renameat2` with `RENAME_NOREPLACE`. Rust's `object_store` crate
exposes a conditional rename operation as a distinct method; OpenDAL adopts the
same semantic capability while retaining its established options-based API.

# Unresolved questions

- Which additional filesystem platforms have a suitable native no-replace
  primitive that OpenDAL can support without weakening the contract?
- Should OpenDAL eventually provide a separate, explicitly non-atomic
  copy-then-delete move helper for object stores?

Neither question blocks the HDFS implementation or the common API.

# Future possibilities

Additional rename conditions can be added to `RenameOptions` if services expose
portable native support.

The FS service can advertise `rename_with_if_not_exists` on platforms where a
native implementation is available and the project defines the desired
cross-platform capability behavior.

Applications that accept non-atomic move semantics can build a separate helper
from copy and delete without changing the guarantees of rename.
