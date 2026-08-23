- Proposal Name: `gcs_versioned_read_and_write`
- Start Date: 2026-08-23
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#7889](https://github.com/apache/opendal/issues/7889)
- AI Assistance: This RFC was drafted using Codex (GPT-5.6 Sol) and was
  human-reviewed before publication.

# Summary

Implement the existing `read_with_version` capability for Google Cloud Storage
(GCS) and add version-matched writes to OpenDAL.

The public write API gains a `version` option and a corresponding
`Capability::write_with_version` flag. For GCS, the version is the object's
[generation][gcs-object-metadata]. A versioned read selects that generation,
while a versioned write uses the generation as an optimistic-concurrency
precondition for replacing the live object.

# Motivation

OpenDAL already represents a GCS object generation as `Metadata::version()`.
The core read API also accepts `ReadOptions::version`, but the GCS service does
not advertise `read_with_version` and does not add the requested generation to
download requests. Users can observe a generation but cannot use it to read the
corresponding object revision through GCS.

GCS also uses [generation rather than ETag preconditions][gcs-preconditions] for
conditional writes. Its JSON API accepts `ifGenerationMatch`, which writes a
new object only when the live object's generation matches the supplied value.
OpenDAL currently has no write option for this service-native condition.

[apache/opendal#7890](https://github.com/apache/opendal/pull/7890) proposed
passing a GCS generation through `if_match`. Review established that OpenDAL
must not reinterpret `if_match`: the option has an ETag contract, and portable
code must not switch between `Metadata::etag()` and `Metadata::version()` based
on the service. A separate version option preserves that contract and exposes
the condition that GCS actually supports.

Together, versioned reads and version-matched writes support an optimistic
read-modify-write loop:

1. Read or stat an object and retain its version.
2. Compute the replacement data.
3. Write only if the live object still has the retained version.
4. Retry the operation when another writer wins the race.

This pattern is useful for manifests, metadata catalogs, locks, and other small
coordination objects stored in GCS.

# Guide-level explanation

`Metadata::version()` returns the service-specific version identifier. For GCS,
this value is the object generation.

Users can select an object revision when reading:

```rust,ignore
let metadata = op.stat("manifest.json").await?;
let generation = metadata.version().expect("GCS returns a generation");

let data = op
    .read_with("manifest.json")
    .version(generation)
    .await?;
```

When [GCS Object Versioning][gcs-object-versioning] retains noncurrent objects,
the same API reads an older generation after the live object changes:

```rust,ignore
let first = op.stat("manifest.json").await?;
let first_generation = first.version().expect("version must be present");

op.write("manifest.json", "new contents").await?;

let old_contents = op
    .read_with("manifest.json")
    .version(first_generation)
    .await?;
```

If the bucket does not retain that generation, GCS returns `NotFound` after it
becomes noncurrent. GCS clients [address retained object versions by
generation][gcs-using-versioned-objects].

Users can also replace the live object conditionally:

```rust,ignore
let metadata = op.stat("manifest.json").await?;
let expected_version = metadata.version().expect("version must be present");

let result = op
    .write_with("manifest.json", "replacement contents")
    .version(expected_version)
    .await;

match result {
    Ok(new_metadata) => {
        // The service accepted the replacement and created a new version.
    }
    Err(err) if err.kind() == ErrorKind::ConditionNotMatch => {
        // The object no longer has expected_version. Reload and retry.
    }
    Err(err) => return Err(err),
}
```

For a write, `version` is an expected version, not a destination version. The
operation never changes historical data and never asks GCS to assign the
supplied generation to the result. GCS does not support specifying the
generation of a destination object, and a successful replacement receives a
[new service-assigned generation][gcs-using-versioned-objects].

Users should inspect both capabilities before using these options:

```rust,ignore
let capability = op.info().capability();
assert!(capability.read_with_version);
assert!(capability.write_with_version);
```

# Reference-level explanation

## Public API

Add `version` to `WriteOptions`:

```rust,ignore
pub struct WriteOptions {
    /// Replace the live object only if its version matches this value.
    pub version: Option<String>,
    // Existing fields remain unchanged.
}
```

Add `version(&str)` to the futures returned by `Operator::write_with` and
`Operator::writer_with`. `Operator::write_options` and
`Operator::writer_options` accept the same option through `WriteOptions`.
Blocking option-based writes receive the field through the existing
`WriteOptions` API.

The raw API carries the condition without interpreting it:

```rust,ignore
pub struct OpWrite {
    version: Option<String>,
    // Existing fields remain unchanged.
}

impl OpWrite {
    pub fn with_version(mut self, version: &str) -> Self;
    pub fn version(&self) -> Option<&str>;
}
```

Add the following capability:

```rust,ignore
pub struct Capability {
    /// Indicates whether writes can require the live object's version to match.
    pub write_with_version: bool,
    // Existing fields remain unchanged.
}
```

`write_with_version` is meaningful only when `write` is also `true`. A service
advertises it only when both one-shot and multi-write paths enforce the version
condition atomically. Layers that reconstruct `OpWrite` must preserve the
field.

The correctness-check and capability-check layers reject a supplied version
when `write_with_version` is `false`. The correctness check is required because
silently dropping this condition can overwrite concurrent changes.

## Write contract

`WriteOptions::version` has the following service-independent contract:

- The value identifies the expected version of the live destination object.
- The service performs the check and replacement atomically.
- A matching version allows the write and creates a new service-assigned
  version.
- A missing object or mismatched version returns
  `ErrorKind::ConditionNotMatch` and does not create or replace the object.
- An unsupported version condition returns `ErrorKind::Unsupported` before any
  data is committed.
- Omitting the option preserves the existing overwrite behavior.

`version` and `if_not_exists` describe mutually exclusive expected states. A
write that supplies both returns `ErrorKind::Unsupported` before sending data.
The GCS service continues to report `write_with_if_match` and
`write_with_if_none_match` as `false`; both options retain their ETag semantics.

Version identifiers remain opaque strings in OpenDAL. A service can validate
its own representation, but core code and layers must not parse, normalize, or
compare them.

Presigned version-matched writes are outside this proposal. Presign APIs reject
a write version instead of generating a request that omits the condition. A
future proposal can add a distinct presign capability and define how clients
must send signed precondition headers.

## GCS versioned reads

GCS advertises `read_with_version: true`. The service maps
`OpRead::version()` to the JSON API [`generation` query parameter on
`objects.get`][gcs-objects-get], including range requests opened by `Reader`.

The option selects a specific object revision. It is not a generation-match
precondition. If the requested generation does not exist, the service maps the
GCS [`404 Not Found` response][gcs-json-status-codes] to
`ErrorKind::NotFound`.

[GCS assigns a generation to every object][gcs-object-versioning], independently
of whether the bucket retains noncurrent objects, so the service does not add an
`enable_versioning` configuration option. Bucket Object Versioning only
determines whether an older generation remains available after replacement or
deletion.

## GCS version-matched writes

GCS advertises `write_with_version: true` and maps `OpWrite::version()` to the
JSON API [`ifGenerationMatch` query parameter][gcs-objects-insert]. A
[`412 Precondition Failed` response][gcs-preconditions] maps to
`ErrorKind::ConditionNotMatch` through the existing GCS error parser.

The one-shot writer adds the condition to the existing
[`objects.insert` request][gcs-objects-insert]. The request uses the version
verbatim as an opaque query value; GCS validates that it is a generation number.

[GCS's XML API multipart uploads cannot enforce request
preconditions][gcs-preconditions]. Therefore, the service must not route a
version-matched multi-write through the current XML multipart implementation.
It uses a [JSON resumable upload][gcs-resumable-uploads] instead and includes
`ifGenerationMatch` in the `objects.insert` request that creates the upload
session. GCS returns a session URI, which the writer uses for the subsequent
data requests as described by the [resumable upload flow][gcs-resumable-flow].
The service advertises `write_with_version` only after both paths enforce the
condition.

The conditional resumable-upload path can also carry `ifGenerationMatch=0`,
which [requires that no live object exists][gcs-preconditions], for
`if_not_exists`. Reusing it fixes the existing GCS multi-write gap tracked by
[apache/opendal#8040](https://github.com/apache/opendal/issues/8040) without
changing the `if_not_exists` contract.

Unconditional GCS writes can continue using the existing one-shot JSON and XML
multipart paths. This keeps the new implementation isolated from writes that do
not need generation preconditions.

## Testing

Core tests verify that:

- `WriteOptions::version` reaches `OpWrite` unchanged.
- `write_with(...).version(...)` and `writer_with(...).version(...)` set the
  option.
- Correctness and capability checks reject unsupported versioned writes.
- Conflicting `version` and `if_not_exists` options fail before a write starts.

GCS request tests verify that:

- Versioned reads add `generation` without changing range or conditional-read
  headers.
- One-shot versioned writes add `ifGenerationMatch`.
- Conditional multi-writes use the JSON resumable path rather than XML
  multipart upload.
- Unconditional requests remain unchanged.

Behavior tests against GCS verify that:

- Reading the current generation succeeds.
- Reading a retained noncurrent generation returns its original data.
- Writing with the live generation succeeds and returns the replacement data.
- Reusing a stale generation returns `ConditionNotMatch` and preserves the
  winning write.
- Multi-write operations enforce the same matching and stale-generation
  behavior.

The historical-read test requires a GCS fixture with
[Object Versioning enabled][gcs-object-versioning]. The conditional-write tests
only require the generation of a live object.

# Compatibility and migration

The proposal adds an optional write field, a builder method, and a capability.
Existing writes that do not set `version` keep their current behavior.

Adding a public field to `WriteOptions` affects exhaustive struct literals.
Callers that construct options with `..Default::default()` remain source
compatible, following the existing options API convention.

The proposal does not change `if_match` or `if_none_match`. In particular, GCS
users migrating from experiments based on apache/opendal#7890 replace:

```rust,ignore
op.write_with(path, data).if_match(generation).await?;
```

with:

```rust,ignore
op.write_with(path, data).version(generation).await?;
```

# Drawbacks

The word `version` has operation-specific behavior: reads select a revision,
while writes check the live destination before creating a new revision. The API
documentation must make this distinction prominent.

Supporting the condition for streaming GCS writes requires a
[JSON resumable upload implementation][gcs-resumable-uploads] in addition to
the existing XML multipart writer. This increases the GCS service's request and
retry complexity.

The capability adds another field to the already large `Capability` struct and
another condition that layers must preserve.

# Rationale and alternatives

## Reuse `if_match`

This would avoid a public API addition, but it would give `if_match` two
incompatible meanings. Most services expect an ETag, while GCS would expect a
generation. Generic callers would need service-specific branching even though
the capability name is identical. Review on apache/opendal#7890 rejected this
approach.

## Add a GCS-specific generation option

Names such as `if_generation_match` accurately describe GCS, but they leak one
provider's vocabulary into the service-independent Operator API. Other services
and integrations already expose opaque version identifiers, so `version`
provides a portable contract.

## Name the option `if_version_match`

This name makes the write semantics more explicit. The proposal uses `version`
to match `ReadOptions`, `StatOptions`, and `DeleteOptions`, while documenting
that the write value is an expected live version. The capability name
`write_with_version` follows OpenDAL's existing naming convention.

## Support only one-shot GCS writes

The current JSON insert path can enforce
[`ifGenerationMatch`][gcs-objects-insert] without a new upload implementation.
However, advertising `write_with_version` while a chunked writer silently
ignores or inconsistently rejects the condition would repeat the capability
mismatch documented in apache/opendal#8040. The proposal requires atomic
enforcement across both paths.

## Gate GCS capabilities behind an `enable_versioning` option

GCS [exposes a generation for every object][gcs-object-versioning] and accepts
generation conditions even when bucket Object Versioning is disabled. A
builder flag would describe bucket retention policy rather than protocol
support, could drift from the actual bucket configuration, and is unnecessary
for conditional writes or reads of the live generation.

# Prior art

The [GCS JSON `objects.get` API][gcs-objects-get] uses `generation` to select a
revision. The [GCS JSON `objects.insert` API][gcs-objects-insert] uses
`ifGenerationMatch` to make replacement conditional on the live generation.
GCS recommends generation preconditions for safe read-modify-write updates in
its [request preconditions documentation][gcs-preconditions]. The
[object metadata documentation][gcs-object-metadata] defines generation and
metageneration, while the [Object Versioning documentation][gcs-object-versioning]
explains when noncurrent generations remain available. The
[resumable uploads documentation][gcs-resumable-uploads] defines the JSON API
flow used by conditional multi-writes.

The Rust [`object_store` crate](https://docs.rs/object_store/latest/object_store/)
models conditional updates with `UpdateVersion`, which keeps `e_tag` and
`version` as separate optional identifiers. This separation lets each backend
use its native concurrency token without changing the ETag contract.

OpenDAL already uses opaque versions for stat, read, delete, list, and copy
operations. This proposal extends that model to destination write
preconditions.

# Unresolved questions

None.

# Future possibilities

- The `object_store` integration can map `UpdateVersion::version` to
  `WriteOptions::version` when the underlying operator advertises
  `write_with_version`.
- Other services can implement `write_with_version` when they provide an
  atomic destination-version precondition distinct from ETag matching.
- A later RFC can define presigned version-matched writes and the corresponding
  capability.
- GCS can add `stat_with_version`, `delete_with_version`, and
  `list_with_versions` independently to complete its object-versioning surface.

[gcs-object-metadata]: https://cloud.google.com/storage/docs/metadata
[gcs-object-versioning]: https://cloud.google.com/storage/docs/object-versioning
[gcs-using-versioned-objects]: https://cloud.google.com/storage/docs/using-versioned-objects
[gcs-objects-get]: https://cloud.google.com/storage/docs/json_api/v1/objects/get
[gcs-objects-insert]: https://cloud.google.com/storage/docs/json_api/v1/objects/insert
[gcs-preconditions]: https://cloud.google.com/storage/docs/request-preconditions
[gcs-resumable-uploads]: https://cloud.google.com/storage/docs/resumable-uploads
[gcs-resumable-flow]: https://cloud.google.com/storage/docs/performing-resumable-uploads
[gcs-json-status-codes]: https://cloud.google.com/storage/docs/json_api/v1/status-codes
