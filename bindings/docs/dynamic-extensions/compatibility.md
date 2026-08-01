<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Extension Compatibility and ABI

Status: pre-RFC contract for the exact-release runtime candidate. OpenDAL does
not provide or guarantee this interface today.

The design uses one compatibility version: the OpenDAL version. A native
extension must target exactly the version used by the runtime. The target
identity remains a separate platform check, not another versioning scheme.

The key constraint is deliberate:

> The candidate design provides stable public language interfaces, but it does
> not provide a stable Rust extension ABI across OpenDAL releases.

Every native extension rebuilds for every OpenDAL release. Equal layouts in two
releases do not create a supported compatibility range.

## Public Language Interfaces

Python, Ruby, and Node.js continue to version their public APIs according to
their existing binding policies. CPython `abi3`, Node-API, and Ruby or Magnus
compatibility describe the interface between a binding adapter and its language
runtime. They are informational for the native extension loader and do not
replace the exact OpenDAL version check.

## Configuration Value Contract

Factories receive language-neutral configuration values. The matching OpenDAL
release defines this closed grammar:

```text
ConfigValue =
    Null
  | Bool(bool)
  | I64(i64)
  | U64(u64)
  | F64(finite IEEE-754 binary64)
  | Utf8(string)
  | Bytes(byte sequence)
  | List(sequence<ConfigValue>)
  | Map(map<Utf8, ConfigValue>)
  | SignedDuration(seconds: i64, nanoseconds: i32)
```

`SignedDuration` is the only duration representation at the shared factory
boundary. It matches `jiff::SignedDuration`: the nanosecond field has an
absolute value below one second and has the same sign as the seconds field when
both are non-zero. This preserves the full signed range without squeezing total
nanoseconds into one `i64`.

Binding-specific interfaces can accept seconds, milliseconds, or native
duration objects, but their adapters must define nanosecond rounding and reject
non-finite, non-canonical, or overflowing values. Each service or layer decides
whether its configuration accepts negative or zero durations. Numeric
conversion never silently truncates, saturates, or wraps.

The decoder enforces a maximum nesting depth of 32, a maximum UTF-8 map key of
4 KiB, a maximum individual string or byte value of 16 MiB, a maximum of 65,536
entries in one list or map, and a maximum encoded request size of 64 MiB. It
rejects invalid UTF-8, duplicate map keys, non-finite floats, unknown value tags,
and values outside the declared numeric ranges before calling package code.

A package owns its configuration fields, defaults, validation, credentials, and
redaction behavior. The matching OpenDAL release defines the factory request;
the runtime does not negotiate separate grammar or configuration-schema
versions.

Schema behavior follows these rules:

- A missing field selects its declared default or produces a missing-field
  error.
- `Null` is valid only when the field is explicitly nullable. Optional and
  nullable are separate properties.
- A package rejects every field absent from its schema.
- A schema marks credential and token fields as secret. The runtime never logs
  raw configuration values, and package errors identify a secret field without
  rendering its value.

`UriRequest` remains separate. It contains one UTF-8 URI and a map of UTF-8
option names to UTF-8 option values, matching current iterator construction.
The runtime applies the same size limits and never includes raw option values in
loader diagnostics. Service code remains responsible for URI semantics and
service-specific validation.

## JSON Manifest

Each language package registers one JSON manifest without activating native
code. A manifest declares exactly one service or layer. A service can also
declare aliases for its canonical scheme.

An illustrative service manifest is:

```json
{
  "opendal_version": "0.55.0",
  "package_id": "opendal-service-s3",
  "package_version": "0.55.0",
  "component": {
    "kind": "service",
    "id": "s3",
    "aliases": []
  },
  "native_artifact_path": "lib/opendal_service_s3.so",
  "native_entry_kind": "c-json",
  "native_entry_symbol": "opendal_service_s3_bootstrap",
  "target_identity": "x86_64-unknown-linux-gnu"
}
```

The exact OpenDAL release defines the document fields and their meaning. The
loader rejects a manifest whose `opendal_version` differs from the runtime; it
does not negotiate manifest revisions. `package_version` remains package
metadata and does not establish native compatibility.

The registry validates document size, UTF-8, JSON structure, IDs, aliases, and
artifact paths before storing the manifest. It must not store credentials, URI
options, or service configuration in registration metadata.

## JSON Bootstrap

The bootstrap exists only to reject an incompatible native artifact before the
runtime enters the release-specific interface. The package exports a
package-unique function with a fixed C calling convention. The function reports
the required document length, then writes UTF-8 JSON into a host-provided
bounded buffer. It does not transfer memory ownership across the boundary.

The bootstrap document repeats these manifest fields:

```json
{
  "opendal_version": "0.55.0",
  "package_id": "opendal-service-s3",
  "package_version": "0.55.0",
  "component_kind": "service",
  "component_id": "s3",
  "target_identity": "x86_64-unknown-linux-gnu",
  "entry_symbol": "opendal_service_s3_entry"
}
```

The RFC must define the maximum document length, pointer validity, encoding,
symbol lifetime, and failure behavior. The bootstrap follows these rules:

- Every exported function uses an explicit C calling convention.
- No panic or foreign exception crosses the call.
- The loader rejects a reported document length above the defined limit before
  allocating or parsing it.
- The loader checks the OpenDAL version, package identity, component identity,
  target identity, and entry symbol before entering package code.
- The JSON manifest and bootstrap document must identify the same package and
  component.
- The bootstrap reports incompatibility; it does not attempt ABI adaptation.

The bootstrap does not expose `Operator`, `Layer`, trait objects, Rust strings,
Rust enums, futures, Tokio handles, or allocator ownership.

## OpenDAL Release Compatibility

The runtime, extension SDK, and official extension packages form one coordinated
OpenDAL release. The SDK build tool pins the exact dependencies, compiler,
target, Cargo profile, Rust flags, panic strategy, and linkage policy used by
that release. It generates both the package manifest and embedded bootstrap
document instead of asking extension authors to copy compatibility metadata into
source code.

Package-local dependencies such as S3 signing, XML parsing, `hdrs`, Foyer, or a
rate limiter can use different versions when their types and globals remain
inside the package. Any value exchanged through the internal interface follows
the exact SDK contract for that OpenDAL release.

The project never replaces a published runtime artifact with different bits
under the same OpenDAL version. A changed artifact requires a new release.

## Change Impact

<!-- markdownlint-disable MD013 -->

| Change                                      | OpenDAL version | Extension action               |
| ------------------------------------------- | --------------- | ------------------------------ |
| Install another compatible component        | Unchanged       | Register the new manifest      |
| Change package-private code or dependencies | Unchanged       | Republish only that package    |
| Change a public binding method only         | Per binding     | No native rebuild              |
| Change the manifest or bootstrap contract   | New release     | Rebuild every native extension |
| Change an SDK handle or factory layout      | New release     | Rebuild every native extension |
| Change ABI-visible OpenDAL code or features | New release     | Rebuild every native extension |
| Change compiler, panic, or linkage inputs   | New release     | Rebuild every native extension |

<!-- markdownlint-enable MD013 -->

## Exact-Release Internal Interface

After bootstrap validation, the runtime enters a release-specific factory
interface generated by the extension SDK. That interface can exchange
runtime-owned operator and layer handles or exact-release Rust adapters.

The following constraints apply:

- The interface is compatible with exactly one OpenDAL release.
- The [Rust ABI has no stability guarantees](https://doc.rust-lang.org/reference/items/external-blocks.html).
- The loader rejects a mismatch before registering factory pointers.
- Package-local code catches panics before returning across the boundary.
- Ownership remains on the creating side unless an SDK handle explicitly
  transfers it.
- Opaque runtime handles are preferred over exposing Rust types directly.

## Target Identity

An equal OpenDAL version does not make an artifact portable across targets. The
loader must also match:

- Operating system and architecture.
- Pointer width, endianness, and calling convention.
- Linux libc family and minimum version where applicable.
- macOS deployment target and architecture.
- Windows toolchain and runtime family.
- Required CPU target features.
- Language adapter variant when the runtime is not physically shared.

Cross-language reuse of one physical native artifact is supported only after
artifact-level tests prove that the adapters load the same runtime identity on
that target.

## Loader State Machine

```text
unregistered
    |
    | register validated JSON manifest
    v
registered
    |
    | first construction
    v
loading -> bootstrap validated -> factory installed -> active
   |               |                    |
   +---- error -----+--------------------+--> failed
```

The loader provides these properties:

1. One manifest registers one component and its service aliases atomically.
2. An identical registration is idempotent.
3. A different owner for the component or an alias rejects registration.
4. Registry locks are released before filesystem access, `dlopen`, addon
   loading, or package code.
5. Only the requested package activates.
6. One failed package does not poison unrelated registrations.
7. Concurrent first construction activates a package once and shares the
   result or failure deterministically.
8. The runtime retains a library lease before storing any callback.
9. The initial implementation never unloads an activated native extension.

The current core registry silently replaces an existing scheme. The extension
registry must reject conflicts instead.

## Package Manager Constraints

Package metadata provides an early compatibility diagnostic. The native loader
remains authoritative.

<!-- markdownlint-disable MD013 -->

| Ecosystem | Proposed constraint                                                              | Additional requirement                                    |
| --------- | -------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Python    | Extension distribution requires the exact OpenDAL runtime release                | Bootstrap version check; wheel target must match          |
| Ruby      | Extension gem requires the exact OpenDAL runtime release                         | Bootstrap version check; source/native policy is explicit |
| Node.js   | Extension uses an exact runtime peer dependency and target-specific dependencies | Reject a nested incompatible `ProcessRuntime`             |

<!-- markdownlint-enable MD013 -->

Dependency installation alone does not register an extension. Each binding uses
its defined discovery mechanism and preserves construction-time native
activation.

## Service Compatibility Rules

- A service factory receives the raw URI and explicit string options.
- The service package owns `Configurator::from_uri`, aliases, validation,
  credentials, redaction, and registry metadata.
- Query options are merged before explicit options by `OperatorUri`, but the
  service configurator can subsequently derive or replace fields from URI
  authority and path.
- Capabilities belong to the constructed operator. Registration metadata does
  not promise static capabilities.
- A package retains every native resource used by active operations.
- Construction errors identify the package and operation without copying
  secrets into loader diagnostics.

S3 must continue deriving bucket and root from its URI. WebDAV must continue
deriving an HTTPS endpoint from authority, including its current behavior of
overwriting an endpoint option. HDFS activation must be lazy and isolated from
`hdfs-native`.

## Layer Compatibility Rules

- A layer factory may complete asynchronously.
- A layer handle retains native state and its extension library lease.
- Applying a layer returns a new operator and does not mutate the source
  operator.
- Later applications are outer layers. The runtime does not sort or deduplicate
  layers.
- Both `apply_service` and `apply_context` behavior must be preserved.
- Reusing one handle preserves that layer's sharing identity.

Timeout and Retry have one cancellation-safe order. Applying Timeout first and
Retry second places Retry outside Timeout, so each retry attempt has its own
timeout. Applying Retry first and Timeout second places Timeout outside Retry;
the timeout can drop Retry's future before Retry restores operation-body state.
Bindings must preserve caller order and reject this known unsafe outer Timeout
composition when their metadata makes it visible.

## Error Contract

The runtime reports stable error categories:

```text
NotInstalled
ManifestInvalid
Conflict
Incompatible
UnsupportedTarget
NativeLoadFailed
BootstrapInvalid
FactoryFailed
LayerInitializationFailed
```

Language adapters map these categories to native exception classes while
retaining package ID, component ID, and construction operation. Diagnostics do
not include credentials or an unredacted configuration map.

## Security Constraints

Native extensions are trusted in-process code. Neither JSON validation nor an
exact OpenDAL version provides a sandbox, signature verification, provenance,
process isolation, or protection from a malicious extension.

The loader and official SDK must:

- Validate all untrusted lengths and identifiers before use.
- Catch panics at every exported native entry and runtime-invoked callback.
- Keep package symbols local where supported.
- Load only a path supplied by an installed, explicitly selected package.
- Pin libraries while any callback, vtable, task, or object may reference them.
- Keep credentials out of manifests and conflict errors.

Package signing and registry provenance remain package-release concerns. They
do not change the in-process trust model.

Official artifacts use unwind-capable panic handling at extension entries. The
SDK and release pipeline reject `panic=abort` for this interface.

## Unsupported Guarantees

The exact-release design does not guarantee:

- Loading an extension built for a different OpenDAL version.
- Native library unloading.
- Safe use of arbitrary extension code or secret redaction by third parties.
- Serialization of live caches, limiters, runtimes, JVMs, or connection pools.
- One physical extension binary across Python, Ruby, and Node.js.
- Static service capabilities before configuration.
- Runtime installation of a missing package.
- Backend correctness, authorization, durability, or resource limits beyond
  the existing OpenDAL threat model.

## Required Conformance

Before publishing a native extension interface, CI must verify:

- 1,000 manifest registrations without eager native activation.
- Single-component conflicts, alias conflicts, and idempotent registration.
- Version and target mismatch rejection before storing callbacks.
- Concurrent activation and process-lifetime library pinning.
- S3 and WebDAV URI and configuration semantics and secret-free errors.
- HDFS load isolation from the base, S3, WebDAV, and `hdfs-native`.
- Foyer asynchronous construction, cancellation, invalidation, and handle
  lifetime.
- Timeout service and executor behavior through a separately packaged layer.
- Timeout-inside-Retry acceptance and unsafe outer-Timeout rejection.
- Throttle shared limiter identity and argument validation.
- Language garbage collection, task cancellation, and callback races under
  sanitizers where available.
- Every supported OS, architecture, libc/deployment floor, and language adapter
  variant at the artifact level.
