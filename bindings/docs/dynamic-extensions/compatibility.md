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

Status: pre-RFC contract for the exact-build runtime candidate. OpenDAL does not
provide or guarantee this ABI today.

This document distinguishes public language compatibility, package discovery,
the safe loader bootstrap, and the build-specific native interface. A version
match in one category does not imply a match in another category.

The key constraint is deliberate:

> The candidate design provides a stable language interface and a stable
> loader handshake, but it does not provide a stable Rust extension ABI.

Native extensions must match an exact runtime build ID. An extension rebuild is
required whenever that build ID changes.

## Compatibility Identities

| Identity                  | Purpose                                                           | Stability rule                                                                |
| ------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Binding interface version | Public Python, Ruby, or Node.js behavior                          | Governed independently by each binding's compatibility policy                 |
| Manifest revision         | Text discovery and registration fields                            | Major for breaking syntax/meaning; minor additions must be ignorable          |
| Extension SDK revision    | Source interface used to build native extensions                  | Semantic versioning can describe source changes, but not native compatibility |
| Bootstrap ABI version     | C-layout descriptor read before native activation                 | Stable major; minor revisions append bounded optional fields                  |
| Runtime build ID          | Exact internal native interface and shared Rust types             | Equality required; no compatibility range                                     |
| Target identity           | OS, architecture, ABI, libc/deployment floor, and target features | Must be compatible with the running process and runtime artifact              |

Equal OpenDAL versions, binding versions, Rust compiler versions, or SDK
revisions are individually insufficient to establish exact-runtime binary
compatibility.

## Public Language Interface

The public binding interface remains independent in each ecosystem:

- Python versions `Operator`, `AsyncOperator`, exceptions, typing, and package
  discovery according to the Python binding policy.
- Ruby versions `OpenDal::Operator`, layers, errors, and blocking behavior
  according to the Ruby binding policy.
- Node.js versions JavaScript classes, promises, synchronous variants, streams,
  and TypeScript declarations according to the Node.js binding policy.

A public binding release may preserve all language-level behavior while
changing its runtime build ID and requiring every native extension to rebuild.
Conversely, an extension package can change a typed configuration helper
without changing the runtime ABI.

Python `abi3` describes compatibility between a Python extension and CPython
versions. A [Python capsule](https://docs.python.org/3/c-api/capsule.html) can
carry an opaque bootstrap pointer through the stable CPython ABI. Neither fact
stabilizes Rust values behind that pointer.

[Node-API](https://nodejs.org/api/n-api.html) describes compatibility between
the base addon and Node.js versions. Its own documentation explicitly excludes
external libraries from that guarantee. It does not make two napi-rs
`External<T>` values from different addons interchangeable.

The Ruby adapter remains subject to the Ruby and Magnus compatibility policy.
A language-neutral service/layer extension should not link Ruby and therefore
does not acquire a Ruby ABI dependency of its own.

## Configuration Value Contract

Factories receive language-neutral configuration values. Version 1 has this
closed grammar:

```text
ConfigValueV1 =
    Null
  | Bool(bool)
  | I64(i64)
  | U64(u64)
  | F64(finite IEEE-754 binary64)
  | Utf8(string)
  | Bytes(byte sequence)
  | List(sequence<ConfigValueV1>)
  | Map(map<Utf8, ConfigValueV1>)
  | DurationNs(u64)
```

`DurationNs` is the only duration representation at the shared factory seam.
It covers zero through `u64::MAX` nanoseconds. Binding-specific interfaces can
accept seconds, milliseconds, or native duration objects, but their adapters
must define nanosecond rounding and reject negative, non-finite, or overflowing
values before producing `DurationNs`. Numeric conversion never silently
truncates, saturates, or wraps.

The grammar has its own major version. Adding a value variant, changing a
numeric interpretation, or changing container semantics creates a new major and
a new exact runtime build ID. A new runtime can implement more than one grammar
major explicitly, but it never reinterprets an old tag.

The decoder enforces a maximum nesting depth of 32, a maximum UTF-8 map key of
4 KiB, a maximum individual string or byte value of 16 MiB, a maximum of 65,536
entries in one list or map, and a maximum encoded request size of 64 MiB. It
rejects invalid UTF-8, duplicate map keys, non-finite floats, unknown value tags,
and values outside the declared numeric ranges before calling package code.

A package versions its configuration schema independently from this value
grammar. Schema behavior follows these rules:

- A missing field selects its declared default or produces a missing-field
  error.
- `Null` is valid only when the field is explicitly nullable. Optional and
  nullable are separate properties.
- A package rejects every field absent from its schema. Version 1 has no
  implicit or client-declared "ignorable unknown" convention.
- A package rejects an unsupported schema revision. It can declare an explicit
  set or range of revisions only when it implements each revision's defaults
  and field meanings.
- A schema marks credential and token fields as secret. The runtime never logs
  raw configuration values, and package errors identify a secret field without
  rendering its value.

`UriRequest` remains separate: it contains one UTF-8 URI and a map of UTF-8
option names to UTF-8 option values, matching current iterator construction.
The runtime applies the same size limits and never includes raw option values in
loader diagnostics. Service code remains responsible for URI semantics and
service-specific validation.

## Manifest Compatibility

Language packages register a text manifest without activating native code. A
manifest contains at least:

```text
manifest_revision
package_id
package_version
provided_services[]
provided_layers[]
native_artifact_path
native_entry_kind
native_entry_symbol
runtime_build_id
target_identity
manifest_digest
```

The registry must validate sizes, encoding, IDs, duplicate names, and paths
before storing the manifest. It compares the declared runtime build ID and
target with the running artifact and rejects a mismatch without loading native
code. It must not store credentials, URI options, or service configuration in
registration metadata.

Manifest evolution follows these rules:

- A new major revision may add a required field or change field meaning.
- A new minor revision may add optional fields only. Unknown fields in a newer
  minor are optional by definition and an older reader ignores them.
- Readers reject an unknown major revision.
- Writers do not omit a field required by their declared major revision.
- A package's embedded native descriptor must repeat the package identity,
  runtime build ID, target identity, and manifest digest. Activation fails if
  the text and native declarations differ.

## Bootstrap ABI

The bootstrap exists only to inspect and reject an extension safely before
entering the exact-build interface. It must not expose `Operator`, `Layer`,
trait objects, Rust strings, Rust enums, futures, Tokio handles, or allocator
ownership.

An illustrative descriptor is:

```c
typedef struct {
  const unsigned char *data;
  size_t len;
} opendal_bytes_v1;

typedef struct {
  uint64_t magic;
  uint32_t struct_size;
  uint16_t bootstrap_major;
  uint16_t bootstrap_minor;
  opendal_bytes_v1 package_id;
  opendal_bytes_v1 manifest_digest;
  opendal_bytes_v1 runtime_build_id;
  opendal_bytes_v1 target_identity;
  void (*exact_build_entry)(void);
} opendal_extension_header_v1;
```

The actual RFC must specify maximum lengths, pointer validity, descriptor
lifetime, encoding, alignment, and failure behavior. Descriptor memory should
be immutable and valid while the native library remains loaded.

The bootstrap ABI follows these rules:

- Every exported function uses an explicit C calling convention.
- No panic or foreign exception crosses the call.
- Every structure begins with a size and version or is referenced by a
  versioned descriptor.
- Minor versions append fields; they never reorder or reinterpret existing
  fields.
- The loader checks `magic`, minimum size, major version, bounded byte slices,
  target identity, manifest digest, and runtime build ID in that order.
- The loader does not call `exact_build_entry` after any failed check.
- The bootstrap reports incompatibility; it does not attempt ABI adaptation.

This stable bootstrap is intentionally too small to construct an operator. A
stable construction interface would be Design D from
[the alternatives](alternatives.md), not an incremental relaxation of the
exact-build rule.

## Runtime Build ID

The coordinated runtime build issues an opaque, collision-resistant build ID.
Extension authors must not type or copy that identity into source code. The
published SDK build tool validates the extension build inputs against the
runtime build manifest and generates both the embedded descriptor and the text
manifest. It fails the build if a required input cannot be verified.

The build manifest behind the ID must cover all ABI-relevant inputs,
including:

- Extension runtime and SDK layout revisions.
- Exact OpenDAL core source identity.
- Exact versions and features of dependencies whose types or globals cross the
  seam.
- `rustc -vV`, target specification, target features, and ABI-relevant codegen
  options.
- Panic strategy, allocator/ownership policy, and symbol/linkage model.
- The supported factory and handle layouts.

Package-local dependencies such as S3 signing, XML parsing, `hdrs`, Foyer, or a
rate limiter do not need identical versions when their types and globals stay
inside the package. If one of their types crosses the seam, it becomes an
ABI-visible dependency and must enter the runtime build identity.

The supported SDK is a coordinated build tool, not only a Rust crate. It pins
the exact SDK dependencies and lock state, checks the compiler and target,
normalizes the permitted Cargo profile and Rust flags, verifies the panic and
linkage policy, and records the actual inputs in a machine-readable build
report. Release CI compares that report with the generated descriptor before it
publishes an artifact.

Build-ID equality is a necessary accidental-mismatch check, not attestation and
not proof that arbitrary native code is compatible or well behaved. Official
packages use the coordinated release pipeline. A supported third-party source
package must build through the published SDK toolchain and pass the same
conformance suite. An arbitrary binary that merely claims or forges an issued
ID is trusted, unsupported in-process code; the loader cannot make it safe.

The project must never replace a published runtime artifact with different bits
or a different build ID under the same package version. A rebuild receives a
new immutable runtime release.

## Change Impact

| Change                                                        | Binding interface version                  | Runtime build ID | Extension action                                   |
| ------------------------------------------------------------- | ------------------------------------------ | ---------------- | -------------------------------------------------- |
| Install another compatible service/layer                      | Unchanged                                  | Unchanged        | Register the new manifest                          |
| Add an optional service config field                          | Service package policy                     | Unchanged        | Republish only that package if native code changes |
| Change a public language method incompatibly                  | Binding policy requires a breaking release | Maybe            | Rebuild only if native inputs change               |
| Add an optional manifest field                                | Manifest minor                             | Unchanged        | Old readers ignore it                              |
| Append a bootstrap descriptor field                           | Bootstrap minor                            | Unchanged        | Old loaders use the recorded structure size        |
| Change an exact SDK handle or factory layout                  | Unchanged or additive language release     | New ID           | Rebuild every native extension                     |
| Change ABI-visible OpenDAL core code or features              | Usually unchanged at language level        | New ID           | Rebuild every native extension                     |
| Change `rustc`, target ABI, panic strategy, or linkage inputs | Unchanged                                  | New ID           | Rebuild every native extension                     |
| Change only a package-private dependency                      | Unchanged                                  | Unchanged        | Rebuild only that package                          |

This table describes compatibility impact, not release numbering. Each binding
and package still follows its own published version policy.

## Exact-Build Internal Interface

After the bootstrap succeeds, the runtime may use a build-specific factory
interface generated by the extension SDK. That interface can exchange
runtime-owned operator and layer handles or exact-build Rust adapters.

The following constraints apply:

- The interface is compatible only with one runtime build ID.
- The [Rust ABI has no stability guarantees](https://doc.rust-lang.org/reference/items/external-blocks.html).
- Extensions rebuild for every new runtime build ID, including patch releases when
  the ABI-relevant build changes.
- Equal layouts observed in two builds do not create a supported compatibility
  range.
- The loader rejects a mismatch before registering factory pointers.
- Package-local code catches panics before returning across the seam.
- Ownership remains on the creating side unless an exact SDK handle explicitly
  transfers it.

The first implementation should prefer opaque runtime handles and generated
adapters even inside the exact-build interface. This reduces the number of Rust
types that can accidentally become ABI-visible and makes a future stable ABI
easier to evaluate.

## Target Identity

A runtime build ID does not make an artifact portable across targets. The
loader must also match:

- Operating system and architecture.
- Pointer width, endianness, and calling convention.
- Linux libc family and minimum version where applicable.
- macOS deployment target and architecture.
- Windows toolchain/runtime family.
- Required CPU target features.
- Language adapter variant when the runtime is not physically shared.

A runtime release may issue different build IDs for Python, Ruby, and Node.js
even when all three use the same extension SDK source. Cross-language reuse of
one physical native artifact is supported only after artifact-level tests prove
that those adapters load the same runtime identity on that target.

## Loader State Machine

```text
unregistered
    |
    | register validated text manifest
    v
registered
    |
    | first construction
    v
loading -> bootstrap validated -> factories installed -> active
   |               |                      |
   +---- error -----+----------------------+--> failed
```

The loader must provide these properties:

1. Registration of all names in one manifest is atomic.
2. An identical registration is idempotent.
3. A different owner for an existing name rejects the full registration.
4. Registry locks are released before filesystem access, `dlopen`, addon
   loading, or package code.
5. Only the requested package activates.
6. One failed package does not poison unrelated registrations.
7. Concurrent first construction activates a package once and shares the
   result or failure deterministically.
8. The runtime retains a library lease before storing any callback.
9. The initial implementation never unloads an activated native extension.

The current core registry silently replaces an existing scheme. The extension
registry must not inherit that behavior.

## Package Manager Constraints

Package metadata provides an early compatibility diagnostic. The native loader
remains authoritative because package managers do not understand the embedded
runtime build ID.

| Ecosystem | Proposed constraint                                                                   | Additional requirement                                             |
| --------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Python    | Extension distribution requires an exact `opendal-runtime` release                    | Embedded build ID check; wheel target must match                   |
| Ruby      | Extension gem requires an exact `opendal-runtime` release                             | Embedded build ID check; source/native gem policy remains explicit |
| Node.js   | Extension uses an exact runtime peer dependency and target-specific optional packages | Embedded build ID check; reject a nested wrong `ProcessRuntime`    |

Extension package versions may evolve independently from their target runtime.
Each published version still declares exactly which runtime release it targets.
A service fix that does not change the runtime can publish a new service package
against the same runtime build ID.

The compatibility aggregator named `opendal` installs the runtime and the
service/layer set provided by the current monolithic release. Minimal users
install the runtime distribution and selected extensions directly. Release
publication order is runtime, extensions, then aggregator.

Dependency installation alone does not register an extension. Each aggregator
must guarantee registration using its binding's defined mechanism: mandatory
named entry-point resolution in Python, a generated registration-stub index in
Ruby, and explicit stub imports during Node.js module initialization. The
aggregator must register metadata only and preserve construction-time native
activation.

## Service Compatibility Rules

- A service factory receives the raw URI and explicit string options.
- The service package owns `Configurator::from_uri`, aliases, validation,
  credentials, and redaction.
- Query options are merged before explicit options by `OperatorUri`, but the
  service configurator may subsequently derive or replace fields from URI
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
- A layer descriptor carries its canonical ID and declared composition safety
  constraints. An operator handle retains the ordered IDs applied through the
  supported binding interface.
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
Official bindings must preserve caller order and reject this known unsafe outer
Timeout composition when the operator's layer metadata makes it visible. They
must document it as unsupported when an opaque third-party layer prevents that
check. The runtime must never silently reorder the layers.

Foyer live cache state and Throttle token history are not automatically
serializable. A package can define a declarative reconstruction policy, but it
must distinguish a reconstructed resource from the original live state.

## Lifetime and Concurrency

- Operator and immutable layer handles may be shared according to the native
  OpenDAL contract.
- Mutable readers, writers, listers, deleters, copiers, and task handles must
  document and enforce their allowed concurrency.
- A language wrapper can be collected while a derived operator remains active;
  the runtime handle must keep native state alive.
- Cancellation must drop or abort the represented operation according to its
  documented semantics. It must not detach work merely to simplify FFI.
- Destruction runs through the creator's function or exact SDK adapter.
- No callback, destructor, or wake operation may target an unloaded library.
- Reuse after `fork` is unsupported for Tokio, JVM, connection-pool, and cache
  state unless a package explicitly documents and tests it.

Node.js adds one constraint: JavaScript and Node-API values belong to one
`napi_env`. A loaded runtime native module owns a process-scoped
`ProcessRuntime`, including registries, native handle identity, activation
state, and library leases. Each environment owns a separate
`EnvironmentAdapter` and cleanup hooks. The adapter must not move
environment-bound values between the main thread, Workers, or Electron
contexts.

## Error Contract

The shared runtime must expose stable error categories for:

- Manifest invalid or conflicting registration.
- Package not installed or not registered.
- Native artifact missing or unsupported on this target.
- Bootstrap symbol or descriptor invalid.
- Runtime build ID, target, or manifest mismatch.
- Native dependency load failure.
- Service/layer configuration invalid.
- Asynchronous layer initialization failure or cancellation.
- Package panic contained at the extension seam.
- Normal OpenDAL operation errors.

Bindings map those categories into language-native exception classes while
retaining package ID, scheme/layer ID, and construction operation. Diagnostics
must not include credentials or an unredacted configuration map.

## Security Constraints

Native extensions are trusted in-process code. Neither the bootstrap nor an
exact build ID provides a sandbox, signature verification, provenance, process
isolation, or protection from a malicious extension.

Compatibility validation still protects the supported binding contract from
accidental mismatches. The loader and official SDK must:

- Validate all untrusted lengths and identifiers before use.
- Catch panics at every exported native entry and runtime-invoked callback.
- Keep package symbols local where supported.
- Load only a path supplied by an installed, explicitly selected package.
- Pin libraries while any callback, vtable, task, or object may reference them.
- Keep credentials out of manifests, fingerprints, and conflict errors.

Package signing and registry provenance remain package-release concerns. They
do not change the in-process trust model.

Official runtime and extension artifacts must use unwind-capable panic handling
at extension entries. An artifact built with `panic=abort` cannot contain a
panic at the seam and still satisfy containment; the SDK and release pipeline
therefore reject that panic strategy for this interface. The build ID records
the strategy so a mismatched artifact also fails compatibility validation.

## Unsupported Guarantees

The exact-build design does not guarantee:

- Loading an extension built for a different runtime build ID.
- Compatibility based only on equal OpenDAL or binding semantic versions.
- Native library unloading.
- Safe use of arbitrary extension code or secret redaction by third parties.
- Serialization of live caches, limiters, runtimes, JVMs, or connection pools.
- One physical extension binary across Python, Ruby, and Node.js.
- Static service capabilities before configuration.
- Runtime installation of a missing package. Discovery only observes packages
  already installed by the application's package manager.
- Backend correctness, authorization, durability, or resource limits beyond
  the existing OpenDAL threat model.

## Required Conformance

Before publishing a native extension interface, CI must verify:

- 1,000 manifest registrations without eager native activation.
- Atomic multi-name conflicts and idempotent duplicate registration.
- Mismatch rejection before storing callbacks.
- Concurrent activation and process-lifetime library pinning.
- S3 and WebDAV URI/configuration semantics and secret-free errors.
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
