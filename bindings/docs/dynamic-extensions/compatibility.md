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

Status: pre-RFC contract for the shared runtime candidate. OpenDAL does not
provide or guarantee this interface today.

The design uses two compatibility axes. A language binding declares the runtime
protocol level that it requires. A native extension must target exactly the
OpenDAL version used by the runtime. The target identity remains a separate
platform check, not another versioning scheme.

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

## Runtime Protocol Compatibility

The binding-to-runtime boundary uses a common, language-neutral protocol. The
protocol is distinct from the runtime package version and from the
exact-release native extension interface.

Each main binding package declares one `required_runtime_protocol`. The runtime
package reports an inclusive supported range:

- `minimum_runtime_protocol` identifies the oldest protocol contract that the
  runtime still supports.
- `runtime_protocol` identifies the newest protocol capability that the runtime
  provides.

A binding is compatible when:

```text
minimum_runtime_protocol
    <= required_runtime_protocol
    <= runtime_protocol
```

For example, a binding that requires protocol 20 can use a runtime that supports
protocols 18 through 23. Package metadata provides an early diagnostic, but the
loaded runtime reports the authoritative values before returning an API table.

The runtime reports the range and acquires the requested API through one
bootstrap function equivalent to:

```c
typedef struct {
  uint32_t struct_size;
  uint32_t minimum_runtime_protocol;
  uint32_t runtime_protocol;
} opendal_runtime_protocol_info_v1;

typedef struct opendal_runtime_api opendal_runtime_api;

int32_t opendal_runtime_get_api_v1(
    uint32_t required_runtime_protocol,
    opendal_runtime_protocol_info_v1 *protocol_info,
    const opendal_runtime_api **api
);
```

The runtime fills `protocol_info` even when the requested level is incompatible
and returns a null `api` in that case. On success, it returns an API table that
conforms to `required_runtime_protocol`. The binding performs this call before
converting configuration or acquiring a runtime-owned handle. The binding uses
no capability introduced after its required level, and the runtime does not
send that interaction a value, callback, or handle kind introduced after that
level. A newer runtime therefore preserves the complete behavior of every
protocol at or above `minimum_runtime_protocol`.

The binding's requested level is also the interaction ceiling for an extension.
The loader rejects an extension whose `required_runtime_protocol` is greater
than the binding's requested level, even when the runtime itself provides that
newer level. An extension can require an older level because the exact-release
runtime and extension enter the interaction at the binding's requested level.

An incompatible change to this bootstrap function uses a new exported symbol,
such as `opendal_runtime_get_api_v2`; it does not add a separately negotiated
ABI-major field to the normal protocol check.

The protocol should expose the smallest practical surface. The exported
bootstrap function acquires a size-tagged API table; a registration or
construction function in that table performs the main operation. Even this
two-function interaction has a protocol: its function signatures, table layout,
`ConfigValue` representation, status codes, handle ownership, and lifetime
rules are the compatibility contract.

Adding an ordinary service configuration field does not raise the runtime
protocol level because each service package owns its schema. Adding a new
shared `ConfigValue` variant, handle kind, factory capability, or lifetime rule
does raise the protocol level. The binding that first uses that capability then
raises `required_runtime_protocol`. The protocol does not contain language- or
service-specific identifiers such as `opendal.python.s3`.

OpenDAL distributes the protocol implementation in the shared runtime package
and publishes that package with the language bindings. Ecosystem-specific
delivery wrappers must resolve the same runtime identity when multiple bindings
load in one process.

## Configuration Value Contract

Factories receive language-neutral configuration values. The runtime protocol
defines this closed grammar at the binding's requested protocol level:

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

A package owns its configuration format, fields, defaults, validation,
credentials, redaction behavior, and any schema version. `ConfigValue` defines
only the shared transport vocabulary; it does not impose one schema model on
all bindings or packages.

Each package therefore decides:

- Whether a missing field selects a default or produces an error.
- Whether `Null` differs from a missing field.
- Whether to reject, ignore, or preserve unknown fields.
- How to represent and evolve package-specific configuration versions.
- Which credential and token fields require redaction.

The runtime enforces the transport limits above and never logs raw configuration
values. Package validation errors must not render secret values.

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
  "required_runtime_protocol": 20,
  "package_id": "opendal-service-s3",
  "package_version": "0.55.0",
  "component": {
    "kind": "service",
    "id": "s3",
    "aliases": []
  },
  "native_artifact_path": "lib/opendal_service_s3.so",
  "native_entry_symbol": "opendal_service_s3_bootstrap_v1",
  "target_identity": "x86_64-unknown-linux-gnu"
}
```

The runtime protocol defines the common document fields and their meaning. The
loader first verifies that `required_runtime_protocol` does not exceed the
binding's already validated requested level, then rejects a native extension
manifest whose `opendal_version` differs from the runtime. It does not negotiate
native extension compatibility from the protocol level. `package_version`
remains package metadata and does not establish native compatibility.

The registry validates document size, UTF-8, JSON structure, IDs, aliases, and
artifact paths before storing the manifest. It must not store credentials, URI
options, or service configuration in registration metadata.

## Bootstrap Encoding Alternatives

The installed discovery manifest remains JSON so the registry can inspect it
without loading native code. The native bootstrap repeats its compatibility
metadata after the library loads. The bootstrap encoding is still a design
choice: OpenDAL should compare a bounded JSON document with an exact-release
C-layout descriptor instead of discarding either option before prototyping.

For directly loaded native libraries, both encodings use a package-unique
exported function with a fixed C calling convention. They exist only to reject
an incompatible native artifact before the runtime enters the release-specific
interface.

Both encodings sit behind the same stable bootstrap envelope and function
signature:

```c
enum {
  OPENDAL_BOOTSTRAP_JSON = 1,
  OPENDAL_BOOTSTRAP_C_LAYOUT_V1 = 2,
};

typedef struct {
  uint32_t struct_size;
  uint32_t payload_encoding;
  const unsigned char *payload;
  size_t payload_len;
} opendal_bootstrap_result_v1;

typedef int32_t (*opendal_bootstrap_fn_v1)(
    opendal_bootstrap_result_v1 *result
);
```

The host initializes `struct_size` to `sizeof(opendal_bootstrap_result_v1)` and
zeroes the other fields before calling the package-unique symbol. Every version
1 bootstrap symbol ends in `_bootstrap_v1` and uses this signature. A future
incompatible envelope uses a new symbol suffix, so the loader never guesses a
function signature from unvalidated package metadata.

The function returns one of these status codes:

- `0`: Success. The result contains one recognized, bounded payload.
- `1`: Invalid argument, including a null result pointer.
- `2`: Unsupported bootstrap envelope, including an undersized `struct_size`.
- `3`: The package could not provide bootstrap metadata.

The loader treats every non-zero or unknown status as `BootstrapInvalid` and
does not read the payload fields. On success, the payload is immutable
package-owned memory that remains valid while the library is loaded. The loader
rejects a null payload, a zero or excessive length, or an unknown encoding
before decoding it.

A Node-API addon initializer cannot use a package-unique C initializer. It
returns the same status, payload-encoding discriminant, and bounded payload
through Node-API values. The environment adapter applies the same validation
before passing metadata to the process runtime.

Prototype runtimes can accept both payload encodings through this envelope for
comparison. A published OpenDAL release selects one encoding for its supported
SDK and official packages. The common function signature remains the same, so a
stale manifest cannot make the loader call the bootstrap with the wrong ABI.

<!-- markdownlint-disable MD013 -->

| Property           | JSON payload                                      | C-layout payload                                 |
| ------------------ | ------------------------------------------------- | ------------------------------------------------ |
| Bootstrap call     | Common version 1 envelope                         | Common version 1 envelope                        |
| Installed manifest | JSON                                              | JSON                                             |
| Native metadata    | UTF-8 names and values                            | Size-tagged structure with bounded byte slices   |
| Payload ownership  | Immutable package memory                          | Immutable package memory                         |
| Validation surface | Length, UTF-8, JSON, fields                       | Pointer, length, size, alignment, and fields     |
| Human inspection   | Direct                                            | Requires a decoding tool                         |
| Node-API transport | String or byte buffer                             | Wrapper around the native structure              |
| Evolution          | Schema follows the exact OpenDAL release          | New layout needs a new payload discriminant      |
| Main risk          | Parser complexity and non-canonical serialization | Unsafe pointer, length, and alignment validation |

<!-- markdownlint-enable MD013 -->

### JSON Payload

The JSON payload is a bounded UTF-8 document. An illustrative document is:

```json
{
  "opendal_version": "0.55.0",
  "required_runtime_protocol": 20,
  "package_id": "opendal-service-s3",
  "package_version": "0.55.0",
  "component_kind": "service",
  "component_id": "s3",
  "target_identity": "x86_64-unknown-linux-gnu",
  "entry_symbol": "opendal_service_s3_entry"
}
```

The loader validates the envelope length before parsing. The RFC must define
canonical encoding where bytewise comparison matters; field comparison must not
depend on JSON object order.

### C-Layout Payload

The C-layout payload points to this illustrative version 1 metadata structure:

```c
typedef struct {
  const unsigned char *data;
  size_t len;
} opendal_bytes;

typedef struct {
  uint32_t struct_size;
  uint32_t required_runtime_protocol;
  opendal_bytes opendal_version;
  opendal_bytes package_id;
  opendal_bytes package_version;
  opendal_bytes component_kind;
  opendal_bytes component_id;
  opendal_bytes target_identity;
  opendal_bytes entry_symbol;
} opendal_c_metadata_v1;
```

The package contract requires every returned pointer to reference immutable
package-owned memory for the declared lifetime. The loader can reject null or
misaligned pointers and invalid structural bounds, but it cannot prove that an
arbitrary in-process pointer is mapped safely. After the checkable pointer and
alignment checks, the loader requires `payload_len` to cover the `struct_size`
field. It then requires `struct_size` to contain every version 1 field and not
exceed `payload_len` before reading any byte slice. Version 1 defines the
complete layout needed to read the OpenDAL version; changing that layout
requires a new payload encoding discriminant. The RFC must define maximum slice
lengths, encoding, and structure lifetime.

### Shared Bootstrap Rules

Whichever encoding the prototype selects, the bootstrap follows these rules:

- Every exported function uses an explicit C calling convention.
- No panic or foreign exception crosses the call.
- Every package-unique bootstrap symbol uses the common envelope signature.
- The loader checks the required runtime protocol, OpenDAL version, package
  identity, component identity, target identity, and entry symbol before
  invoking the release-specific entry point or factory.
- The JSON manifest and native bootstrap must identify the same package and
  component.
- The loader reports incompatible metadata; it does not attempt ABI adaptation.

The bootstrap does not expose `Operator`, `Layer`, trait objects, Rust strings,
Rust enums, futures, Tokio handles, or allocator ownership.

JSON is the leading candidate because the discovery manifest and native
metadata can share parsing and scalar-value conventions, and the Node-API
adapter can transport it without native structure access. The C-layout payload
remains a candidate if the prototype demonstrates simpler or safer activation
on the supported native targets. The selection must follow cross-platform
loader tests, not document preference alone.

## OpenDAL Release Compatibility

The runtime, extension SDK, and official extension packages form one coordinated
OpenDAL release. The SDK build tool pins the exact dependencies, compiler,
target, Cargo profile, Rust flags, panic strategy, and linkage policy used by
that release. It generates both the package manifest and embedded bootstrap
document instead of asking extension authors to copy compatibility metadata into
source code.

Package-local dependencies such as S3 signing, XML parsing, `hdrs`, Foyer, or a
rate limiter can use different versions when their types and globals remain
inside the package and their dynamic symbols satisfy the [native symbol
isolation contract](symbol-isolation.md). Any value exchanged through the
internal interface follows the exact SDK contract for that OpenDAL release.

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
- All cross-artifact calls enter through validated bootstrap or SDK functions;
  exact-release matching does not permit ambient Rust symbol resolution.

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

| Ecosystem | Main binding constraint                  | Native extension constraint             | Additional requirement                                    |
| --------- | ---------------------------------------- | --------------------------------------- | --------------------------------------------------------- |
| Python    | Required runtime protocol                | Exact OpenDAL runtime release           | Bootstrap version check; wheel target must match          |
| Ruby      | Required runtime protocol                | Exact OpenDAL runtime release           | Bootstrap version check; source/native policy is explicit |
| Node.js   | Required runtime protocol                | Exact runtime and target dependencies   | Reject a nested incompatible `ProcessRuntime`             |

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
- Enforce the export allowlist defined by the [native symbol isolation
  contract](symbol-isolation.md).
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
