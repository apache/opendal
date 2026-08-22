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

# Native Extension Symbol Isolation

Status: pre-RFC contract for the shared runtime candidate. OpenDAL does not
provide or guarantee this isolation today.

Each final native artifact statically contains much of its Rust dependency
graph. Loading two artifacts into one process can therefore create two copies
of `opendal-core`, Tokio, and other dependencies. Matching source versions do
not turn those copies into one runtime, and Rust visibility does not determine
which symbols a platform dynamic loader exports.

The shared runtime design requires two separate properties:

1. **Symbol isolation** prevents one native artifact from accidentally
   resolving private code or data from another artifact.
2. **Runtime state ownership** gives process-coupled facilities one explicit
   owner and exposes them to extensions through runtime handles or SDK
   functions.

Symbol isolation makes private dependency copies possible. It does not make
duplicated runtime state safe.

## Isolation Invariants

The runtime and every native extension must satisfy these invariants:

- The runtime exports only its versioned runtime bootstrap and explicitly
  documented platform integration symbols.
- A service or layer extension exports only its generated, package-unique
  bootstrap symbol and any initializer required by its language runtime.
- Every other package symbol has local or hidden visibility in the final
  artifact.
- An artifact does not rely on unresolved Rust symbols being supplied by the
  main binding, runtime, or another extension.
- A private dependency type, vtable, allocator-owned value, thread-local, or
  mutable global does not cross the extension boundary.
- The side that creates an allocation destroys it unless an SDK handle
  explicitly transfers ownership.
- The runtime pins an extension while any callback, vtable, task, or object can
  execute code from that extension.

An exact OpenDAL release match does not relax these rules. Release matching
selects the release-specific SDK contract; it does not authorize ambient
symbol resolution between artifacts.

## Dependency Ownership

<!-- markdownlint-disable MD013 -->

| Dependency kind | Examples | Required treatment |
| --------------- | -------- | ------------------ |
| Runtime-owned state | Operator registry, executor, timers, shared HTTP context, library leases | The shared runtime owns the state. Extensions use runtime handles or SDK functions. |
| Extension-private implementation | Signing, XML parsing, checksums, service-specific caches | The extension may contain a private copy when its symbols stay local and its values do not cross the boundary. |
| Process-global native facility | JVM, TLS libraries with global configuration, native client libraries | Packaging selects one compatible process-wide instance or isolates the facility out of process. Symbol hiding alone is insufficient. |
| Exact-release interface | Factory adapters, opaque handle operations, creator-side destructors | The extension SDK generates the interface for one coordinated OpenDAL release. |
| Language initializer | CPython, Ruby, or Node-API module entry | The artifact exports only the initializer required by that language in addition to any explicitly selected OpenDAL bootstrap. |

<!-- markdownlint-enable MD013 -->

This classification applies to every transitive dependency, not only Tokio.
The SDK must decide whether a dependency is runtime-owned, extension-private,
or process-global before its code enters an extension artifact.

## Export Surfaces

The intended native export surfaces are small and reviewable. For example:

```text
shared runtime:
    opendal_runtime_get_api_v1

S3 extension:
    opendal_service_s3_bootstrap_v1

Timeout extension:
    opendal_layer_timeout_bootstrap_v1
```

A language-native addon may instead expose the initializer required by its
language runtime. For example, Node-API controls the addon initializer name.
The initializer must return or register the same validated package metadata
and API rather than creating an unversioned second interface.

Public Rust items such as `Operator`, `Layer`, or Tokio functions are source
interfaces. They are not part of the dynamic export allowlist and do not form a
stable ABI.

## Platform Enforcement

The SDK build pipeline owns symbol visibility at the final link step:

- On ELF targets, it uses a version script or equivalent export list, localizes
  archive symbols, and inspects both dynamic symbols and `DT_NEEDED` entries.
  `RTLD_LOCAL` remains defense in depth; it is not the isolation mechanism.
- On macOS, it uses an exported-symbols list and inspects exports, imports, and
  install names in the final Mach-O artifact.
- On Windows, it generates an explicit `.def` file or equivalent export list
  and inspects the PE export/import tables and dependent DLLs.

The build must fail when the toolchain cannot enforce the selected export
surface. "Where supported" is not sufficient for an official extension target.

## Artifact Monitoring

Every release pipeline produces a normalized report for the runtime, each main
binding artifact, and each official extension artifact. The report records:

- Exported dynamic symbols.
- Imported dynamic symbols and their provider libraries.
- Direct native library dependencies.
- Target identity and the SDK inputs that selected the allowlists.

The pipeline compares the report with a checked-in or generated allowlist and
fails on any unreviewed addition. This catches transitive crates that introduce
`no_mangle` or C exports even when their Rust symbols remain hidden. Release
artifacts retain the report so later toolchain or dependency updates can be
compared with the accepted baseline.

Artifact inspection must cover the final repaired wheel, gem, npm package, or
shared runtime artifact. Auditing an intermediate Cargo output does not detect
changes introduced by packaging repair, symbol stripping, or native dependency
relocation.

## Co-loading Tests

Export inspection cannot prove runtime behavior. The conformance suite also
loads multiple independently compiled extensions into one process and verifies:

1. Each loader lookup resolves only the package-unique bootstrap requested by
   the manifest.
2. No deliberately duplicated private sentinel resolves across artifacts.
3. An operator created by a service extension composes with a layer extension
   through the exact-release SDK interface.
4. Real operations exercise runtime-coupled facilities such as timers,
   executors, credential file loading, HTTP, and cancellation.
5. All extension-owned objects are destroyed before a test unloads a library.

The [prototype](prototype/README.md) supplies an initial Linux export audit and
explores layout-sensitive `Operator` pointer exchange between independently
linked S3 and Timeout artifacts. It does not implement the proposed extension
SDK, its ownership contract, or a shared runtime. Therefore it does not satisfy
the third, fourth, or fifth conformance properties.

## Non-goals

Symbol isolation does not provide:

- A security sandbox for native extensions.
- A stable Rust ABI across OpenDAL releases.
- Compatibility between arbitrary process-global native libraries.
- Shared state merely because two artifacts contain the same dependency
  version.
- Safe library unloading without the lifetime rules in the compatibility
  contract.

Native extensions remain trusted in-process code under the project
[security threat model](../../../SECURITY-THREAT-MODEL.md). The isolation policy
prevents accidental linking and interposition; it does not constrain malicious
native code.
