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

# Dynamic Extension Design Alternatives

Status: pre-RFC design comparison. No alternative has been accepted or
implemented.

This document compares four ways to deliver OpenDAL services and layers to
language bindings. Designs B and D remain useful comparisons even though their
maintenance or semantic costs make them unsuitable for the current proposal.

## Decision Guide

```text
Must packages compose after installation?
  no  -> Design A: feature-selected monolithic binding
  yes -> Must arbitrary native layers preserve core semantics?
           no  -> Design B: language-owned operator adapters
           yes -> Must old binaries survive OpenDAL upgrades?
                    no  -> Design C: exact-release shared runtime
                    yes -> Design D: stable C operation interface
```

OpenDAL should prototype Design C. Design A remains useful for custom builds.
Design B does not preserve arbitrary native layers and repeats an operation
adapter in every language. Design D preserves cross-version binaries only by
requiring OpenDAL to maintain a second operation model.

## Runtime Distribution Alternatives

Design C still needs a distribution choice. The protocol and the package are
separate concepts: independently built components always need a protocol, even
if that protocol consists of only a small API lookup and construction surface.
The runtime package determines who ships and owns its implementation.

<!-- markdownlint-disable MD013 -->

| Model | Distribution | Strengths | Limitations |
| ----- | ------------ | --------- | ----------- |
| Binding-embedded runtime | Every main binding package carries a private runtime implementation | Simplest installation; no separate runtime dependency; binding maintainers control loading | Duplicates native artifacts; can create multiple runtime identities in one process; lets bindings drift in behavior and build settings |
| Host-provided runtime | The main binding loads a runtime and exposes its API to extension packages | Avoids a separately visible runtime package; extensions reuse the host instance | Couples discovery to each host language; makes extensions depend on host loading order and adapter-specific mechanisms; does not naturally coordinate multiple languages |
| Shared runtime package | Main bindings and extension packages depend on one separately versioned runtime package | Gives all languages one runtime identity and implementation; centralizes compatibility checks, fixes, and native resources; fits coordinated OpenDAL releases | Requires cross-ecosystem native artifact resolution; needs package-manager rules against incompatible duplicate runtimes; adds an explicit dependency |

<!-- markdownlint-enable MD013 -->

OpenDAL selects the shared runtime package for the prototype. The project
will publish the runtime and language bindings as a coordinated release family,
which strengthens consistency and simplifies release management.
Although each binding could carry the same implementation, a separate package
provides one place to version, test, diagnose, and update that implementation.
Target-specific wheels, gems, and npm packages may act as ecosystem delivery
wrappers, but they must represent the same runtime release and protocol.

## Summary

<!-- markdownlint-disable MD013 -->

| Property                       | A. Static build   | B. Language adapters | C. Exact-release runtime | D. Stable C interface      |
| ------------------------------ | ----------------- | -------------------- | ------------------------ | -------------------------- |
| Independently install services | No                | Yes                  | Yes                      | Yes                        |
| Independently install layers   | No                | Language decorators  | Yes                      | Yes                        |
| Full native layer semantics    | Yes               | No                   | Yes                      | Only represented semantics |
| One core/runtime graph         | Yes               | Usually no           | Yes                      | Usually no                 |
| Third-party source extensions  | Rebuild host      | Yes                  | Yes, exact release       | Yes                        |
| Cross-version native binaries  | Not applicable    | Language-dependent   | No                       | Yes, within ABI rules      |
| Cross-language native package  | No                | No                   | Possible, not promised   | Yes, by design             |
| Native dependency isolation    | No                | Yes                  | Yes                      | Yes                        |
| Implementation cost            | Lowest            | Moderate             | High                     | Highest                    |
| Primary risk                   | Artifact variants | Semantic loss        | Packaging and linking    | ABI breadth and safety     |

<!-- markdownlint-enable MD013 -->

## Design A: Feature-Selected Monolithic Binding

### Static Build Seam

Cargo features select services and layers when the language extension is
compiled. The resulting wheel, gem, or npm native package contains one OpenDAL
core and all selected implementations.

```text
language caller -> one native binding -> compiled services and layers
```

### Static Build Strengths

- Preserves all native behavior without a new runtime interface.
- Uses ordinary Rust ownership and one Tokio/core graph.
- Has the smallest implementation and verification cost.
- Works well for downstream users who build one controlled deployment image.

### Static Build Limitations

- A service cannot be added after the binding is built.
- Two feature variants normally provide the same import or module name and
  cannot be installed together.
- One uncommon native dependency can constrain the whole artifact. A build
  containing libhdfs-backed HDFS may fail to load where Java or Hadoop libraries
  are absent.
- A large published feature set increases build time, artifact size, supply
  chain surface, and platform exclusions.

### Static Build Use

Keep this path for custom source builds and hermetic distributions. It does not
meet the independently installable package requirement.

## Design B: Language-Owned Operator Adapters

### Language Adapter Seam

Each service package owns an operator and exposes a Python, Ruby, or JavaScript
operation protocol. The base binding delegates every operation through that
language protocol. Layers decorate language objects.

```text
language Operator facade -> language operation protocol -> package-owned backend
                                      ^
                                      |
                              language decorator
```

### Language Adapter Strengths

- Uses normal language package-loading mechanisms.
- Allows pure-language third-party services without a Rust ABI.
- Lets each native package own its core version and runtime because Rust values
  do not cross the package boundary.
- Isolates HDFS dependencies in the HDFS package.

### Language Adapter Limitations

- Repeats or generates the complete operation adapter for every language.
- Adds language calls to streaming and operation-body paths unless the adapter
  adds another native batching interface.
- Prevents a native OpenDAL layer from wrapping an operator owned by another
  extension.
- Eventually requires readers, writers, listers, deleters, copiers,
  cancellation, HTTP context, and executor context to reproduce native layers.

Timeout is decisive. `TimeoutLayer` wraps service calls and replaces the
executor in `OperationContext`. A method decorator cannot reproduce the
executor behavior used by concurrent block operations. Foyer is stateful and
intercepts operation bodies; a language decorator can implement a different
cache but cannot claim native `FoyerLayer` equivalence without a larger
protocol.

### Language Adapter Use

Keep this design as the service-only comparison. It does not meet the proposal's
complete native-layer requirement, and OpenDAL does not plan to maintain a full
operation adapter separately for every language.

## Design C: Exact-Release Shared Native Runtime

### Shared Runtime Seam

The binding adapter and all native packages use one shared native runtime
module. That module owns OpenDAL and Tokio types and registries. Each extension
registers one erased service or layer factory compiled for exactly the same
OpenDAL release. Node.js can place an environment adapter over those
process-scoped resources; it does not create a second OpenDAL graph for each
Worker.

```text
language adapter -> extension runtime <- service/layer native libraries
                         |
                         v
                  runtime-owned Operator
```

The loader reads compatibility metadata through a small C calling convention.
The [compatibility contract](compatibility.md#bootstrap-encoding-alternatives)
compares a JSON document with a C-layout descriptor for that metadata. The
loader validates the exact OpenDAL version and target before entering a
release-specific internal interface.

### Shared Runtime Strengths

- Preserves the native `Operator`, `Layer`, and `OperationContext` model.
- Keeps one OpenDAL core, Tokio runtime, and shared HTTP/context graph.
- Makes service and layer packages small and keeps configuration local.
- Supports stateful layer handles and asynchronous layer construction.
- Isolates native dependencies through lazy activation.
- Shares the extension SDK and conformance suite across language bindings.

### Shared Runtime Limitations

- Does not promise Rust ABI stability. Every native package rebuilds for each
  OpenDAL release.
- Requires cross-platform shared-library discovery, repair,
  rpath/install-name behavior, Windows DLL lookup, and symbol visibility work.
- Requires package managers to prevent or clearly reject mixed release trains.
- Needs artifact-level proof before one physical extension artifact can be
  shared across language ecosystems.
- Treats native packages as trusted code and does not unload them in the initial
  design.

### Shared Runtime Use

This is the recommended balance when install-time composition and complete
native layers matter, but old extension binaries do not need to survive OpenDAL
upgrades.

## Design D: Stable C Operation Interface

### Stable C Interface Seam

The base binding and each native extension exchange only C-layout function
tables, opaque reference-counted handles, fixed-width scalars, buffers, errors,
and versioned wire values. Rust values never cross the package boundary.

```text
language adapter -> base operation graph
                         |
                         v
                   stable C ABI
                         |
                         v
               extension operation graph
```

To support arbitrary services and layers, the interface must represent:

- Service construction, capabilities, and every operation.
- Readers, writers, listers, deleters, copiers, and streams.
- Futures, polling or completion, waking, and cancellation.
- Buffer ownership and creator-side destruction.
- Errors and extensible operation options.
- HTTP and executor resources in `OperationContext`.
- Separate service and context hooks for layers.

### Stable C Interface Strengths

- Allows extensions to use different OpenDAL core and Rust compiler versions.
- Lets a correctly versioned extension binary survive base-binding upgrades.
- Can share native extensions across Python, Ruby, and Node.js on one target.
- Allows other C-ABI languages to implement extensions.

### Stable C Interface Limitations

- Creates a second stabilized representation of OpenDAL's raw operation model.
- Creates a large unsafe verification surface around async cancellation, stream
  ownership, context composition, and wire evolution.
- Can load an old extension that lacks new operation semantics, requiring
  explicit negotiation and unsupported results for every evolution.
- Allows each extension to embed its own OpenDAL and Tokio graph, increasing
  size and making cross-extension layers more expensive.

Long-term ABI governance would require size-tagged function tables, append-only
minor evolution, explicit breaking revisions, creator-provided destructors,
and documented task polling and cancellation semantics. Binary compatibility
would mean safe loading and explicit capability negotiation, not automatic
support for every new operation.

### Stable C Interface Use

Keep this design as the comparison for cross-version binary compatibility.
OpenDAL does not plan to implement it because its maintenance and verification
costs exceed the current requirement. A service-only C interface would not meet
the native-layer requirement.

## Constraint Comparison

### S3 and WebDAV

All designs keep URI interpretation inside the service implementation. The base
binding passes a raw URI and explicit options instead of maintaining a central
configuration schema.

This matters because generic precedence is insufficient:

- `OperatorUri` applies query options first and explicit options second.
- S3 then derives bucket and root from the URI name and path.
- WebDAV derives `https://authority` as its endpoint, overwriting an endpoint
  option supplied earlier.

Designs B, C, and D preserve these rules when the package owns construction. A
base-owned universal schema is rejected in every design.

### HDFS

Design A cannot guarantee that the base artifact loads without HDFS native
dependencies when HDFS is compiled into it. Designs B, C, and D can register a
lightweight manifest and activate HDFS only at construction. `hdfs` and
`hdfs-native` remain independent packages and schemes.

### Foyer

Foyer requires asynchronous cache creation and a reusable stateful handle.
Design C holds the native layer in the shared runtime. Design D represents it
through a reference-counted layer handle. Design B can provide a language-level
cache but cannot claim arbitrary native layer composition.

Foyer's current key uses path and optional version, not service identity. No
design should advertise one cache handle as isolated across unrelated storages
until the package adds namespacing or restricts the handle to one logical
storage.

### Timeout and Throttle

Designs A, C, and D preserve both Timeout service wrapping and executor
wrapping. Design B cannot do so through method delegation alone.

One Throttle layer owns shared limiter state. Designs C and D preserve the
identity of the layer handle so applying it to two operators shares a quota.
The extension adapter must validate positive bandwidth and burst values before
calling a constructor that currently asserts them.

Every viable design must preserve the canonical Timeout/Retry ordering defined
by the [layer compatibility rules](compatibility.md#layer-compatibility-rules).

## Why Node.js Does Not Change the Decision

[Node-API](https://nodejs.org/api/n-api.html) is ABI-stable across supported
Node.js versions, so it is a strong boundary between JavaScript and the base
addon. It does not stabilize OpenDAL's internal Rust types or external libraries
used by the addon. Therefore Node.js benefits from Design C for the same reason
as Python and Ruby.

Node.js already publishes target-specific native npm packages, which provides a
useful packaging pattern for extension artifacts. It still needs an exact
OpenDAL version check, lazy activation, per-Worker adapter state, and a clear
rule against transferring JavaScript wrappers between Node environments.

## Recommendation

Prototype Design C with one language-neutral extension SDK and three binding
adapters. Preserve Design A as a supported custom-build path. Keep Designs B
and D documented as rejected alternatives so future proposals can evaluate
whether their requirements justify the maintenance or semantic costs.
