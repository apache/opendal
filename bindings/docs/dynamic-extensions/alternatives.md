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
language bindings. The designs place the extension seam at different levels,
which changes their native fidelity, compatibility, and implementation cost.

## Decision Guide

```text
Must packages compose after installation?
  no  -> Design A: feature-selected monolithic binding
  yes -> Must arbitrary native layers preserve core semantics?
           no  -> Design B: language-owned operator adapters
           yes -> Must an old binary survive runtime/core upgrades?
                    no  -> Design C: exact-build shared runtime
                    yes -> Design D: stable C operation interface
```

OpenDAL should prototype Design C. Design A remains useful for custom builds.
Design B does not meet the native-layer requirement. Design D should be chosen
only if cross-version third-party binary compatibility becomes a strategic
requirement that justifies stabilizing a second operation model.

## Summary

| Property                       | A. Static build   | B. Language adapters | C. Exact runtime       | D. Stable C interface      |
| ------------------------------ | ----------------- | -------------------- | ---------------------- | -------------------------- |
| Independently install services | No                | Yes                  | Yes                    | Yes                        |
| Independently install layers   | No                | Language decorators  | Yes                    | Yes                        |
| Full native layer semantics    | Yes               | No                   | Yes                    | Only represented semantics |
| One core/runtime graph         | Yes               | Usually no           | Yes                    | Usually no                 |
| Third-party source extensions  | Rebuild host      | Yes                  | Yes, exact build       | Yes                        |
| Cross-version native binaries  | Not applicable    | Language-dependent   | No                     | Yes, within ABI rules      |
| Cross-language native package  | No                | No                   | Possible, not promised | Yes, by design             |
| Native dependency isolation    | No                | Yes                  | Yes                    | Yes                        |
| Implementation cost            | Lowest            | Moderate             | High                   | Highest                    |
| Primary risk                   | Artifact variants | Semantic loss        | Packaging/linking      | ABI breadth and safety     |

## Design A: Feature-Selected Monolithic Binding

### Seam

Cargo features select services and layers when the language extension is
compiled. The resulting wheel, gem, or npm native package contains one OpenDAL
core and all selected implementations.

```text
language caller -> one native binding -> compiled services and layers
```

### Strengths

- Preserves all native behavior without a new runtime interface.
- Uses ordinary Rust ownership and one Tokio/core graph.
- Has the smallest implementation and verification cost.
- Works well for downstream users who build one controlled deployment image.

### Limitations

- A service cannot be added after the binding is built.
- Two feature variants normally provide the same import/module name and cannot
  be installed together.
- One uncommon native dependency can constrain the whole artifact. A build
  containing libhdfs-backed HDFS may fail to load where Java/Hadoop libraries
  are absent.
- A large published feature set increases build time, artifact size, supply
  chain surface, and platform exclusions.

### Appropriate Use

Keep this path for custom source builds and hermetic distributions. It does not
meet the independently installable package requirement.

## Design B: Language-Owned Operator Adapters

### Seam

Each service package owns an operator and exposes a Python, Ruby, or JavaScript
operation protocol. The base binding delegates every operation through that
language protocol. Layers decorate language objects.

```text
language Operator facade -> language operation protocol -> package-owned backend
                                      ^
                                      |
                              language decorator
```

### Strengths

- Package loading uses normal language mechanisms.
- Pure-language third-party services need no Rust ABI.
- Each native package can own its core version and runtime because Rust values
  do not cross the seam.
- HDFS dependencies remain isolated in the HDFS package.

### Limitations

- Every service package repeats or generates the complete operation adapter.
- Language calls appear in streaming and operation-body paths unless the
  adapter adds another native batching interface.
- A native OpenDAL layer cannot wrap an operator owned by another extension.
- Reproducing native layers eventually requires readers, writers, listers,
  deleters, copiers, cancellation, HTTP context, and executor context. At that
  point this design has recreated a language-specific version of Design D.

### Constraint Failures

Timeout is decisive. `TimeoutLayer` wraps service calls and replaces the
executor in `OperationContext`. A method decorator cannot reproduce the
executor behavior used by concurrent block operations.

Foyer is also stateful and intercepts operation bodies. A language decorator
can implement a different cache, but it cannot claim native `FoyerLayer`
equivalence without a much larger protocol.

### Appropriate Use

Use this design when service extensibility matters but native layer parity does
not. It does not meet this proposal's layer requirement.

## Design C: Exact-Build Shared Native Runtime

### Seam

The binding adapter and all native packages use one shared native runtime
module. That module owns OpenDAL/Tokio types and registries. Extensions register
erased service or layer factories compiled for exactly that runtime build.
Node.js can place an environment adapter over those process-scoped resources;
it does not create a second OpenDAL graph for each Worker.

```text
language adapter -> extension runtime <- service/layer native libraries
                         |
                         v
                  runtime-owned Operator
```

The loader first reads a stable, C-layout bootstrap descriptor. It validates an
exact runtime build ID before entering any build-specific internal interface.

### Strengths

- Preserves the native `Operator`, `Layer`, and `OperationContext` model.
- Keeps one OpenDAL core, Tokio runtime, and shared HTTP/context graph.
- Makes service and layer packages small and keeps configuration local.
- Supports stateful layer handles and asynchronous layer construction.
- Isolates native dependencies through lazy activation.
- Shares the extension SDK and conformance suite across language bindings.

### Limitations

- Rust ABI stability is not promised. Every native package rebuilds for each
  runtime build ID.
- Cross-platform shared-library discovery, repair, rpath/install-name behavior,
  Windows DLL lookup, and symbol visibility require substantial engineering.
- Package managers must prevent or clearly reject mixed runtime release trains.
- One physical extension artifact shared across language ecosystems requires proof;
  the initial design only requires shared source and contracts.
- Native packages remain trusted code and cannot be unloaded safely in the
  initial design.

### Appropriate Use

This is the recommended balance when install-time composition and complete
native layers matter, but old extension binaries do not need to survive runtime
upgrades.

## Design D: Stable C Operation Interface

### Seam

The host and each native extension exchange only C-layout function tables, opaque
reference-counted handles, fixed-width scalars, buffers, errors, and versioned
wire values. Rust values never cross the seam.

```text
language adapter -> host operation graph -> stable C ABI -> extension operation graph
```

To support arbitrary services and layers, the interface must represent:

- Service construction, capabilities, and every operation.
- Readers, writers, listers, deleters, copiers, and streams.
- Futures, polling or completion, waking, and cancellation.
- Buffer ownership and creator-side destruction.
- Errors and extensible operation options.
- HTTP and executor resources in `OperationContext`.
- Separate service and context hooks for layers.

### Strengths

- Extensions can use different OpenDAL core and Rust compiler versions.
- A correctly versioned extension binary can survive host upgrades.
- Native extensions can be shared by Python, Ruby, and Node.js on one target.
- Other C-ABI languages can implement extensions.

### Limitations

- The C interface becomes a second stabilized representation of OpenDAL's raw
  operation model.
- Async cancellation, stream ownership, context composition, and wire evolution
  create a large unsafe verification surface.
- Old extensions may be binary compatible while lacking new operation semantics.
  They must reject required fields they do not understand rather than silently
  dropping them.
- Each extension may embed its own OpenDAL/Tokio graph, increasing size and
  making cross-extension layers more expensive.

### ABI Evolution

If OpenDAL selects this design, it must govern the operation ABI independently
from every language binding:

- Every function table starts with a structure size and ABI version.
- A minor ABI revision can append optional methods but cannot reorder fields or
  change existing ownership or semantics.
- A breaking layout, ownership rule, or required semantic change increments the
  ABI major.
- Extensible wire values distinguish required and optional fields. An old
  extension rejects an unknown required field instead of silently losing it.
- Every output has a creator-provided destructor.
- Task polling and mutable operation bodies define concurrency and cancellation
  explicitly.
- Loader negotiation checks ABI major, wire major, target identity, and required
  interface IDs before it exchanges handles.

Stable ABI compatibility does not mean that an old extension implements every
new operation. It means the host can load the extension safely, negotiate its
capabilities, and receive an explicit unsupported result.

### Appropriate Use

Choose this design only when the project commits to long-term binary extension
governance. A service-only C interface is not enough for the layer requirement.

## Constraint Comparison

### S3 and WebDAV

All designs must keep URI interpretation inside the service implementation.
The shared host passes a raw URI and explicit options instead of maintaining a
central configuration schema.

This matters because generic precedence is insufficient:

- `OperatorUri` applies query options first and explicit options second.
- S3 then derives bucket and root from the URI name and path.
- WebDAV derives `https://authority` as its endpoint, overwriting an endpoint
  option supplied earlier.

Designs B, C, and D can preserve these rules if the package owns construction.
A host-owned universal schema is rejected in every design.

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

Designs A, C, and D can preserve both Timeout service wrapping and executor
wrapping. Design B cannot do so through method delegation alone.

One Throttle layer owns shared limiter state. Designs C and D preserve the
identity of the layer handle so applying it to two operators shares a quota.
The extension adapter must validate positive bandwidth and burst values before
calling a constructor that currently asserts them.

Timeout must be inside Retry. Applying Timeout and then Retry gives each retry
attempt a deadline. Applying Retry and then an outer Timeout is cancellation
unsafe because Timeout can drop Retry before it restores operation-body state.
An extension design must preserve the safe order and must not advertise an
outer whole-sequence Timeout as equivalent.

## Why Node.js Does Not Change the Decision

[Node-API](https://nodejs.org/api/n-api.html) is ABI-stable across supported
Node.js versions, so it is a strong seam between JavaScript and the base addon.
It does not stabilize OpenDAL's internal Rust types or external libraries used
by the addon. Therefore Node.js benefits from Design C for the same reason as
Python and Ruby.

Node.js already publishes target-specific native npm packages, which provides a
useful packaging pattern for extension artifacts. It still needs an exact
runtime build check, lazy activation, per-Worker adapter state, and a clear rule
against transferring JavaScript wrappers between Node environments.

## Recommendation

Prototype Design C with one language-neutral extension SDK and three binding
adapters. Preserve Design A as a supported custom-build path. Do not combine C
with B: a language-owned operator plus an exact-runtime capsule creates two
competing composition models and inherits both sets of lifetime rules.

Reconsider Design D only if users require already-built third-party extensions to
survive OpenDAL runtime upgrades. That requirement should be explicit because
it changes the project from coordinating a release train to governing a stable
operation ABI.
