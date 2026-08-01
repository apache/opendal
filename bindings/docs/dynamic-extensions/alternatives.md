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

This document compares the two implementation models that fit the project's
maintenance constraints. The designs differ in whether users select components
at build time or install them as packages later.

## Decision Guide

```text
Must packages compose after installation?
  no  -> Design A: feature-selected monolithic binding
  yes -> Design C: exact-release shared runtime
```

OpenDAL should prototype Design C. Design A remains useful for custom builds.
The project does not plan to maintain language-specific operation adapters or a
second stable operation ABI.

## Summary

<!-- markdownlint-disable MD013 -->

| Property                       | A. Static build   | C. Exact-release runtime |
| ------------------------------ | ----------------- | ------------------------ |
| Independently install services | No                | Yes                      |
| Independently install layers   | No                | Yes                      |
| Full native layer semantics    | Yes               | Yes                      |
| One core/runtime graph         | Yes               | Yes                      |
| Third-party source extensions  | Rebuild host      | Yes, exact release       |
| Cross-version native binaries  | Not applicable    | No                       |
| Native dependency isolation    | No                | Yes                      |
| Implementation cost            | Lowest            | High                     |
| Primary risk                   | Artifact variants | Packaging and linking    |

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

The loader reads a bounded JSON bootstrap document through a small C calling
convention. It validates the exact OpenDAL version and target before entering a
release-specific internal interface.

### Shared Runtime Strengths

- Preserves the native `Operator`, `Layer`, and `OperationContext` model.
- Keeps one OpenDAL core, Tokio runtime, and shared HTTP/context graph.
- Makes service and layer packages small and keeps configuration local.
- Supports stateful layer handles and asynchronous layer construction.
- Isolates native dependencies through lazy activation.
- Shares the extension SDK and conformance suite across language bindings.

### Shared Runtime Limitations

- Rust ABI stability is not promised. Every native package rebuilds for each
  OpenDAL release.
- Cross-platform shared-library discovery, repair, rpath/install-name behavior,
  Windows DLL lookup, and symbol visibility require substantial engineering.
- Package managers must prevent or clearly reject mixed release trains.
- One physical extension artifact shared across language ecosystems requires
  proof; the initial design requires only shared source and contracts.
- Native packages remain trusted code and cannot be unloaded safely in the
  initial design.

### Shared Runtime Use

This is the recommended balance when install-time composition and complete
native layers matter, but old extension binaries do not need to survive OpenDAL
upgrades.

## Constraint Comparison

### S3 and WebDAV

Both designs keep URI interpretation inside the service implementation. The
shared host passes a raw URI and explicit options instead of maintaining a
central configuration schema.

This matters because generic precedence is insufficient:

- `OperatorUri` applies query options first and explicit options second.
- S3 then derives bucket and root from the URI name and path.
- WebDAV derives `https://authority` as its endpoint, overwriting an endpoint
  option supplied earlier.

### HDFS

Design A cannot guarantee that the base artifact loads without HDFS native
dependencies when HDFS is compiled into it. Design C registers a JSON manifest
and activates HDFS only at construction. `hdfs` and `hdfs-native` remain
independent packages and schemes.

### Foyer

Foyer requires asynchronous cache creation and a reusable stateful handle.
Design C holds the native layer in the shared runtime.

Foyer's current key uses path and optional version, not service identity. The
design must not advertise one cache handle as isolated across unrelated storages
until the package adds namespacing or restricts the handle to one logical
storage.

### Timeout and Throttle

Both designs preserve Timeout service wrapping and executor wrapping. One
Throttle layer owns shared limiter state. Design C preserves the identity of the
layer handle so applying it to two operators shares a quota. The extension
adapter must validate positive bandwidth and burst values before calling a
constructor that currently asserts them.

Both designs must preserve the canonical Timeout/Retry ordering defined by the
[layer compatibility rules](compatibility.md#layer-compatibility-rules).

## Why Node.js Does Not Change the Decision

[Node-API](https://nodejs.org/api/n-api.html) is ABI-stable across supported
Node.js versions, so it is a strong seam between JavaScript and the base addon.
It does not stabilize OpenDAL's internal Rust types or external libraries used
by the addon. Therefore Node.js benefits from Design C for the same reason as
Python and Ruby.

Node.js already publishes target-specific native npm packages, which provides a
useful packaging pattern for extension artifacts. It still needs an exact
OpenDAL version check, lazy activation, per-Worker adapter state, and a clear
rule against transferring JavaScript wrappers between Node environments.

## Recommendation

Prototype Design C with one language-neutral extension SDK and three binding
adapters. Preserve Design A as a supported custom-build path.
