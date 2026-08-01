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

# Dynamic Service and Layer Extensions

Status: pre-RFC design exploration. OpenDAL has not accepted or implemented the
extension interface described by these documents.

This design allows a language binding to install services and layers as
independent packages. For example, an application can install only S3,
Timeout, and Foyer without rebuilding or replacing the base binding.

The leading prototype candidate uses an exact-release shared native runtime.
Python, Ruby, and Node.js use different language interfaces over the same
native extension model. Selecting that candidate remains conditional on a
successful cross-platform packaging prototype and the OpenDAL RFC process.

## Documents

- [Design alternatives](alternatives.md) compares four possible extension
  seams and explains why Design C is the leading prototype candidate.
- [Compatibility and ABI](compatibility.md) defines version and target checks,
  loader checks, lifetime rules, and supported stability guarantees.
- [Python design](python.md) defines Python packages, discovery, typing, and
  migration.
- [Ruby design](ruby.md) defines Ruby gems, blocking construction, and
  middleware migration.
- [Node.js design](nodejs.md) defines npm packages, Node-API integration,
  worker behavior, and ESM/CommonJS loading.

These documents are preliminary design input. They are not an accepted RFC or
a commitment to package names, release dates, or compatibility guarantees. A
subsequent proposal must follow the [OpenDAL RFC
process](../../../core/core/src/docs/rfcs/README.md) before implementation.

## Requirements

The design must satisfy all of the following requirements:

- A service or layer package can be installed without rebuilding the base
  language binding.
- A service package owns its configuration schema, URI interpretation,
  credentials, redaction behavior, and registry metadata.
- A layer package preserves the complete native `Layer` behavior, including
  both service and operation-context composition.
- Stateful layers can be reused intentionally. Reusing one Throttle handle
  shares one limiter; reusing one Foyer handle shares one cache.
- Layer construction can be asynchronous. Foyer must not force asynchronous
  resource creation into a synchronous registration callback.
- The registry supports at least 1,000 installed manifests without loading
  every native library during base import.
- Official and third-party packages use the same extension interface and
  compatibility checks.
- The loader rejects incompatible native code before exchanging Rust values or
  storing callbacks.
- Active operators, layers, operation bodies, and tasks keep their extension
  code loaded.

## Current Implementation Evidence

The proposal derives its constraints from these current implementations:

- The [core split RFC](../../../core/core/src/docs/rfcs/6828_core.md) prepares
  service and layer crates for a future extension ecosystem but leaves dynamic
  loading unresolved.
- The current [operator registry](../../../core/core/src/types/operator/registry.rs)
  stores plain factories, while
  [`OperatorUri`](../../../core/core/src/types/operator/uri.rs) defines the
  initial URI and explicit-option merge.
- The native [`Layer` trait](../../../core/core/src/raw/layer.rs) composes both
  service and operation-context planes.
- [Timeout](../../../core/layers/timeout/src/lib.rs),
  [Foyer](../../../core/layers/foyer/src/lib.rs), and
  [Throttle](../../../core/layers/throttle/src/lib.rs) exercise context
  replacement, asynchronous state, and shared state respectively.
- The [S3 configurator](../../../core/services/s3/src/config.rs) and
  [WebDAV configurator](../../../core/services/webdav/src/config.rs) demonstrate
  why each service must own URI interpretation.
- The [HDFS service](../../../core/services/hdfs/README.md) documents its
  libhdfs/JVM environment, which must remain outside the base binding.
- The [Python](../../python/Cargo.toml), [Ruby](../../ruby/Cargo.toml), and
  [Node.js](../../nodejs/Cargo.toml) manifests show the current monolithic
  feature and native-library layouts.
- The [security threat model](../../../SECURITY-THREAT-MODEL.md) treats binding
  ownership, lifetime, and safe FFI behavior as part of OpenDAL's contract.

## Candidate Architecture

```text
Python package       Ruby gem              npm package
      |                  |                      |
      v                  v                      v
Python adapter       Ruby adapter          Node-API adapter
      \                  |                     /
       +-----------------+--------------------+
                         |
                         v
              OpenDAL extension runtime
        registry, Tokio, core types, loader,
          errors, handles, library leases
                  /                 \
                 v                   v
        service extension       layer extension
          S3 / WebDAV / HDFS    Timeout / Foyer / Throttle
```

The diagram shows a logical architecture. The first implementation may package
a separate runtime artifact for each language ecosystem. Sharing the SDK,
manifest rules, and conformance suite is required; sharing one physical runtime
library across Python, Ruby, and Node.js installations is not.

### Why one common runtime graph is required

Services and layers from independent packages must compose into the same
language-level `Operator`. A native layer must be able to wrap an operator from
another package while preserving both `Layer::apply_service` and
`Layer::apply_context`. That requires shared OpenDAL types, Tokio resources,
HTTP and executor context, registry ownership, and handle identity.

If each package owned a separate OpenDAL runtime graph, Rust `Operator`, `Layer`,
and `OperationContext` values could not cross package boundaries through a
supported stable ABI. Cross-package composition would then require either a
second stable operation interface or a complete operation adapter in each
language. The project does not plan to maintain those larger interfaces, and a
language adapter cannot preserve arbitrary native layer semantics.

The common runtime graph therefore owns composition machinery and extensions
register release-specific factories into it. Service packages still own their
configuration, URI interpretation, credentials, redaction, and package-local
dependencies. "Common" means one graph for extensions loaded by one binding
runtime; it does not require one physical runtime library to be shared across
Python, Ruby, and Node.js.

### Binding adapter

Each binding adapter translates language values, async behavior, exceptions,
and garbage collection into runtime-owned handles. It does not implement
service configuration or native layer composition.

The adapter preserves each language's normal interface:

- Python keeps `Operator`, `AsyncOperator`, `await`, type stubs, and Python
  package discovery.
- Ruby keeps blocking `Operator` operations, `require`, keyword arguments, and
  Ruby exceptions.
- Node.js keeps promises, synchronous variants where supported, JavaScript
  streams, ESM/CommonJS exports, and Node-API.

The project should share the native extension contract, not force one public
language API across all three bindings. The JSON manifest, bootstrap metadata,
configuration value grammar, factory semantics, error categories, and
conformance suite are common. Constructor names, typing, duration syntax,
blocking behavior, and package discovery remain binding-specific.

### Extension runtime

The runtime module owns process-scoped native resources. Python and Ruby
initially use one `NativeRuntime` per loaded runtime module and process. The
Node.js runtime calls that owner `ProcessRuntime` and places one
`EnvironmentAdapter` per `napi_env` over it.

The runtime module owns:

- The OpenDAL core and Tokio runtime used by extension objects.
- Service and layer registries.
- Scheme dispatch while preserving the original construction request for
  package-owned URI parsing and configurators.
- Operator, layer, and operation-body handles.
- Panic containment and language-neutral error details.
- Native library activation and process-lifetime pinning.

The current `OperatorRegistry` is not sufficient for this role. It stores
plain function pointers and replaces an existing scheme during registration.
The extension registry must record package ownership and perform atomic,
conflict-detecting registration.

### Language package

Each installable package contains:

- Language code or type declarations.
- A JSON manifest for exactly one service or layer, with the package ID,
  canonical component ID, service aliases when applicable, native artifact
  path, unique entry symbol, and required OpenDAL version.
- A target-specific native library, or source needed to build one.
- Package-specific documentation and conformance tests.

Registering the JSON manifest must not load the native library. The runtime
activates the library when a caller first constructs a declared service or
layer. This isolates HDFS and similar native dependencies.

### Native extension

A native extension contains exactly one service or layer factory. The package
exports one generated, package-unique bootstrap symbol and hides other
package-local symbols where the platform permits it.

The bootstrap exposes the package identity, target, and exact OpenDAL version
through a small C calling convention. The [compatibility
contract](compatibility.md#bootstrap-encoding-alternatives) compares a bounded
JSON document with an exact-release C-layout descriptor instead of removing
either encoding before prototyping. Only after all checks pass may the runtime
enter the release-specific internal interface. That internal interface is not a
stable Rust ABI.

Node.js packages are an exception to the unique-initializer rule when the
native artifact is itself a Node-API addon: Node defines the addon initializer.
The Node adapter must use a language-appropriate initializer and validate the
same package identity in the returned bootstrap metadata.

## Construction Requests

A service factory accepts one of two request forms:

```text
UriRequest {
    raw_uri,
    explicit_string_options,
}

ConfigRequest {
    structured_values,
}
```

`structured_values` uses the language-neutral `ConfigValue` grammar
defined by [the compatibility contract](compatibility.md#configuration-value-contract).
Each binding validates and converts its public values before invoking a native
factory; a native package never receives Python, Ruby, or JavaScript objects.

The URI request preserves the original URI and explicit options. The package
constructs its local `OperatorUri` and calls its own `Configurator::from_uri`.
The runtime must not replace service-specific behavior with a universal
precedence rule.

A layer factory accepts structured configuration and returns an asynchronous
result:

```text
create_layer(layer_id, structured_values) -> future<LayerHandle>
```

`LayerHandle` retains state and can be applied to more than one operator.
Applying a layer returns a new operator. The runtime never sorts layers: later
applications are outer layers, matching OpenDAL core behavior.

## Registration Rules

- Scheme and layer IDs use lowercase canonical strings.
- One manifest claims exactly one service or layer. A service manifest can also
  declare aliases for its canonical scheme.
- Re-registering the identical package and manifest is idempotent.
- A different owner claiming the component ID or one of its aliases rejects the
  manifest.
- Registry locks are not held while loading code or running a factory.
- Capabilities are read from a constructed operator, not declared statically in
  registration metadata.
- A failed native activation affects only the requested package.
- Extensions remain loaded for the rest of the process in the first version.

## Package Families

Each ecosystem needs a runtime distribution and independent extension packages:

```text
runtime distribution       minimal binding and native runtime
service/layer packages     exact dependency on that OpenDAL release
```

A minimal installation uses the runtime distribution plus selected extension
packages. Independently installable does not imply independently ABI-versioned:
every native package must be rebuilt for a new OpenDAL release.

The 1,000-manifest requirement measures registry behavior, not a commitment to
publish 1,000 official package families. Before a split release, each ecosystem
needs reserved package names, trusted publishing, coordinated release tooling,
and rollback rules. OpenDAL can publish only supported high-value extensions
while leaving the same SDK available to third parties.

## Security Model

Native extensions are trusted in-process code. Compatibility validation
prevents accidental mismatches; it does not sandbox, authenticate, or constrain
a malicious package.

OpenDAL still owns these in-scope properties:

- No panic or language exception crosses any FFI boundary.
- Safe binding operations do not cause use-after-free or data races.
- Official packages redact credentials from errors and diagnostics.
- Registry metadata and build diagnostics do not contain secrets.
- An extension callback cannot run after its library is unloaded.

The loader must use an explicit package-provided artifact path. It must not scan
arbitrary library search paths and execute every matching file.

## Delivery Gates

The design is not ready for an RFC decision until prototypes demonstrate:

1. A minimal runtime plus separately packaged FS and Retry tracers. Memory
   remains in the runtime because OpenDAL core always provides it.
2. S3 and WebDAV URI behavior without host-owned configuration schemas.
3. Lazy HDFS failure without affecting S3, WebDAV, or `hdfs-native`.
4. Complete Timeout context behavior and shared Throttle state.
5. Asynchronous, stateful Foyer construction and lifetime handling.
6. Atomic registration and lazy lookup with 1,000 synthetic packages.
7. Target packaging on supported Linux, macOS, and Windows variants.
8. Python, Ruby, and Node.js adapters passing the same native conformance suite.

If those gates pass, the next deliverable is a `0000_*.md` RFC. These pre-RFC
documents remain supporting analysis rather than evidence that the candidate
has already been accepted.
