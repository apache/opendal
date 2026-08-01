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

# Node.js Dynamic Extension Design

Status: pre-RFC binding-specific design proposal. The current Node.js binding
remains one native addon with compile-time service and layer selection.

The exact-build extension design is useful for Node.js. Node-API makes the base
addon portable across supported Node.js versions, but it does not stabilize the
Rust interface between separately built OpenDAL native packages.

This document applies the [shared extension architecture](README.md) with
Node-specific package loading, addon initialization, Worker, and event-loop
constraints.
The [shared compatibility contract](compatibility.md) is canonical for native
ABI, configuration, lifetime, and loader rules; this document defines Node.js
deltas.

## Current Constraints

- `bindings/nodejs` builds one napi-rs `cdylib` using Node-API version 6.
- Services are selected by Cargo features. The default includes S3 and WebDAV;
  the published feature set has explicit exclusions and target differences.
- `Operator` owns both asynchronous and blocking OpenDAL operators.
- Current layers are binding-local `NodeLayer` trait objects wrapped in
  napi-rs `External<Layer>` values.
- An `External<T>` from one independently built addon is not a supported handle
  for another addon. Its Rust type and layout belong to the addon that created
  it.
- The generated loader already selects target packages by operating system,
  architecture, and Linux libc, but it loads only one monolithic `.node` file.
- The package currently exposes only its root and `package.json` subpaths.
- ESM and CommonJS wrappers maintain central layer export lists and must stay in
  sync.

The dynamic design should reuse the target-package pattern without exposing
napi-rs implementation types as the extension contract.

## npm Package Layout

The proposed family is:

```text
@opendal/runtime                  Node-API adapter and JavaScript interface
@opendal/runtime-linux-x64-gnu    target-specific runtime addon
@opendal/service-s3               S3 JavaScript stub and types
@opendal/service-s3-linux-x64-gnu target-specific S3 native addon/library
@opendal/layer-timeout            Timeout stub and types
@opendal/layer-foyer              Foyer stub and types
opendal                           compatibility aggregator and re-export
```

Each root extension package declares:

- An exact peer dependency on the compatible `@opendal/runtime` release.
- Target packages as optional dependencies with `os`, `cpu`, and `libc`
  metadata where available.
- ESM and CommonJS entry points that use one registration implementation.
- A mandatory embedded runtime build-ID check.
- A clear error for optional dependencies omitted during installation.

The unscoped `opendal` package preserves the current import and installs the
runtime plus the service/layer set provided by the current release. It re-exports
the runtime classes and registers those packages' text manifests. It does not
contain a second native runtime.

Minimal applications depend directly on `@opendal/runtime` and selected
extensions. Package names remain provisional pending npm namespace and release
prototypes.

One root package plus several target packages per extension creates a large
publication matrix. A registry with 1,000 manifest records can be efficient;
that does not prove that publishing or loading 1,000 native npm package families
is operationally practical.

## Runtime Ownership

The native architecture gives process resources and Node environment state
different owners:

```text
loaded @opendal/runtime native module
  ProcessRuntime
    OpenDAL core, Tokio, registries, activation state, handle identity
    process-lifetime native library leases

main napi_env                 Worker napi_env
  EnvironmentAdapter A         EnvironmentAdapter B
  JS wrappers and callbacks    JS wrappers and callbacks
  Promise completion bridge    Promise completion bridge
```

One loaded runtime native module owns one `ProcessRuntime`. The exact peer
dependency should normally produce one such module in a process; nested
incompatible runtime installations can load distinct modules with distinct
process-runtime identities. The `ProcessRuntime` owns manifest conflicts,
activate-once state, native factories, Tokio/core resources, native handles, and
library leases.

Every `napi_env` owns one `EnvironmentAdapter`. It owns JavaScript wrappers,
references, resolver/activation callbacks, Promise completion bridges, and
environment cleanup hooks. Registering through an environment adapter commits
the manifest atomically into its `ProcessRuntime`; an identical process-level
registration is idempotent and a different owner is a conflict. Native package
activation runs once per `ProcessRuntime`, not once per Worker.

Node-API environments can be initialized and destroyed multiple times and can
run concurrently in Workers. A `napi_env`, `napi_value`, reference, JavaScript
callback, or public wrapper must never move between environment adapters. Node
documents these environment-lifecycle rules in its [Node-API
documentation](https://nodejs.org/api/n-api.html).

Environment cleanup stops new calls, cancels or finishes environment-owned
asynchronous work, releases all JavaScript references, and removes its local
callbacks. It does not unload process-pinned extension libraries or invalidate
native handles owned by another environment. A Worker can therefore terminate
independently of operations in another Worker.

## Native Registration Adapter

The first Node prototype should compare two packaging implementations:

1. The `ProcessRuntime` loads a language-neutral native library directly from
   the package manifest.
2. An environment-bound JavaScript activator loads a target-specific Node-API
   descriptor addon in the calling `napi_env`.

Both implementations use the same descriptor, runtime build ID, factories, and
conformance suite. A literal separately installed shared Rust `dylib` is not a
design assumption; npm/pnpm/Yarn layouts, rpaths, and Windows DLL discovery must
prove it first.

Direct host loading is the preferred starting point because it naturally
preserves text registration, process-level activate-once behavior, and
construction-time native activation.

The Node-API descriptor variant requires this explicit environment-bound
activation sequence:

1. The JavaScript registration stub gives its `EnvironmentAdapter` a text
   manifest and a local activation callback. It loads no native target package.
2. First construction asks the adapter, on its JavaScript thread, to invoke that
   callback.
3. The callback resolves the package's target artifact and synchronously loads
   its `.node` addon in the same `napi_env`. ESM and CommonJS wrappers call one
   shared loader implementation.
4. The addon uses the normal Node-API initializer and returns a `napi_external`
   containing the C-layout descriptor. It does not return a napi-rs class or an
   `External<T>` shared with the base.
5. The base validates the descriptor before giving its language-neutral exact
   entry to the `ProcessRuntime`. The process runtime installs the factories
   atomically and records an explicit process-lifetime library lease.
6. The environment adapter releases its activation callback after success. The
   factory does not retain `napi_env`, `napi_value`, JavaScript references, or
   thread-safe functions.

The variant is viable only if the platform prototype can retain a native
library lease independently from the initiating environment. A variant that
loads the HDFS addon during registration, or whose callbacks become invalid
when the initiating Worker exits, fails the design requirements.

The base schedules extension futures on runtime-owned native resources and
adapts completion into the calling environment.

## Registration Interface

Explicit registration avoids bundler-dependent side effects:

```javascript
import { Operator } from "@opendal/runtime";
import { registerS3 } from "@opendal/service-s3";

registerS3();

const op = Operator.fromUri("s3://photos/archive", {
  region: "us-east-1",
});
```

`registerS3()` is idempotent for the same package and `ProcessRuntime`. It
registers a text manifest without activating native code. Under the descriptor
addon prototype it also installs one environment-local activation callback;
under direct host loading the manifest's artifact path is sufficient.

The compatibility `opendal` aggregator can register its dependency set during
module initialization so existing construction remains concise:

```javascript
import { Operator } from "opendal";

const op = new Operator("s3", {
  bucket: "photos",
  region: "us-east-1",
});
```

The aggregator imports registration stubs, not every native target library.
Native activation still occurs on first construction.

A package may offer a documented side-effect registration subpath for
convenience, but it must mark that subpath appropriately for bundlers. The
explicit function remains the unambiguous interface.

## Proposed Layer Interface

`Operator.layer()` should accept a base-owned opaque `Layer`, not
`ExternalObject<Layer>` from napi-rs internals.

Synchronous factories work for layers without asynchronous resources:

```javascript
import { TimeoutLayer } from "@opendal/layer-timeout";
import { ThrottleLayer } from "@opendal/layer-throttle";

const timeout = new TimeoutLayer();
timeout.timeout = 60_000;
timeout.ioTimeout = 10_000;

const limit = new ThrottleLayer(10 * 1024, 10 * 1024 * 1024);

const layered = op.layer(limit.build()).layer(timeout.build());
```

The compatibility `opendal` package preserves these current constructors,
setters, and `.build()` calls. Internally, `.build()` returns a base-owned opaque
`Layer` instead of a package-local napi-rs `External<T>`. If the generated
`ExternalObject<Layer>` TypeScript name cannot remain as a deprecated alias, the
type-name change must wait for the binding's next breaking public release; it
does not justify weakening the native handle boundary.

Foyer uses a Promise-returning factory because JavaScript constructors cannot
be asynchronous:

```javascript
import { FoyerLayer } from "@opendal/layer-foyer";

const cache = await FoyerLayer.create({
  memoryCapacity: 64 << 20,
  storagePath: "/var/cache/opendal",
});

const cached = op.layer(cache);
```

The binding should not provide a synchronous Foyer constructor that blocks the
event loop. A separately documented worker/off-thread helper can be evaluated
later.

Layer packages validate JavaScript numbers before calling Rust. Timeout values
use non-negative integer milliseconds and convert to the shared
[`DurationNs`](compatibility.md#configuration-value-contract) representation.
The adapter rejects values above `u64::MAX` nanoseconds and never truncates,
saturates, or wraps them. Throttle bandwidth and burst must be positive integers
in the supported `u32` range so invalid input cannot reach the core
constructor's assertions. Other options convert through the same shared
`ConfigValueV1` grammar; native factories never receive JavaScript objects.

Applying one Throttle or Foyer handle to several operators preserves shared
native state. Later `.layer()` calls remain outer layers. Timeout must be
applied before Retry so each attempt has a deadline. Applying an outer Timeout
after Retry is unsupported because it can cancel Retry before operation-body
state is restored. The adapter preserves order and rejects this known unsafe
composition when both layer IDs are visible.

## Async Operations and Cancellation

The base adapter owns the conversion between runtime futures and JavaScript
Promises. Extension factories and operations do not call Node-API from Tokio
worker threads.

Promise cancellation policy must be explicit because JavaScript Promises do not
provide universal cancellation. Where an operation accepts an `AbortSignal`,
the adapter forwards it into the runtime and drops or aborts the native future
according to OpenDAL semantics.

Worker termination must not leave a callback targeting a destroyed `napi_env`.
Process-level native work may outlive one environment only when it has no
environment-owned completion callback and its resources have an explicit owner.

## ESM, CommonJS, and Bundlers

- ESM and CommonJS exports converge on one `EnvironmentAdapter` per Node
  environment and the same `ProcessRuntime` for that loaded runtime module.
- Calling registration through both module systems is idempotent.
- Export maps include a supported registration/runtime subpath instead of
  relying on generated private files.
- Side-effect-only registration modules declare their side effects so bundlers
  do not remove them.
- Package stubs resolve native artifacts relative to their own installed
  package, not the current working directory.
- Errors distinguish an unsupported target from installation with
  `--omit=optional` and from an incompatible runtime.

Dynamic native extensions are scoped to native Node-API targets. WASI and other
environments without compatible dynamic loading require a static bundled
design and should not silently fall back to this interface.

## Version and Error Behavior

Node-API version compatibility and OpenDAL runtime compatibility remain
separate. The generated package-version checks used by a native loader are not
a replacement for a mandatory embedded build-ID handshake.

Extension lifecycle errors should be JavaScript `Error` subclasses or errors
with stable codes:

```text
OPENDAL_EXTENSION_NOT_INSTALLED
OPENDAL_EXTENSION_LOAD_FAILED
OPENDAL_EXTENSION_INCOMPATIBLE
OPENDAL_EXTENSION_CONFLICT
OPENDAL_LAYER_INITIALIZATION_FAILED
```

Errors retain package ID, scheme/layer ID, target, and construction operation.
They do not expose credentials or an unredacted option object. Normal OpenDAL
errors retain their structured kind instead of becoming only a formatted
reason string.

## Multiple Runtime Versions

npm can install nested copies of a package. An extension stub may therefore see
a different `@opendal/runtime` instance from the one that created an operator.

The design applies three defenses:

1. Exact peer dependencies make the intended singleton visible to the package
   manager.
2. Registration records the specific `ProcessRuntime` identity.
3. A layer/operator wrapper verifies process-runtime identity before native
   handle use and throws `OPENDAL_EXTENSION_INCOMPATIBLE` on mismatch.

The adapter must never reinterpret a handle from another `ProcessRuntime`, even
when package versions appear equal. JavaScript wrappers also remain confined to
their creating `EnvironmentAdapter`.

## Migration

1. Introduce `ProcessRuntime`, `EnvironmentAdapter`, and the extension registry
   inside the current addon.
2. Replace the public `ExternalObject<Layer>` detail with a base-owned opaque
   layer wrapper while retaining current constructors.
3. Make compiled services/layers use the internal extension factory model.
4. Publish runtime and target packages using the existing platform-loader
   experience.
5. Extract S3 and Timeout as tracer package families and register them from the
   `opendal` compatibility aggregator.
6. Add Foyer async creation and HDFS lazy activation as design gates.
7. Test both direct native loading and Node-API descriptor addons before
   selecting the physical linking model.
8. Publish the third-party SDK only after Worker, ESM/CommonJS, target, and
   lifetime conformance passes.

## Node.js Conformance Gates

- ESM/CommonJS double registration, one environment adapter per `napi_env`, and
  process-level activate-once behavior.
- Main thread plus multiple Workers importing, using, and terminating adapters.
- Two incompatible nested runtime versions rejecting cross-instance handles.
- Missing optional target package and unsupported target diagnostics.
- glibc/musl, macOS, and Windows artifact selection.
- Bundler retention for any documented side-effect registration entry.
- S3/WebDAV construction without central JavaScript config schemas.
- HDFS registration without Java/Hadoop and isolated activation failure.
- Foyer Promise construction, rejection, cleanup, and reusable handle state.
- Timeout executor behavior and Throttle shared identity through extracted
  packages.
- No callback into a destroyed Node environment or unloaded native library.
- 1,000 synthetic registrations without loading 1,000 native addons.
