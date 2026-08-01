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

# Ruby Dynamic Extension Design

Status: pre-RFC binding-specific design proposal. The current Ruby binding
remains one gem and one Magnus native extension.

This document applies the [shared extension architecture](README.md) to Ruby.
It incorporates the version-locked shared runtime alternative from the earlier
Ruby design and aligns that native model with Python and Node.js.
The [shared compatibility contract](compatibility.md) is canonical for native
ABI, configuration, lifetime, and loader rules; this document defines Ruby
deltas.

## Current Constraints

The current Ruby binding provides a useful migration base but not a native
extension seam:

- `bindings/ruby` builds one `opendal_ruby` `cdylib` and one `opendal` gem.
- `OpenDal::Operator.new(scheme, options)` is blocking-only and constructs
  through the compiled core registry.
- Ruby does not currently expose `Operator.from_uri`, `Operator.via_iter`, or an
  async operator.
- Retry, concurrent-limit, Throttle, and Timeout middleware implementations are
  compiled into the same native extension.
- `Operator#middleware` uses Ruby duck typing, but an independently built native
  middleware still cannot access the wrapped Rust `Operator` in another DSO.
- Operation failures currently map broadly to Ruby `RuntimeError`.
- The release process builds a source gem and a small best-effort native-gem
  matrix. A source-build path remains important.

The dynamic design must not describe proposed methods or guarantees as current
behavior.

## Gem Layout

The proposed release family is:

```text
opendal-runtime          owns `require "opendal"` and the shared runtime
opendal-service-s3       provides S3 registration and native artifacts
opendal-service-hdfs     provides libhdfs-backed HDFS lazily
opendal-layer-timeout    provides Timeout
opendal-layer-foyer      provides Foyer
opendal                  compatibility aggregator
```

Each native service/layer gem requires an exact `opendal-runtime` release and
embeds its runtime build ID. The `opendal` compatibility gem depends on the
runtime and the extensions included by the current monolithic gem. This
preserves:

```console
gem install opendal
```

Applications that need a smaller installation select the runtime and extensions
in their `Gemfile`:

```ruby
gem "opendal-runtime", "= <runtime-release>"
gem "opendal-service-s3"
gem "opendal-layer-timeout"
gem "opendal-layer-foyer"
```

The aggregator must not install files that conflict with the runtime gem. It is
metadata and dependency coordination, not a second owner of `lib/opendal.rb` or
the runtime native library.

## Registration and Activation

Each extension gem contains a Ruby registration stub, a text manifest, and gem
metadata mapping its canonical service/layer IDs to that stub:

```ruby
require "opendal/runtime"

OpenDal::Runtime.register_manifest(
  File.expand_path("../../../opendal-extension.json", __dir__)
)
```

The expected require paths are:

```ruby
require "opendal"
require "opendal/services/s3"
require "opendal/layers/timeout"
require "opendal/layers/foyer"
```

Requiring an extension reads and registers metadata but does not activate its
native library. The first service/layer construction performs native loading
and the exact build-ID check.

The compatibility aggregator publishes a generated index of the registration
stubs provided by its exact dependencies. When `require "opendal"` activates
that aggregator, the runtime requires each indexed registration stub. Those
stubs register text manifests only; they do not load native libraries.

For a minimal installation, construction of an unregistered scheme must resolve
the one matching registration stub from installed gem metadata. The resolver
reports duplicate claims, caches results and deterministic failures, handles
aliases deterministically, and never requires every native extension at
startup. Installing dependencies alone is not treated as registration.

Explicit `require` remains the preferred deterministic registration path. An
application that wants to detect native dependency failures during controlled
startup must also construct or explicitly probe the service/layer, because
registration alone intentionally performs no native load.

## Proposed Operator Interface

`Operator.new` remains the compatibility constructor:

```ruby
require "opendal"
require "opendal/services/s3"

op = OpenDal::Operator.new("s3", {
  "bucket" => "photos",
  "region" => "us-east-1",
})
```

The binding can add URI and explicit registry construction as additive methods:

```ruby
op = OpenDal::Operator.via_iter("s3", {
  "bucket" => "photos",
  "region" => "us-east-1",
})

op = OpenDal::Operator.from_uri(
  "s3://photos/archive?region=us-east-1",
  {"endpoint" => "https://s3.example.com"}
)
```

After those methods exist, `Operator.new` delegates to `via_iter`. Scheme
strings remain canonical so third-party services do not require edits to a
base enum.

The service gem receives the original URI and explicit string options. S3,
WebDAV, HDFS, and third-party gems retain their own configurator behavior,
validation, credentials, and redaction.

Typed Ruby configuration objects and hashes convert to the shared
[`ConfigValueV1`](compatibility.md#configuration-value-contract) grammar. The
base adapter rejects symbols or objects without a declared conversion, cyclic
containers, oversized values, unknown fields, and numeric overflow before
calling package code. Native factories never retain Ruby objects.

## Proposed Layer Interface

New code uses `OpenDal::Layers` and `Operator#layer`:

```ruby
require "opendal/layers/throttle"
require "opendal/layers/timeout"

limit = OpenDal::Layers::Throttle.new(10 * 1024, 10 * 1024 * 1024)
timeout = OpenDal::Layers::Timeout.new(60, 10)

layered = op.layer(limit).layer(timeout)
```

The Ruby object wraps a runtime-owned native `LayerHandle`, not a package-local
Rust object exposed through Magnus. `Operator#layer` returns a new operator and
preserves native service and context hooks.

The current names remain compatibility adapters:

- `Operator#middleware(value)` delegates to `Operator#layer(value)`.
- `OpenDal::Middleware::*` aliases the corresponding `OpenDal::Layers::*`
  classes during a deprecation period. The layer classes preserve the current
  positional constructors so the aliases do not change existing calls.
- A pure Ruby object implementing only `apply_to` remains a Ruby decorator and
  must not be described as equivalent to an arbitrary native layer.

Timeout values remain finite, non-negative seconds at the Ruby interface. The
adapter uses the current `Duration::try_from_secs_f64` rule, which rounds to the
nearest nanosecond with ties to even, and then emits `DurationNs`. It rejects
values above `u64::MAX` nanoseconds instead of saturating them.

Throttle accepts only positive integer `bandwidth` and `burst` values in the
supported `u32` range. Ruby validation must reject invalid values before the
native constructor can assert.

Later layer calls are outer layers. Timeout must be applied before Retry, which
puts Retry outside Timeout and gives each attempt a deadline. Applying Timeout
after Retry is unsupported because the outer Timeout can cancel Retry before it
restores operation-body state. The binding preserves order and rejects that
known unsafe composition when it can observe both layer IDs.

## Asynchronous Layer Construction

Ruby operations remain blocking in the initial design. A layer such as Foyer
still needs asynchronous native initialization:

```ruby
require "opendal/layers/foyer"

cache = OpenDal::Layers::Foyer.build(
  memory_capacity: 64 << 20,
  storage_path: "/var/cache/opendal"
)

cached = op.layer(cache)
```

`Foyer.build` submits the async factory to the shared runtime and waits while
releasing the GVL. It must use the runtime's Tokio instance, clean up partial
resources on failure, and return only after it owns a valid `LayerHandle`.

The package must not start a private Tokio runtime or hold Ruby values inside
its native future. A future Ruby async interface can adapt the same runtime
future without changing the extension interface.

## Stateful Layers

One Ruby layer object preserves one native sharing identity:

- Applying one Throttle object to several operators shares one quota.
- Constructing two Throttle objects creates independent quotas.
- Applying one Foyer object shares one cache subject to its documented
  namespace restriction.
- Derived operators keep the layer alive after the original Ruby wrapper is
  collected.

The first version does not define `Marshal` support for operators or live layer
handles. A future declarative recipe format must reconstruct new native state
rather than claiming to serialize cache contents, limiter history, a JVM, or a
Tokio runtime.

## Errors

The runtime should expose Ruby exception classes for extension lifecycle
failures:

```text
OpenDal::ExtensionNotInstalled
OpenDal::ExtensionLoadError
OpenDal::ExtensionIncompatible
OpenDal::ExtensionConflict
OpenDal::LayerInitializationError
```

Normal OpenDAL error kinds should also map to stable Ruby exception classes
rather than losing all structure in `RuntimeError`. Compatibility aliases or a
common superclass can preserve existing rescue behavior.

Errors include package ID, scheme/layer ID, and construction operation. They do
not include credentials or unredacted option hashes.

## Native Gem and Loader Constraints

- The base Magnus extension follows the binding's supported Ruby versions and
  platforms.
- A language-neutral service/layer library should not link Ruby or Magnus.
- Native extension gems still need artifacts for every supported OS,
  architecture, libc/deployment floor, and runtime build ID.
- Source gems build against the exact SDK/runtime metadata and verify the
  resulting embedded build ID.
- The runtime loads an explicit artifact path from the gem manifest and keeps
  the library pinned.
- Linux symbol visibility, macOS install names, and Windows DLL discovery must
  be tested with gems installed in normal Bundler layouts.
- A native-gem failure may fall back to a documented source build, but it must
  not silently load a different runtime build.

The current native-gem matrix is best effort. Dynamic extensions should not
claim broader binary coverage until runtime plus adapter artifacts pass an
installation test on that platform.

## Ractor, Threads, and Fork

The first design does not promise Ractor shareability. Runtime registries and
native handles may be process-global Rust state, but they must not retain
Ractor-local Ruby objects.

Blocking operations and layer initialization release the GVL only through
well-defined base-adapter helpers. Package code must not call Ruby from shared
runtime worker threads.

Runtime, JVM, connection-pool, and Foyer state is unsupported after `fork`
unless a package later defines explicit reinitialization behavior.

## Migration

1. Introduce `NativeRuntime` and an extension registry inside the current gem.
2. Route compiled services and middleware through internal factories using the
   proposed SDK shapes.
3. Add `via_iter`, `from_uri`, `layer`, `OpenDal::Layers`, and structured errors
   without removing current methods.
4. Extract S3 and Timeout as tracer gems and make `opendal` depend on them.
5. Add gem-metadata resolution for compatibility constructors.
6. Validate WebDAV configurator behavior and HDFS lazy activation.
7. Validate Foyer initialization and Throttle sharing before publishing the
   third-party SDK.
8. Expand native gem targets only after artifact-level installation tests pass.

## Ruby Conformance Gates

- Base runtime installation and `require "opendal"` without optional extensions.
- Explicit requires and selected lazy resolution without eager native loading.
- Existing `Operator.new` and `middleware` compatibility behavior.
- Proposed URI construction after `from_uri` is implemented.
- S3/WebDAV secret-free construction errors.
- HDFS failure scoped to the HDFS gem while other services remain usable.
- Foyer initialization with the GVL released, including cancellation/cleanup.
- Layer wrapper garbage collection while derived operators remain active.
- Source gem plus every claimed native gem target.
- Thread, Ractor-rejection, and fork-rejection behavior documented and tested.
