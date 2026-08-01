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

# Python Dynamic Extension Design

Status: pre-RFC binding-specific design proposal. The current Python binding
remains a single native distribution.

This document applies the [shared extension architecture](README.md) to Python.
It preserves `Operator` and `AsyncOperator` while moving service and layer
implementation dependencies into independently installable distributions.
The [shared compatibility contract](compatibility.md) is canonical for native
ABI, configuration, lifetime, and loader rules; this document defines Python
deltas.

## Current Constraints

The current Python binding has several compile-time assumptions that cannot
serve as a dynamic extension interface:

- `bindings/python` builds one PyO3 `cdylib`, `opendal._opendal`.
- Published wheels enable the binding's `services-all` feature, subject to its
  explicit exclusions and platform conditions.
- `Scheme` is a feature-gated Rust enum. It cannot represent a service installed
  after the base extension was compiled.
- `opendal.services` and `opendal.layers` are PyO3 submodules inserted into
  `sys.modules`, not filesystem packages that other distributions can extend.
- `Layer` stores `Box<dyn PythonLayer>` in the base native library. A PyO3
  subclass marker does not make that Rust trait object transferable from an
  independently linked extension.
- `opendal.config.ServiceConfig` is one generated closed union of compiled
  services.
- Operator pickle state records construction URI/options but does not record
  applied layers.

The migration must change these internals without requiring every caller to
adopt a new operator abstraction.

## Distribution Layout

The proposed release family is:

```text
opendal                   owns the `opendal` import package and native runtime
opendal-service-s3        contributes the S3 manifest, native code, and typing
opendal-service-hdfs      contributes libhdfs-backed HDFS lazily
opendal-layer-timeout     contributes Timeout
opendal-layer-foyer       contributes Foyer
```

Every native service/layer distribution requires an exact
`opendal` release and embeds that OpenDAL version in its JSON bootstrap
document. The base wheel remains installable on its own:

```console
python -m pip install opendal
```

An application installs the base runtime and selected packages:

```console
python -m pip install \
  opendal \
  opendal-service-s3 \
  opendal-layer-timeout \
  opendal-layer-foyer
```

The extension package names are provisional.

## Import Layout

The intended typed import layout is:

```text
opendal                    regular package owned by the base distribution
opendal.services           namespace subpackage
opendal.services.s3        supplied by opendal-service-s3
opendal.services.hdfs      supplied by opendal-service-hdfs
opendal.layers             namespace subpackage
opendal.layers.timeout     supplied by opendal-layer-timeout
opendal.layers.foyer       supplied by opendal-layer-foyer
```

Python packaging supports splitting namespace subpackages across
distributions, but every participant must follow one consistent layout. See
the [PyPA namespace package guide](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/).

Before using this layout, the base binding must move the current native
`opendal.services` and `opendal.layers` definitions under a private native
module and expose real Python package directories. Existing flat names can be
re-exported during migration.

If wheel-install and namespace ownership prototypes are not reliable across the
supported installers, the first tracer packages may use unambiguous top-level
imports such as `opendal_service_s3`. The runtime extension contract does not
depend on the cosmetic import layout.

## Discovery and Activation

Python entry points advertise installed manifests:

```toml
[project.entry-points."opendal.services"]
s3 = "opendal.services.s3:_register"

[project.entry-points."opendal.layers"]
foyer = "opendal.layers.foyer:_register"
```

[Entry points](https://packaging.python.org/en/latest/specifications/entry-points/)
allow the runtime to find an installed package without importing every package.
The resolver follows these rules:

1. `import opendal` loads only the base adapter and runtime.
2. An explicit service/layer import registers its JSON manifest.
3. Construction of an unregistered scheme must look up the one entry point with
   the matching canonical name and load only that registration stub. It reports
   a conflict if more than one distribution claims the name.
4. Native code activates only when the caller constructs that service/layer.
5. Resolver results and deterministic failures are cached.
6. Discovery never installs a missing distribution at runtime.

Explicit imports remain useful for deterministic startup and access to typed
configuration classes. Entry-point discovery preserves the current concise URI
path for callers that only need strings.

## Proposed Operator Interface

Existing construction remains valid:

```python
import opendal

op = opendal.Operator("s3", bucket="photos", region="us-east-1")

async_op = opendal.AsyncOperator.from_uri(
    "s3://photos/archive?region=us-east-1",
    endpoint="https://s3.example.com",
)
```

Strings are the canonical dynamic scheme identifiers. The existing `Scheme`
enum may remain as a frozen compatibility aid for previously bundled official
services, but it is not an inventory of installed extensions.

Typed configuration moves into its service distribution:

```python
from opendal import AsyncOperator
from opendal.services.s3 import S3Config

config = S3Config(scheme="s3", bucket="photos", region="us-east-1")
op = AsyncOperator.from_config(config)
```

The base `from_config` runtime path accepts a generic mapping or service recipe.
Package-local generated `TypedDict` or dataclass definitions provide field
checking without extending one base `ServiceConfig` union. The service package
owns structured serialization and validation for the matching OpenDAL release.

The adapter converts mappings to the shared
[`ConfigValue`](compatibility.md#configuration-value-contract) grammar. It
rejects unsupported Python objects, cyclic containers, oversized values,
unknown fields, and numeric overflow before package construction. Package-local
types can expose Python-native values, but no `PyObject` crosses the factory
seam.

URI construction sends the original URI plus explicit string options to the
service factory. It does not convert them through a central Python config
schema. This preserves S3 and WebDAV configurator behavior.

## Proposed Layer Interface

Simple layer factories remain synchronous:

```python
from opendal.layers.throttle import ThrottleLayer
from opendal.layers.timeout import TimeoutLayer

limit = ThrottleLayer(bandwidth=10 * 1024, burst=10 * 1024 * 1024)
timeout = TimeoutLayer(timeout=60.0, io_timeout=10.0)

layered = op.layer(limit).layer(timeout)
```

Resource-backed construction is asynchronous:

```python
from opendal.layers.foyer import FoyerLayer

cache = await FoyerLayer.create(
    memory_capacity=64 << 20,
    storage_path="/var/cache/opendal",
)

cached = async_op.layer(cache)
```

A blocking helper may be provided for synchronous applications only if it uses
the same runtime factory, releases the GIL while waiting, and has defined
cancellation/cleanup behavior. It must not create a second Tokio runtime inside
the Foyer package.

Every concrete Python layer wraps a base-owned opaque `LayerHandle`. It does
not expose a package-local Rust trait object. Applying it returns a new operator
and preserves the native layer's service and context hooks.

One layer object may carry shared state:

- Applying one Throttle object to two operators shares its quota.
- Applying two independently constructed Throttle objects creates two quotas.
- Applying one Foyer object reuses one cache, subject to the cache namespace
  restriction documented by that package.

Throttle accepts only positive integer `bandwidth` and `burst` values in the
supported `u32` range. Python validation must reject invalid values before the
native constructor can assert.

Timeout values remain finite, positive seconds at the Python interface. The
adapter uses the current `Duration::try_from_secs_f64` rule, which rounds to the
nearest nanosecond with ties to even, and then emits `SignedDuration`. It
rejects values outside `SignedDuration`'s `i64`-seconds range instead of
saturating them.

Later `.layer()` calls are outer layers. The binding preserves the
[canonical Timeout/Retry order](compatibility.md#layer-compatibility-rules) and
rejects the known unsafe composition when it can observe both layer IDs.

## Async Behavior

The shared runtime owns operation futures. `AsyncOperator` converts them into
Python awaitables through the base adapter. A service or layer package does not
capture Python event loops, `PyObject` references, or PyO3 runtime state in its
native factory.

Cancellation of a Python awaitable must reach the runtime operation. The
adapter must not detach a future merely because its Python wrapper was dropped.

Blocking `Operator` and `AsyncOperator` should retain the same constructed
native operator graph when converted or cloned. Rebuilding from a scheme and
options would lose stateful Foyer and Throttle identity.

## Errors

The Python adapter should distinguish extension failures before mapping normal
OpenDAL operation errors:

```text
ExtensionNotInstalled
ExtensionLoadError
ExtensionIncompatible
ExtensionConflict
LayerInitializationError
```

`ExtensionNotInstalled` may include the canonical distribution name as an
installation hint. It must not run `pip`, modify the environment, or infer a
third-party package name from untrusted input.

Configuration and operation errors continue to use OpenDAL's Python exception
hierarchy. Extension diagnostics include package and scheme/layer IDs but omit
credentials and unredacted option maps.

## Serialization

A dynamic operator cannot rely on native pointer serialization. New pickle
support must choose one of these explicit policies:

- Serialize a versioned service recipe plus ordered layer recipes, then
  reconstruct fresh native resources.
- Reject pickling when an applied service/layer has no declarative
  reconstruction policy.

It must never silently discard layers. Reconstructing a Foyer layer creates or
reopens a cache according to package policy; it does not preserve live in-memory
entries. Reconstructing Throttle starts new token history.

Existing layered pickles did not record layer recipes, so migration code cannot
recover that lost information retroactively.

## Packaging Constraints

The current release matrix includes CPython 3.10-specific wheels, CPython 3.11
`abi3` wheels, and free-threaded CPython wheels. The dynamic design must prove
which artifacts can actually be shared:

- The Python adapter follows its existing CPython/`abi3` compatibility rules.
- A language-neutral service/layer library should not link CPython.
- The extension still needs one artifact per supported native target and libc
  or deployment floor.
- One extension wheel can cover several Python versions only if all of those base
  wheels use the same OpenDAL version and compatible shared library.
- Wheel repair must retain the intended shared runtime relationship instead of
  copying private runtime libraries into every extension under conflicting
  names.
- Free-threaded Python requires explicit lifetime and concurrency tests; `abi3`
  does not imply free-threaded compatibility.

The libhdfs-backed HDFS wheel may have a smaller platform allowlist or ship as a
source distribution. Installing `opendal`, S3, WebDAV, or
`hdfs-native` must not load HDFS code or require Java/Hadoop.

## Migration

1. Add the runtime, JSON bootstrap, and registry internally while services/layers
   remain compiled into the base wheel.
2. Make built-in adapters use the same internal factory interface intended for
   external packages.
3. Turn `opendal.services` and `opendal.layers` into filesystem/namespace
   packages and re-export existing names.
4. Extract S3 and Timeout as tracer distributions. Keep Memory in the runtime
   because OpenDAL core always provides it.
5. Remove extracted components from the base wheel after their packages are
   available.
6. Generate package-local configuration types from the Rust service metadata.
7. Validate WebDAV, HDFS, Foyer, and Throttle before declaring the interface
   complete.
8. Introduce versioned serialization or explicit non-picklability for dynamic
   operators.

## Python Conformance Gates

- Base import with no optional extensions installed.
- Explicit imports and lazy entry-point lookup with 1,000 synthetic manifests.
- Namespace-package coexistence under pip and other supported installers.
- Sync and async S3/WebDAV construction through URI and typed config paths.
- HDFS registration/import without native activation and scoped load failure.
- Foyer async creation, cancellation, garbage collection, and reuse.
- Timeout executor behavior through a separately packaged layer.
- Throttle argument validation and shared-handle identity.
- Pickle reconstruction or explicit rejection with missing/incompatible
  packages.
- CPython 3.10, `abi3`, and free-threaded artifact composition on every
  supported target.
