# Rust extension probe

This standalone workspace loads an OpenDAL service and layer from a native
library and passes existing Rust objects across the boundary. It does not define
a stable ABI or change OpenDAL core.

The experiment accompanies [RFC 8207](https://github.com/apache/opendal/pull/8207)
and follows the shared-runtime approach in the
[Lance probe](https://github.com/lance-format/lance/pull/10714).

## Components

- `runtime` is a Rust `dylib`. It owns the shared `opendal-core` and Tokio
  dependencies, re-exports their APIs, and loads extensions with `libloading`.
- `plugin` is a `cdylib`. Its `extern "Rust"` entry point registers the existing
  Fs builder as `demo-fs` and a factory returning `Arc<dyn Layer>` for the existing
  TimeoutLayer. Fs and TimeoutLayer are dependencies of the plugin only.
- `host` depends only on the runtime crate. It discovers both factories after
  loading the plugin and uses normal OpenDAL APIs. It creates the Tokio runtime.

Service factories use OpenDAL's existing `OperatorRegistry`, including its normal
operator construction and default layers. A small, separate `LayerRegistry`
exists only in this demo. `DynamicLayer` adapts `Arc<dyn Layer>` to the generic
`Operator::layer` argument and forwards both composition hooks.

The loader intentionally retains each successfully resolved library for the
process lifetime, before invoking its entry point. Factories, OIO handles,
futures, buffers, static strings, and destructors may therefore remain usable
after the load call returns and operators are dropped. There is no unload API.

## Run on Linux

From this directory:

```sh
python3 run.py
```

The runner builds all three crates together using the checked-in lockfile and
`-C prefer-dynamic`. It uses Cargo's reported artifact paths and does not choose a
target directory. It sets `LD_LIBRARY_PATH` for the child process to include the
runtime and the toolchain's dynamic standard library, checks dynamic dependencies
with `ldd`, and runs the host against a temporary directory. It removes the test
data afterward. Python 3, `ldd`, and the repository's Rust toolchain are required.

The executable assertions cover:

- The service and layer are unavailable before loading and available afterward.
  The plugin observes the same registry addresses and a current Tokio runtime.
- Host and plugin resolve the same runtime shared library. The service hook
  produces a wrapper; the context hook supplies an executor timeout and survives
  replacement of the operator's base context.
- A boxed timeout future crosses the boundary, initially returns `Pending`,
  wakes the host, and completes. The composed executor runs a host-created task.
- The plugin-backed Fs operator writes a two-segment `Buffer` through a writer
  and closes it. A separate standard-library file read verifies the bytes.
  Stat, list, range reads, delete, and `NotFound` also work.
- A reader remains usable after its operators are dropped, and its returned
  buffer remains usable after the reader is dropped. The plugin layer wraps a
  host-created memory service, which accepts that buffer. The registered service
  factory can create another operator after earlier operators are dropped.

Focused source checks:

```sh
cargo fmt --all -- --check
RUSTFLAGS='-C prefer-dynamic' cargo clippy --workspace --all-targets --locked -- -D warnings
```

## Compatibility and limits

The loader is unsafe: callers must supply a trusted library from the matching
build. Passing opaque pointers or trait objects does not make the Rust ABI
stable. This probe builds host, runtime, and plugin in one Cargo invocation with
one compiler, target, dependency graph, and configuration. An equal OpenDAL
version string is insufficient. Runtime changes can require plugin rebuilds.

This demo does not implement a build-identity check, independently built SDK
packaging, compatibility negotiation, or loading multiple runtime versions.
Those need a separate distribution design before accepting arbitrary packages.
Libraries remain resident even if an entry point panics after resolution.

The current probe targets Linux. It does not validate Python/JNI integration,
HTTP backends, custom allocators, unloading, or cross-version compatibility.
It checks native Buffer interoperability, not allocation counts or throughput;
it makes no measured zero-copy or performance claim.
