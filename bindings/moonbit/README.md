# Apache OpenDAL™ MoonBit Binding

> [!WARNING]
> This binding is experimental and source-only. Its API may change without
> notice.

The first development phase supports MoonBit's native target and the OpenDAL
memory service. It provides operator construction, whole-object reads and
writes, checked errors, and deterministic or automatic operator cleanup.

## Installation

Build the binding from an Apache OpenDAL source checkout. It currently requires:

- Rust 1.91 or later.
- A C compiler supported by MoonBit.
- MoonBit compiler `0.10.6+80dc50f24`.

The binding owns a small Rust `cdylib` that depends directly on OpenDAL core.
It does not ship prebuilt native artifacts or a MoonBit package registry
release. From `bindings/moonbit`, build the native bridge in release mode:

```shell
cargo build --release
```

Then expose the resulting library to the native linker and loader in the
current shell:

```shell
export OPENDAL_MOONBIT_LIB_DIR="$(cd target/release && pwd)"
export LIBRARY_PATH="$OPENDAL_MOONBIT_LIB_DIR${LIBRARY_PATH:+:$LIBRARY_PATH}"
export LD_LIBRARY_PATH="$OPENDAL_MOONBIT_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export DYLD_LIBRARY_PATH="$OPENDAL_MOONBIT_LIB_DIR${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
```

## Usage

In a consumer that vendors this source module, depend on `apache/opendal` and
use the memory service:

```moonbit
let operator = @opendal.Operator::new("memory")
defer operator.close()

operator.write("hello.txt", b"Hello, MoonBit!")
let content = operator.read("hello.txt")
```

`moon.pkg` declares only the logical `opendal_moonbit` native dependency; it
does not encode Cargo output directories or build profiles. Cargo does not run
automatically when Moon builds this module. Any native executable using this
source module must make a compatible `libopendal_moonbit` available to its
linker and loader. With MoonBit 0.10.6, the final executable package must also
repeat `-lopendal_moonbit` in its native link flags because dependency link
flags are not propagated. This phase requires an OpenDAL source checkout and
is not zero-configuration downstream consumption.

Fallible operations raise `OpenDalError`. Its `ErrorInfo` preserves a stable
error kind, the operation, the original path when available, and OpenDAL's
message. `close` is idempotent; reads and writes after close raise
`ResourceClosed`. A finalizer releases an operator that is not closed explicitly.

Whole-object reads use OpenDAL's reader API and reject objects larger than 64
MiB before growing the binding-owned output buffer beyond that limit. Streaming
APIs are outside this phase.

## Development

The MoonBit source checks do not require a built Rust bridge. From
`bindings/moonbit`, run:

```shell
moon check --target native --deny-warn
moon fmt --check
moon info --target native
```

These commands type-check the native package with warnings denied, verify
formatting, and regenerate the public package interface. Run the Rust checks
separately:

```shell
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
```

## Testing

After completing the native bridge setup from Installation, run the native
memory tests with:

```shell
moon test --target native --release
```

The tests cover binary and empty round-trips, Unicode paths, invalid text, typed
error paths, the whole-object allocation bound, idempotent close, use after
close, explicit release, and finalizer release.

## Current scope

This phase intentionally does not include other services, async APIs, streaming,
operation options, WebAssembly, JavaScript, Windows support, packaging, or
release artifacts. CI currently verifies the Linux native target only. The Rust
C ABI is private to this experimental binding.
