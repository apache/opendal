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

The binding links to the sibling [`bindings/c`](../c) library and does not ship
prebuilt native artifacts or a MoonBit package registry release. Build the C
library from `bindings/moonbit` before compiling a consumer:

```shell
make build-c
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

The source build currently links the shared library produced in
`bindings/c/target/debug`. `make test` configures its runtime lookup path. For a
different native executable launched from `bindings/moonbit`, expose the same
directory first:

```shell
# Linux
export LD_LIBRARY_PATH="$(pwd)/../c/target/debug:${LD_LIBRARY_PATH:-}"

# macOS
export DYLD_LIBRARY_PATH="$(pwd)/../c/target/debug:${DYLD_LIBRARY_PATH:-}"
```

Fallible operations raise `OpenDalError`. Its `ErrorInfo` preserves a stable
error kind, the operation, the original path when available, and OpenDAL's
message. `close` is idempotent; reads and writes after close raise
`ResourceClosed`. A finalizer releases an operator that is not closed explicitly.

Whole-object reads collect data through the C binding's reader API and reject
objects larger than 64 MiB before allocating their contents. Streaming APIs are
outside this phase.

## Development

From `bindings/moonbit`, build the OpenDAL C binding and check the MoonBit source:

```shell
make check
```

`make check` builds `bindings/c`, checks the native package with warnings denied,
verifies formatting, and regenerates the public package interface.

## Testing

Run the native memory tests with:

```shell
make test
```

The tests cover binary and empty round-trips, Unicode paths, invalid text, typed
error paths, the whole-object allocation bound, idempotent close, use after
close, explicit release, and finalizer release.

## Current scope

This phase intentionally does not include other services, async APIs, streaming,
operation options, WebAssembly, JavaScript, packaging, or release artifacts.
