This service implements filesystem access for WebAssembly components running
in WASI Preview 2 compatible runtimes such as Wasmtime or Wasmer.

## Capabilities

This service supports:

| Capability     | Supported |
|----------------|-----------|
| stat           | ✅        |
| read           | ✅        |
| write          | ✅        |
| create_dir     | ✅        |
| delete         | ✅        |
| list           | ✅        |
| copy           | ✅        |
| rename         | ✅        |
| atomic_write   | ❌        |
| append         | ✅        |

## Configuration

- `root`: Root path within the WASI filesystem. Must be within a preopened directory.

## Prerequisites

1. Target must be `wasm32-wasip2`
2. The WASI runtime must provide preopened directories
3. The requested root path must be accessible via preopened directories

## Example

```rust,ignore
use opendal::services::WasiFs;
use opendal::Operator;

let builder = WasiFs::default().root("/data");
let op = Operator::new(builder)?.finish();

// Read a file
let content = op.read("hello.txt").await?;

// Write a file
op.write("output.txt", "Hello, WASI!").await?;

// List directory
let entries = op.list("/").await?;
```

## Runtime Configuration

When running WebAssembly components, ensure the host runtime grants filesystem
access via preopened directories:

```bash
# Wasmtime
wasmtime run --dir /host/path::/guest/path component.wasm

# Wasmer
wasmer run --dir /host/path::/guest/path component.wasm
```

## Limitations

- **No atomic writes**: The `abort()` method returns an error since WASI doesn't provide atomic file operations
- **No file locking**: File locking is not supported
- **Symbolic links**: Support depends on the runtime
- **Timestamps**: Accuracy depends on runtime support
- **Platform-specific**: Only compiles for `wasm32-wasip2` target
