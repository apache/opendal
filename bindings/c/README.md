# Apache OpenDAL™ C Binding (WIP)

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/c/)

A C binding for OpenDAL, providing blocking access to S3, GCS, Azure Blob, the
local filesystem, and 50+ more storage backends through a single C API.

We release the OpenDAL C binding independently of the
[`opendal` crate](https://crates.io/crates/opendal) (Rust core). For updates
and compatibility, use the C binding version instead of the `opendal` crate
version.

**User guide**: https://opendal.apache.org/docs/bindings/c

## Building

Requirements: C11/C++14 compiler, CMake, Rust toolchain.

```shell
mkdir -p build && cd build

# Default build (memory + fs services)
cmake ..
make

# Enable additional services
cmake .. -DFEATURES="opendal/services-s3,opendal/services-gcs"
make
```

The header is at `include/opendal.h`. The shared library is written to
`../../target/debug/` after building.

## Example

```c
#include <assert.h>
#include <stdio.h>
#include "opendal.h"

int main(void)
{
    /* Build an operator for the "memory" service. */
    opendal_operator *op = NULL;
    opendal_error *err = opendal_operator_new("memory", NULL, &op);
    assert(err == NULL && op != NULL);

    /* Write bytes. */
    const char *msg = "Hello, OpenDAL!";
    opendal_bytes data = { .data = (uint8_t *)msg, .len = 15, .capacity = 15 };
    err = opendal_operator_write(op, "/hello.txt", &data);
    assert(err == NULL);

    /* Read them back. */
    opendal_bytes read_bytes;
    err = opendal_operator_read(op, "/hello.txt", &read_bytes);
    assert(err == NULL);
    printf("%.*s\n", (int)read_bytes.len, read_bytes.data);
    opendal_bytes_free(&read_bytes);

    opendal_operator_free(op);
    return 0;
}
```

See `examples/` for more, including error handling patterns.

Full documentation is at https://opendal.apache.org/docs/bindings/c.

## Output parameters

Fallible operations return `opendal_error *`: `NULL` means success. Result values
are written through output parameters; free a returned error with
`opendal_error_free()`.

Each output pointer must be non-NULL, correctly aligned, and point to writable
storage for its declared type. The storage may be uninitialized. Calls initialize
outputs on success and ordinary failure and do not retain their addresses.
Failure outputs are NULL pointers, zero scalars, or empty `opendal_bytes`.
Release any previously owned output before reusing its storage: the call
replaces the value without freeing it. Free returned handles and byte buffers
with their corresponding OpenDAL release routines.

## Contributing

See [`CONTRIBUTING.md`](../../CONTRIBUTING.md) at the repository root. To run
tests you need Valgrind and GTest:

```shell
cd build && make tests && ./tests
```

Format C sources with `make format` (requires `clang-format`). Do not manually
format `include/opendal.h`.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.

## Native logging

Call `opendal_logging_init` once to install a program-lifetime, thread-safe C
callback for OpenDAL's Rust log facade. The default `opendal_operator_new`
constructor emits operation events through that callback. The explicit
`opendal_operator_new_with_layers` constructor uses only the supplied layers.
The callback may also receive other Rust log records from the same library.
Successful registration enables all Rust log levels for this facade.

The callback receives DEBUG=0 (including TRACE), INFO=1, WARN=2, or ERROR=3,
a source file, a line number, and a message. File and message are borrowed,
NUL-terminated strings valid only during the callback. Copy values that must
outlive the invocation. Embedded NUL bytes are escaped as `\0`. The callback
must not unwind or recursively log through this facade.

The initializer returns NULL on success. Free a returned error with
`opendal_error_free`; a null callback or an already installed logger returns
`OPENDAL_CONFIG_INVALID`. Failed registration keeps the existing logger and
its level. The library cannot replace or unregister it.

When adapting to glog, map levels explicitly: DEBUG to `VLOG(1)`, INFO to
`LOG(INFO)`, WARN to `LOG(WARNING)`, and ERROR to `LOG(ERROR)`. The callback's
integer values are not glog severity constants.
