# Apache OpenDAL™ C Binding (WIP)

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/c/)

A C binding for OpenDAL, providing blocking access to S3, GCS, Azure Blob, the
local filesystem, and 50+ more storage backends through a single C API.

The C binding is available as source. Its [Cargo.toml](./Cargo.toml) enables
the OpenDAL blocking API, and CMake builds the memory and `fs` services
by default.

**User guide**: https://opendal.apache.org/docs/bindings/c

## Building

Requirements: C11/C++14 compiler, CMake 3.24 or later, Rust toolchain.

```shell
mkdir -p build && cd build

# Default build (memory + fs services)
cmake ..
cmake --build .
```

`FEATURES` is a comma-separated list of Cargo features. Use the
`opendal/` prefix for service features listed in the [core Cargo.toml](https://github.com/apache/opendal/blob/main/core/Cargo.toml).
Setting `FEATURES` replaces the default list, so include `opendal/services-fs`
to retain the `fs` service.

To add S3 and GCS, reconfigure and rebuild from the same build directory:

```shell
cmake .. -DFEATURES="opendal/services-fs,opendal/services-s3,opendal/services-gcs"
cmake --build .
```

Configuration-driven layers require a registered factory and the corresponding
`opendal/layers-*` Cargo feature. The default build supports `concurrent_limit`,
`logging`, `retry`, and `timeout` as names in the `layers` option.

The header is at `include/opendal.h`. The default build writes the shared
library to `target/debug/` relative to `bindings/c`.

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

## Operator configuration

Both C constructors accept service options and scoped options set with
`opendal_operator_options_set`. Use `context.http.*` for the reqwest HTTP
context, `layers` for a comma-separated list of registered layers in
inner-to-outer order, and `layer.<name>.*` for options of a listed layer.
Pass configuration from trusted sources.

`opendal_operator_new` appends the C binding's default retry and logging layers
after any configured layers. Setting `layers=retry` therefore places a
configured retry layer inside the default retry layer.

`opendal_operator_new_with_layers` omits those C defaults and appends any
layers in the supplied C handle in insertion order. Pass `NULL` as the
handle to use the configured layers without appending any C layers. Both
constructors retain OpenDAL's core layers.

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
tests you need Valgrind and GTest. The C test suite requires the `fs` service,
which the default CMake configuration enables alongside the built-in memory
service:

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
callback for OpenDAL's Rust log facade. The logging layer added by
`opendal_operator_new` emits operation events through that callback. To enable
operation logging with `opendal_operator_new_with_layers`, set `layers=logging`
in the options. The callback may also receive other Rust log records from the
same library. Successful registration enables all Rust log levels for this
facade.

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
