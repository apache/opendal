# Apache OpenDAL™ C Binding (WIP)

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/c/)

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

> **Note**: This binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to this binding's version rather than the core version.

## Example

A simple read and write example

```C
#include "assert.h"
#include "opendal.h"
#include "stdio.h"

int main()
{
    /* Initialize a operator for "memory" backend, with no options */
    opendal_result_operator_new result = opendal_operator_new("memory", 0);
    assert(result.operator_ptr != NULL);
    assert(result.error == NULL);

    /* Prepare some data to be written */
    opendal_bytes data = {
        .data = (uint8_t*)"this_string_length_is_24",
        .len = 24,
    };

    /* Write this into path "/testpath" */
    opendal_error *error = opendal_operator_write(op, "/testpath", &data);
    assert(error == NULL);

    /* We can read it out, make sure the data is the same */
    opendal_result_read r = opendal_operator_read(op, "/testpath");
    opendal_bytes read_bytes = r.data;
    assert(r.error == NULL);
    assert(read_bytes.len == 24);

    /* Lets print it out */
    for (int i = 0; i < 24; ++i) {
        printf("%c", read_bytes.data[i]);
    }
    printf("\n");

    /* the opendal_bytes read is heap allocated, please free it */
    opendal_bytes_free(&read_bytes);

    /* the operator_ptr is also heap allocated */
    opendal_operator_free(&op);
}
```

For more examples, please refer to `./examples`

## Prerequisites

To build OpenDAL C binding, the following is all you need:

- A compiler that supports **C11** and **C++14**, _e.g._ clang and gcc

- To format the code, you need to install **clang-format**

  - The `opendal.h` is not formatted by hands when you contribute, please do not format the file. **Use `make format` only.**
  - If your contribution is related to the files under `./tests`, you may format it before submitting your pull request. But notice that different versions of `clang-format` may format the files differently.

- (optional) **Doxygen** need to be installed to generate documentations.

For Ubuntu and Debian:

```shell
# install C/C++ toolchain
sudo apt install -y build-essential

# install clang-format
sudo apt install clang-format

# install and build GTest library under /usr/lib and softlink to /usr/local/lib
sudo apt-get install libgtest-dev

# install CMake
sudo apt-get install cmake

# install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Makefile

- To **build the library and header file**.

  ```sh
  mkdir -p build && cd build
  cmake ..
  make
  ```

  - The header file `opendal.h` is under `./include`

  - The library is under `../../target/debug` after building.

  - use `FEATURES` to enable services, like `cmake .. -DFEATURES="opendal/services-memory,opendal/services-fs"`

- To **clean** the build results.

  ```sh
  cargo clean
  cd build && make clean
  ```

- To build and run the **tests**. (Note that you need to install Valgrind and GTest)

  ```sh
  cd build
  make tests && ./tests
  ```

- To build the **examples**

  ```sh
  cd build
  make basic error_handle async_stat
  ```

## Async APIs

The C binding also exposes a small asynchronous API surface, mirroring Rust's `async` operator. Each async call returns a future handle that you can await or cancel.

- Create an async operator with `opendal_async_operator_new` (cast the returned `op` field to `opendal_async_operator`).
- Start operations with `opendal_async_operator_stat`, `opendal_async_operator_write`, `opendal_async_operator_read`, and `opendal_async_operator_delete`.
- Await results using the matching `opendal_future_*_await` helpers, or abort early with `opendal_future_*_free`.

```c
opendal_result_operator_new res = opendal_async_operator_new("memory", NULL);
const opendal_async_operator* op = (const opendal_async_operator*)res.op;

opendal_bytes data = { .data = (uint8_t*)"hello", .len = 5, .capacity = 5 };
opendal_error* write_err = opendal_future_write_await(
    opendal_async_operator_write(op, "hello.txt", &data).future);

opendal_result_read read_out = opendal_future_read_await(
    opendal_async_operator_read(op, "hello.txt").future);
printf("read %zu bytes: %.*s\n", read_out.data.len, (int)read_out.data.len, read_out.data.data);
opendal_bytes_free(&read_out.data);

opendal_future_delete_await(opendal_async_operator_delete(op, "hello.txt").future);
opendal_async_operator_free(op);
```

See `tests/async_stat_test.cpp` for more complete usage with GoogleTest.

## Documentation

The documentation index page source is under `./docs/doxygen/html/index.html`.
If you want to build the documentations yourself, you could use

```sh
# this requires you to install doxygen
make doc
```

## Used by

Check out the [users](./users.md) list for more details on who is using OpenDAL.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
