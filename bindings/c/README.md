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
  make basic error_handle async_stat compare_sync_async
  ```

- The `compare_sync_async` example prints the same write/read/delete flow with both
  blocking and async operators so you can see the API differences in one run.

## Async APIs

OpenDAL’s C binding mirrors the Rust async operator, but keeps all runtime management on the Rust side so C callers never need to embed Tokio. The design is intentionally future/await centric:

- `opendal_async_operator_new` builds an async operator that internally holds a clone of the core `Operator` plus a handle to OpenDAL’s shared Tokio runtime.
- Each async method (`*_stat`, `*_write`, `*_read`, `*_delete`) immediately returns an opaque `opendal_future_*` handle. Creating the future is non-blocking—the runtime schedules the real work on its thread pool.
- You stay in control of when to pull the result. Call `opendal_future_*_await` to block the current thread until the operation finishes, or `opendal_future_*_poll` to integrate with your own event loop without blocking.
- If you abandon an operation, call `opendal_future_*_free` to cancel it. This aborts the underlying task and drops any pending output safely.

Because futures carry ownership of the eventual metadata/error objects, the `*_await` helpers always transfer heap allocations using the same conventions as the blocking API (free metadata with `opendal_metadata_free`, free errors with `opendal_error_free`, etc.).

### Usage example

Below is a full async stat sequence that starts the request, performs other work, then awaits the result. The same pattern applies to read/write/delete by swapping the function names.

```c
#include "opendal.h"
#include <stdio.h>
#include <unistd.h>

static void sleep_ms(unsigned int ms) { usleep(ms * 1000); }

int main(void) {
    opendal_result_operator_new res = opendal_async_operator_new("memory", NULL);
    if (res.error) {
        fprintf(stderr, "create async op failed: %d\n", res.error->code);
        opendal_error_free(res.error);
        return 1;
    }
    const opendal_async_operator* op = (const opendal_async_operator*)res.op;

    opendal_result_future_stat fut = opendal_async_operator_stat(op, "missing.txt");
    if (fut.error) {
        fprintf(stderr, "stat future failed: %d\n", fut.error->code);
        opendal_error_free(fut.error);
        opendal_async_operator_free(op);
        return 1;
    }

    printf("stat scheduled, doing other work...\n");
    sleep_ms(500); // keep UI/event loop responsive while I/O runs

    opendal_result_stat out = opendal_future_stat_await(fut.future);
    if (out.error) {
        printf("stat failed as expected: %d\n", out.error->code);
        opendal_error_free(out.error);
    } else {
        printf("stat succeeded, size=%llu\n",
               (unsigned long long)opendal_metadata_content_length(out.meta));
        opendal_metadata_free(out.meta);
    }

    opendal_async_operator_free(op);
    return 0;
}
```

Need non-blocking integration with your own loop? Call `opendal_future_stat_poll(fut.future, &out)` inside your loop. It returns `OPENDAL_FUTURE_PENDING` until the result is ready; once it reports `OPENDAL_FUTURE_READY`, call `opendal_future_stat_await` exactly once to consume the output.

See `examples/async_stat.c` for a narrated walkthrough and `tests/async_stat_test.cpp` for GoogleTest-based assertions that cover both success and error paths.

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
