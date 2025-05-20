# Apache OpenDAL™ C Binding (WIP)

[![](https://img.shields.io/badge/status-unreleased-red)](https://opendal.apache.org/bindings/c/)

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

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

## Logging

The OpenDAL C binding provides two mechanisms for enabling logs, which can be helpful for debugging and understanding internal operations.

### 1. Environment Variable based Logging (via `RUST_LOG`)

This is the quickest way to get detailed logs from OpenDAL. It uses the `tracing` ecosystem from Rust, similar to how `RUST_LOG` works for other Rust applications.

**Initialization:**
Call `opendal_init_logger()` once at the beginning of your program.

```c
#include "opendal.h"

int main() {
    // Initialize the standard logger
    opendal_init_logger();

    // ... rest of your OpenDAL operations ...
    // Example:
    // opendal_operator_options *options = opendal_operator_options_new();
    // opendal_result_operator_new result_op_new = opendal_operator_new("memory", options);
    // opendal_operator *op = result_op_new.op;
    // if (op) {
    //     opendal_operator_exists(op, "some_path");
    //     opendal_operator_free(op);
    // }
    // opendal_operator_options_free(options);

    return 0;
}
```

**Usage:**
Set the `RUST_LOG` environment variable when running your application. For example:
- `RUST_LOG=trace ./your_application` (for very verbose logs from all components)
- `RUST_LOG=opendal_core=debug,opendal_c=info ./your_application` (for debug logs from `opendal_core` and info logs from the C binding itself)

Refer to the `examples/logging_example.c` for a runnable example.

### 2. Custom Callback based Logging (e.g., for `glog` integration)

This method allows you to redirect OpenDAL's logs to your existing C/C++ logging infrastructure, such as `glog`. You provide a C callback function that OpenDAL will invoke for each log event.

**Callback Signature:**
Your callback function must match the `opendal_glog_callback_t` type:
```c
typedef void (*opendal_glog_callback_t)(int level, const char *file, unsigned int line, const char *message);
```
- `level`: An integer representing the log level (e.g., 0 for DEBUG/TRACE, 1 for INFO, 2 for WARNING, 3 for ERROR).
- `file`: The source file where the log originated (can be NULL).
- `line`: The line number in the source file (can be 0).
- `message`: The log message.

**Initialization:**
Call `opendal_init_glog_logging()` once at the beginning of your program, passing your callback function.

```c
#include "opendal.h"
#include <stdio.h> // For a simple printf logger

// Your custom logging function
void my_custom_logger(int level, const char* file, unsigned int line, const char* message) {
    const char* level_str = "UNKNOWN";
    switch (level) {
        case 0: level_str = "DEBUG"; break;
        case 1: level_str = "INFO"; break;
        case 2: level_str = "WARN"; break;
        case 3: level_str = "ERROR"; break;
    }
    if (file) {
        printf("[%s] %s:%u: %s\n", level_str, file, line, message);
    } else {
        printf("[%s] %s\n", level_str, message);
    }
    fflush(stdout);
}

int main() {
    // Initialize with your custom glog-style logger
    opendal_init_glog_logging(my_custom_logger);

    // ... rest of your OpenDAL operations ...
    // Example:
    // opendal_operator_options *options = opendal_operator_options_new();
    // opendal_result_operator_new result_op_new = opendal_operator_new("memory", options);
    // opendal_operator *op = result_op_new.op;
    // if (op) {
    //     opendal_operator_exists(op, "another_path");
    //     opendal_operator_free(op);
    // }
    // opendal_operator_options_free(options);

    return 0;
}
```

Refer to the `examples/glog_example.c` for a runnable example.

**Note:** Currently, while the infrastructure for `glog` style callback logging is in place and `opendal_init_glog_logging` can be called, there is an outstanding issue where OpenDAL's internal logs may not be consistently routed through this callback. The `RUST_LOG` based method is more reliable for observing detailed internal logs at this time. The `LoggingLayer` is automatically applied to operators, which generates the necessary trace events.

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
  make basic error_handle
  ```

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
