# OpenDAL C Binding (WIP)

## Example
A simple read and write example
```C
#include "assert.h"
#include "opendal.h"
#include "stdio.h"

int main()
{
    /* Initialize a operator for "memory" backend, with no options */
    opendal_operator_ptr op = opendal_operator_new("memory", 0);
    assert(op.ptr != NULL);

    /* Prepare some data to be written */
    opendal_bytes data = {
        .data = (uint8_t*)"this_string_length_is_24",
        .len = 24,
    };

    /* Write this into path "/testpath" */
    opendal_code code = opendal_operator_blocking_write(op, "/testpath", data);
    assert(code == OPENDAL_OK);

    /* We can read it out, make sure the data is the same */
    opendal_result_read r = opendal_operator_blocking_read(op, "/testpath");
    opendal_bytes* read_bytes = r.data;
    assert(r.code == OPENDAL_OK);
    assert(read_bytes->len == 24);

    /* Lets print it out */
    for (int i = 0; i < 24; ++i) {
        printf("%c", read_bytes->data[i]);
    }
    printf("\n");

    /* the opendal_bytes read is heap allocated, please free it */
    opendal_bytes_free(read_bytes);

    /* the operator_ptr is also heap allocated */
    opendal_operator_free(&op);
}
```
For more examples, please refer to `./examples`

## Prerequisites

To build OpenDAL C binding, the following is all you need:
- **A C++ compiler** that supports **c++14**, *e.g.* clang++ and g++

- To format the code, you need to install **clang-format**
    - The `opendal.h` is not formatted by hands when you contribute, please do not format the file. **Use `make format` only.**
    - If your contribution is related to the files under `./tests`, you may format it before submitting your pull request. But notice that different versions of `clang-format` may format the files differently.

- **GTest(Google Test)** need to be installed to build the BDD (Behavior Driven Development) tests. To see how to build, check [here](https://github.com/google/googletest).
- (optional) **Doxygen** need to be installed to generate documentations.

For Ubuntu and Debian:
```shell
# install C/C++ toolchain
sudo apt install -y build-essential

# install clang-format
sudo apt install clang-format

# install and build GTest library under /usr/lib and softlink to /usr/local/lib
sudo apt-get install libgtest-dev
cd /usr/src/gtest
sudo cmake CMakeLists.txt
sudo make
sudo cp lib/*.a /usr/lib
sudo ln -s /usr/lib/libgtest.a /usr/local/lib/libgtest.a
sudo ln -s /usr/lib/libgtest_main.a /usr/local/lib/libgtest_main.a
```

## Makefile
- To **build the library and header file**.

  ```sh
  make build
  ```

  - The header file `opendal.h` is under `./include` 

  - The library is under `../../target/debug` after building.


- To **clean** the build results.

  ```sh
  make clean
  ```

- To build and run the **tests**. (Note that you need to install GTest)

  ```sh
  make test
  ```

- To build the **examples**

  ```sh
  make examples
  ```

  

## Documentation
The documentation index page source is under `./docs/doxygen/html/index.html`.
If you want to build the documentations yourself, you could use
```sh
# this requires you to install doxygen
make doc
```


## License

[Apache v2.0](https://www.apache.org/licenses/LICENSE-2.0)
