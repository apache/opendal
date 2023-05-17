# Contributing
1. [Contributing](#contributing)
   1. [Setup](#setup)
      1. [Using a dev container environment](#using-a-dev-container-environment)
      2. [Bring your own toolbox](#bring-your-own-toolbox)
   2. [Build](#build)
   3. [Test](#test)
   4. [Docs](#docs)
   5. [Misc](#misc)

## Setup

### Using a dev container environment
OpenDAL provides a pre-configured [dev container](https://containers.dev/) that could be used in [Github Codespaces](https://github.com/features/codespaces), [VSCode](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/), [JuptyerLab](https://jupyterlab.readthedocs.io/en/stable/). Please pick up your favourite runtime environment.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/incubator-opendal?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox
To build OpenDAL C binding, the tools you need are very simple.

The following is all you need
- **A C++ compiler** that supports **c++14**, *e.g.* clang++ and g++
- **A Rust toolchain** that satisfies the MSRV
- **(optional)** if you want to format the code, you need to install **clang-format**
    - The `opendal.h` is not formatted by hands when you contribute, please do not format the file. **Use `make format` only.**
    - If your contribution is related to the files under `./tests`, you may format it before submitting your pull request. But notice that different versions of `clang-format` may format the files differently.
- **(optional)** **GTest(Google Test)** need to be installed if you want to build the BDD (Behavior
  Driven Development) tests. To see how to build, check [here](https://github.com/google/googletest).

## Build
To build the library and header file.
```shell
make build
```

- The header file `opendal.h` is under `./include` 
- The library is under `../../target/debug` after building.

To clean the build results.
```shell
make clean
```

## Test
To build and run the tests. (Note that you need to install GTest)
```shell
make test
```

```text
[==========] Running 5 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 5 tests from OpendalBddTest
[ RUN      ] OpendalBddTest.Write
[       OK ] OpendalBddTest.Write (4 ms)
[ RUN      ] OpendalBddTest.Exist
[       OK ] OpendalBddTest.Exist (0 ms)
[ RUN      ] OpendalBddTest.EntryMode
[       OK ] OpendalBddTest.EntryMode (0 ms)
[ RUN      ] OpendalBddTest.ContentLength
[       OK ] OpendalBddTest.ContentLength (0 ms)
[ RUN      ] OpendalBddTest.Read
[       OK ] OpendalBddTest.Read (0 ms)
[----------] 5 tests from OpendalBddTest (4 ms total)

[----------] Global test environment tear-down
[==========] 5 tests from 1 test suite ran. (4 ms total)
[  PASSED  ] 5 tests.
```

## Docs
The OpenDAL C binding currently do not have an official documentation online(Open to contribution).

If you want to know more about the OpenDAL C binding, you may:
- Have a look at the comments inline with Rust codes under the `./src` or the comments in `opendal.h`
- Have a look at the comments of the Rust core
- Have a look at the test files, they contain compact and simple usage of OpenDAL C binding

## Misc
- We use `cbindgen` to generate the header file and library. If you want to contribute to the C binding core code, you may see the official [documentation](https://github.com/mozilla/cbindgen/blob/master/docs.md) of `cbindgen`.
- We have github action that shows how to setup the building and testing environment, if you are still not clear how to setup your environment, you may see [this github action](../../.github/workflows/bindings_c.yml).
