# Contributing
1. [Contributing](#contributing)
   1. [Setup](#setup)
      1. [Using a dev container environment](#using-a-dev-container-environment)
      2. [Bring your own toolbox](#bring-your-own-toolbox)
   2. [Prepare](#prepare)
   3. [Build](#build)
   4. [Test](#test)
   5. [Docs](#docs)

## Setup

### Using a dev container environment

### Bring your own toolbox
To build OpenDAL C binding, the tools you need it very simple.
All you need is
- A C++ compiler that supports c++14
- A Rust toolchain that satifies the MSRV
- (optional) if you want to format the code, you need to install clang-format
    - The `opendal.h` is not formatted by hands when you contribute, please do not format the file
    - If your contribution is related to the files under `./tests`, you may format it before submitting
      your Pull Request. But notice that different versions of `clang-format` may format the files
      differently.

## Prepare

## Build
To build the library and header file.
```shell
make build
```
The header file `opendal.h` is under `./include` and the library is under `../../target/debug` after
building.

To clean the build results.
```shell
make clean
```

## Test
To build and run the tests.
```shell
make test
```

## Docs
The OpenDAL C binding currently do not have a documentation online(Open to contribution).
If you want to know more about the C binding, you may:
- Have a look at the comments inline with Rust codes under the `./src` or the comments in `opendal.h`
- Have a look at the comments of the Rust core
- Have a look at the test files, they contain compact and simple usage of OpenDAL C binding

