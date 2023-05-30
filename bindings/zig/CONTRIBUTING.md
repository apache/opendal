# Contributing
- [Contributing](#contributing)
   - [Setup](#setup)
      - [Using a dev container environment](#using-a-dev-container-environment)
      - [Bring your own toolbox](#bring-your-own-toolbox)
   - [Build](#build)
   - [Test](#test)
   - [Docs](#docs)
   - [Misc](#misc)

## Setup

### Using a dev container environment
OpenDAL provides a pre-configured [dev container](https://containers.dev/) that could be used in [Github Codespaces](https://github.com/features/codespaces), [VSCode](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/), [JuptyerLab](https://jupyterlab.readthedocs.io/en/stable/). Please pick up your favourite runtime environment.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/incubator-opendal?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox
To build OpenDAL Zig binding, the following is all you need:
- **Zig toolchain** is based on **LLVM-toolchain**(clang/++, libcxx, lld).

## Build
To build the library and header file.
```shell
zig build
```

- zig build need add header file `opendal.h` is under `../c/include` 
- The library is under `../../target/debug` or `../../target/release` after building.

To clean the build results.
```shell
zig build uninstall
```

## Test
To build and run the tests. (Note that you need to install GTest)
```shell
zig build test -fsummary
```

```text
Build Summary: 3/3 steps succeeded; 1/1 tests passed
test success
└─ run test 1 passed 960us MaxRSS:3M
   └─ zig test Debug native success 982ms MaxRSS:164M
```

