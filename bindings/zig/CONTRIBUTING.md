# Contributing

- [Contributing](#contributing)
  - [Setup](#setup)
    - [Using a dev container environment](#using-a-dev-container-environment)
    - [Bring your own toolbox](#bring-your-own-toolbox)
  - [Build](#build)
  - [Test](#test)

## Setup

### Using a dev container environment

OpenDAL provides a pre-configured [dev container](https://containers.dev/) that could be used in [GitHub Codespaces](https://github.com/features/codespaces), [VSCode](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/), [JupyterLab](https://jupyterlab.readthedocs.io/en/stable/). Please pick up your favourite runtime environment.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/opendal?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox

To build OpenDAL Zig binding locally, you need:

- [Zig](https://ziglang.org/download) 0.11.0 or higher

> **Note**:
>
> 0.11.0 is not released yet. You can use master instead before the official 0.11.0 released.

## Build

First, build the C bindings:

```shell
zig build libopendal_c
```

> **Note**:
>
> - `zig build` adds the header file `opendal.h` under `../c/include`
> - The library is under `../../target/debug` or `../../target/release` after building.

## Test

To build and run the tests.

```shell
zig build test -fsummary
```

```text
Build Summary: 3/3 steps succeeded; 1/1 tests passed
test success
└─ run test 1 passed 960us MaxRSS:3M
   └─ zig test Debug native success 982ms MaxRSS:164M
```

