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

To build OpenDAL D binding locally, you need:

- [dmd/ldc/gdc](https://dlang.org/download)


## Build

First, build the C bindings:

```shell
dub build -b release
```

> **Note**:
>
> - `dub build` adds the header file `opendal.h` under `../c/include`
> - The library is under `../../target/debug` or `../../target/release` after building.

## Test

To build and run the tests.

```shell
$ dub test
  Generating test runner configuration 'opendal-test-unittest' for 'unittest' (library).
    Starting Performing "unittest" build using /usr/bin/ldc2 for x86_64.
    Building opendal ~master: building configuration [opendal-test-unittest]
   Pre-build Running commands
    Finished `release` profile [optimized] target(s) in 0.08s
Cargo build completed successfully
     Linking opendal-test-unittest
     Running opendal-test-unittest 
Basic Operator creation and write test passed
1 modules passed unittests
```


