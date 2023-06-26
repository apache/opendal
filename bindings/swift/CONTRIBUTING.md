# Contributing

- [Contributing](#contributing)
  - [Setup](#setup)
    - [Using a dev container environment](#using-a-dev-container-environment)
    - [Bring your own toolbox](#bring-your-own-toolbox)
  - [Build](#build)
  - [Test](#test)

## Setup

### Using a dev container environment

OpenDAL provides a pre-configured [dev container](https://containers.dev/) that could be used in [Github Codespaces](https://github.com/features/codespaces), [VSCode](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/), [JuptyerLab](https://jupyterlab.readthedocs.io/en/stable/). Please pick up your favourite runtime environment.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/incubator-opendal?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox

To build OpenDAL Swift binding, the Swift toolchain is required to be installed.

For Ubuntu and CentOS, you may follow the instructions in [this documentation](https://www.swift.org/download) to install it. Make sure that `swift` binary is added to your path:

```shell
export PATH=/path/to/swift-toolchain/usr/bin:"${PATH}"
```

For macOS, you can install Xcode or Xcode command line tools to get your environment all set.

Then you can validate your Swift installation by executing `swift --version`.

```text
$ swift --version

Swift version 5.8.1 (swift-5.8.1-RELEASE)
Target: x86_64-unknown-linux-gnu
```

For better development experience, we recommend you to configure your editors to use SourceKit-LSP (the LSP for Swift). Visit SourceKit-LSP [repository](https://github.com/apple/sourcekit-lsp) for more details.

## Build

To build the binding package:

```shell
$ make
```

Swift packages are not released via binary artifacts typically. However, you can still build the package locally to check whether there are compilation errors.

To clean the build results:

```shell
$ make clean
```

## Test

To build and run the tests:

```text
$ make test

Test Suite 'All tests' started at xxx.
Test Suite 'OpenDALPackageTests.xctest' started at xxx.
Test Suite 'OpenDALTests' started at xxx.
Test Case '-[OpenDALTests.OpenDALTests testSimple]' started.
Test Case '-[OpenDALTests.OpenDALTests testSimple]' passed (xx seconds).
Test Suite 'OpenDALTests' passed at xxx.
         Executed 1 test, with 0 failures (0 unexpected) in xx (xx) seconds
Test Suite 'OpenDALPackageTests.xctest' passed at xxx.
         Executed 1 test, with 0 failures (0 unexpected) in xx (xx) seconds
Test Suite 'All tests' passed at xxx.
         Executed 1 test, with 0 failures (0 unexpected) in xx (xx) seconds
```
