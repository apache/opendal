# Contributing

- [Contributing](#contributing)
  - [Setup](#setup)
    - [Using a dev container environment](#using-a-dev-container-environment)
    - [Bring your own toolbox](#bring-your-own-toolbox)
  - [Prepare](#prepare)
  - [Build](#build)
  - [Test](#test)
  - [Docs](#docs)

## Setup

Building `python` bindings requires some extra setup.

For small or first-time contributions, we recommend the dev container method. Prefer to do it yourself? That's fine too!

### Using a dev container environment

OpenDAL provides a pre-configured [dev container](https://containers.dev/) that could be used in [GitHub Codespaces](https://github.com/features/codespaces), [VSCode](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/), [JupyterLab](https://jupyterlab.readthedocs.io/en/stable/). Please pick up your favourite runtime environment.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/opendal?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox

The `python` binding requires `Python` to be built. We recommend using the latest stable version for development.

Most operating systems and distributions already have Python installed. If not, please install Python and its development tools first.

For Ubuntu and Debian:

```shell
sudo apt install -y python3-dev python3-pip python3-venv
```

## Prepare

All operations were performed within a Python virtual environment (venv) to prevent conflicts with the system's Python environment or other project venvs.

OpenDAL specify the `requires-python` in `pyproject.toml` as `>= 3.10`. You can use `python -m venv venv` to setup virtualenv to start development.

After `venv` has been prepared, you can activate it by `source venv/bin/activate`.

To simplify our work, we will utilize the tool [`maturin`](https://github.com/PyO3/maturin). Kindly install it beforehand.

```shell
pip install maturin
```

## Build

To build python binding only:

```shell
maturin build
```

To build and install python binding directly in the current virtualenv:

```shell
maturin develop
```

Note: `maturin develop` will be faster, but doesn't support all the features. In most development cases, we recommend using `maturin develop`.

## Test

OpenDAL adopts `pytest` for behavior tests:

```shell
maturin develop -E test
# To run `test_write.py` and use `fs` operator
OPENDAL_TEST=fs OPENDAL_FS_ROOT=/tmp pytest -vk test_write
```

## Docs

Build API docs:

```shell
maturin develop -E docs
pdoc opendal
```
