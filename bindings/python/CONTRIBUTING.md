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

> [!TIP]
> We recommend to use [`uv`](https://github.com/astral-sh/uv) to manage the Python packages and environments. You can install it by `curl -LsSf https://astral.sh/uv/install.sh | sh`.

## Prepare

All operations were performed within a Python virtual environment (venv) to prevent conflicts with the system's Python environment or other project venvs.

OpenDAL specify the `requires-python` in `pyproject.toml` as `>= 3.10`. You can use `uv venv --python 3.10` to setup the virtualenv for development.

To get ready for the development, kindly install all the required dependencies:

```shell
uv sync --all-groups --all-extras
```

## Build

To build python binding only:

```shell
uv build
```

Note that `uv` will detect the related files and re-build this package when necessary.

> [!NOTE]
> If you want to have a full-featured opendal, you can run `uv build` and install the
> wheel file in the `dist` directory.

## Test

OpenDAL adopts `pytest` for behavior tests:

```shell
# To run `test_write.py` and use `fs` operator
OPENDAL_TEST=fs OPENDAL_FS_ROOT=/tmp uv run pytest -vk test_write
```

## Docs

Build API docs:

```shell
uv run pdoc opendal
```
