# Contributing

First off, thank you for considering contributing to Apache OpenDAL! Your help is
greatly appreciated. This guide will provide you with all the information you need to
get started.

---

## Prerequisites

Before you begin, please ensure you have the following tools installed:

- **Git**: For version control.
- **Python**: Version 3.10 or newer.
- **Just**: A command runner used to simplify our development workflow. You can install
  it with:
  ```shell
  # macOS
  brew install just

  # Windows
  choco install just

  # Or with cargo (if you have Rust installed)
  cargo install just
  ```

---

## Getting Started: Setup

All development commands are managed through a `justfile`. Setting up your environment
is as simple as running one command.

1. **Clone the repository:**
   ```shell
   git clone [https://github.com/apache/opendal.git](https://github.com/apache/opendal.git)
   cd opendal/bindings/python
   ```

2. **Set up the virtual environment and install dependencies:**
   ```shell
   just setup
   ```
   This command will create a Python virtual environment in the `.venv` directory and
   install all necessary development and runtime dependencies using `uv`.

---

## Development Workflow

The `justfile` provides recipes for common development tasks. You can see all available
commands by running `just --list`.

### Building the Project

You can build a development or release version of the library.

- **Install for development:** This builds the package and installs it in the current
  virtual environment, allowing you to immediately use it for testing.
  ```shell
  just install-dev
  ```

- **Build a distributable wheel:** If you need to create a wheel file for distribution.
  ```shell
  # Build a development wheel
  just build-dev

  # Build an optimized release wheel
  just build-release
  ```
  The built wheel will be located in the `dist/` directory.

### Running Tests

We use `pytest` for testing. The `test` recipe runs all tests within the virtual
environment.

```shell
# Run all tests
just test

# Pass arguments directly to pytest
# For example, run tests for the 'fs' operator matching 'test_write'
OPENDAL_TEST=fs OPENDAL_FS_ROOT=/tmp just test -vk test_write
```

### Code Quality: Formatting & Linting

We use `ruff` for Python linting/formatting and `cargo fmt`/`clippy` for Rust. `just`
makes it easy to ensure your code meets our quality standards.

- **Format all code:**

  ```shell
  just fmt
  ```

- **Check for linting errors:**

  ```shell
  just lint
  ```

- **Automatically fix what can be fixed:**

  ```shell
  just fix
  ```

> **Important\!** Before committing your changes, please run the pre-commit checks to
> ensure everything is formatted and linted correctly.
>
> ```shell
> just pre-commit
> ```

### Building Documentation

The API documentation is built using `mkdocs`. This is automatically handled when you
build the project.

```shell
# Building the project also builds the docs
just build-dev
```

### Cleaning the Workspace

If you need to start fresh, the `clean` command will remove all build artifacts, caches,
and the virtual environment.

```shell
just clean
```

This command removes the `.venv`, build artifacts, and Python cache files.

---

Thank you again for your contribution\!
