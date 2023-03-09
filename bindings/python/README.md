# OpenDAL Python Binding

This crate intends to build a native python binding.

## Building

Install `maturin`:

```shell
pip install maturin
```

Setup virtualenv:

```shell
virtualenv venv
```

Activate venv:

```shell
source venv/bin/activate
````

Build bindings:

```shell
maturin develop
```

Running some tests:

```shell
pip install -r test_requirements.txt
pytest
```
