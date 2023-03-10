# OpenDAL Python Binding

This crate intends to build a native python binding.

## Installation

```bash
pip install opendal
```

## Usage

```python
import opendal

op = opendal.Operator("fs", root="/tmp")
op.write("test.txt", b"Hello World")
print(op.read("test.txt"))
print(op.stat("test.txt").content_length)
```

Or using the async API:

```python
import asyncio

async def main():
    op = opendal.AsyncOperator("fs", root="/tmp")
    await op.write("test.txt", b"Hello World")
    print(await op.read("test.txt"))

asyncio.run(main())
```

## Development

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
