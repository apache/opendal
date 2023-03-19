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

Build bindings:

```shell
just build_bindings_python
```

Running some tests:

```shell
just test_bindings_python
```
