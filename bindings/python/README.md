# Apache OpenDAL™ Python Binding

[![](https://img.shields.io/badge/status-released-blue)](https://pypi.org/project/opendal/)
[![PyPI](https://img.shields.io/pypi/v/opendal.svg?logo=PyPI)](https://pypi.org/project/opendal/)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/python/)

This package intends to build a native python binding for Apache OpenDAL.

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Useful Links

- [Documentation](https://opendal.apache.org/docs/python/)
- [Examples](./examples)
- [Upgrade Guide](./upgrade.md)

## Installation

```bash
pip install opendal
```

## Usage

fs service example:
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

s3 service example:
```python
import opendal

op = opendal.Operator("s3", root="/tmp", bucket="your_bucket_name", region="your_region")
op.write("test.txt", b"Hello World")
print(op.read("test.txt"))
print(op.stat("test.txt").content_length)
```

Or using the async API:

```python
import asyncio

async def main():
    op = opendal.AsyncOperator("s3", root="/tmp", bucket="your_bucket_name", region="your_region")
    await op.write("test.txt", b"Hello World")
    print(await op.read("test.txt"))

asyncio.run(main())
```


## Development

Setup virtualenv:

```shell
uv venv --python 3.10
```

Install all the dependencies:

```shell
uv sync --all-groups --all-extras
```

Run some tests:

```shell
# To run `test_write.py` and use `fs` operator
OPENDAL_TEST=fs OPENDAL_FS_ROOT=/tmp uv run pytest -vk test_write
```

Build API docs:

```shell
uv run pdoc -t ./template opendal
```

## Used by

Check out the [users](./users.md) list for more details on who is using OpenDAL.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
