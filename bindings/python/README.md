# Apache OpenDAL™ Python Binding

[![Status](https://img.shields.io/badge/status-released-blue)](https://pypi.org/project/opendal/)
[![PyPI](https://img.shields.io/pypi/v/opendal.svg?logo=PyPI)](https://pypi.org/project/opendal/)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/bindings/python)

This package provides a native Python binding for **Apache OpenDAL™**, a data access
layer that allows you to access various storage services in a unified way.

![OpenDAL Python Usage Demo](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

> **Note**: This binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to this binding's version rather than the core version.

## Useful Links

- **User guide**: [opendal.apache.org/docs/bindings/python](https://opendal.apache.org/docs/bindings/python) — install, connect, common tasks, and going to production.
- **API reference**: [opendal.apache.org/docs/python](https://opendal.apache.org/docs/python/)
- **Services & configuration**: [opendal.apache.org/services](https://opendal.apache.org/services)
- **Upgrade guide**: [`upgrade.md`](./upgrade.md)
- **Examples**: [`docs/examples`](./docs/examples)

## Installation

```bash
pip install opendal
```

## Quickstart

```python
import opendal

# Configure a service, then build an operator from it.
op = opendal.Operator("fs", root="/tmp")

# The same verbs work on every service.
op.write("test.txt", b"Hello World")
print(op.read("test.txt"))
print(op.stat("test.txt").content_length)
```

To use a real backend, change the scheme and pass its configuration — the
operations stay identical:

```python
op = opendal.Operator("s3", bucket="your_bucket", region="your_region")
```

OpenDAL also has a first-class async API via `opendal.AsyncOperator`. See
[Getting started](https://opendal.apache.org/docs/bindings/python/getting-started)
and [Connecting to your storage](https://opendal.apache.org/docs/bindings/python/connecting)
for the full guide.

## Contributing

This project uses [`just`](https://github.com/casey/just) as a command runner.
For a complete guide on building, testing, and contributing, see
**[CONTRIBUTING.md](./CONTRIBUTING.md)**.

## Used By

Check out the [users list](./users.md) for more details on who is using OpenDAL.

## License and Trademarks

Licensed under the Apache License, Version 2.0:
http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of
the Apache Software Foundation.
