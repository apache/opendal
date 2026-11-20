---
title: Python
sidebar_label: Overview
slug: /bindings/python
description: The OpenDAL Python binding — install it, see what it supports, and where to go next.
---

# Apache OpenDAL™ Python Binding

A native Python binding for OpenDAL with one API for S3, GCS, Azure Blob,
the local filesystem, and other [storage services](/services).

New to the model behind the API? Read [Concepts](../../03-concepts.mdx) first —
the shared storage semantics and path and operation concepts every binding
expresses through its own native API.

:::note
This binding has its own version number, independent of the Rust core. Check
compatibility against the binding's version, not the core's.
:::

## Status

Released and stable, published to [PyPI](https://pypi.org/project/opendal/),
[Anaconda main](https://anaconda.org/main/opendal), and
[conda-forge](https://anaconda.org/conda-forge/opendal).

## Capabilities

- **Sync and async** — use [`Operator`] or [`AsyncOperator`] with the same verbs.
- **File-like objects** — `op.open(path, "rb")` returns a familiar
  read/write/seek file you can stream through.
- **Services** — PyPI wheels include S3, GCS, and the other services in the
  [release feature group](./02-getting-started.md#build-with-additional-services).
- **Presigned URLs** for services that support them (async API).

## Installation

Install from PyPI:

```shell
pip install opendal
```

Or install from Anaconda main:

```shell
conda install opendal
```

Or install from conda-forge:

```shell
conda install conda-forge::opendal
```

## Useful Links

- **API reference**: [opendal.apache.org/docs/python](https://opendal.apache.org/docs/python/)
- **Services & configuration**: [/services](/services)
- **Upgrade guide**: [`upgrade.md`](https://github.com/apache/opendal/blob/main/bindings/python/upgrade.md)
- **Source & examples**: [`bindings/python/`](https://github.com/apache/opendal/tree/main/bindings/python)

## Next steps

1. [Getting started](./02-getting-started.md) — a runnable program, then a real backend.
2. [Connecting to your storage](./03-connecting.md) — build an operator for any service.
3. [Common tasks](./04-tasks.md) — read, write, stream, list, presign, and more.
4. [Going to production](./05-production.md) — retries, errors, and capability checks.

[`Operator`]: https://opendal.apache.org/docs/python/api/operator/#opendal.Operator
[`AsyncOperator`]: https://opendal.apache.org/docs/python/api/async_operator/#opendal.AsyncOperator
