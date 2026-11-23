---
title: Common tasks
sidebar_label: Common tasks
description: Task-oriented recipes for OpenDAL in Python — read, write, stream, list, delete, copy, and presign.
---

# Common tasks

These examples assume an `op = opendal.Operator(...)` built as in
[Getting started](./02-getting-started.md). For full signatures, see the
[API reference](https://opendal.apache.org/docs/python/).

A few conventions used throughout:

- Paths are relative to the operator's root; a trailing `/` means a directory
  (`logs/app/` is a directory, `logs/app` is a file).
- `read` returns `bytes`; `write` takes `bytes`.
- The async API mirrors these — use `opendal.AsyncOperator` and `await` each call.

## Read a whole file

```python
data = op.read("path/to/file")     # bytes
text = data.decode()
```

## Read part of a file

```python
data = op.read("path/to/file", offset=0, size=1024)
```

## Stream a large file

Don't load gigabytes into memory — open a file-like object and read in chunks:

```python
with op.open("big.bin", "rb") as f:
    while chunk := f.read(8192):
        ...  # process chunk (bytes)
```

## Write a whole file

```python
op.write("path/to/file", b"Hello, World!")
```

## Stream a large upload

Use `shutil.copyfileobj` to stream a local file to a service that supports
incremental writes. Use the same chunk size for local reads and OpenDAL writes.

```python
import shutil

chunk_size = 8 * 1024 * 1024  # 8 MiB

with open("big.bin", "rb") as src:
    with op.open("uploads/big.bin", "wb", chunk=chunk_size, concurrent=8) as dst:
        shutil.copyfileobj(src, dst, length=chunk_size)
```

`length` controls how many bytes `copyfileobj` reads at a time. `chunk` sets the
OpenDAL write chunk size, subject to service limits. On services that support
parallel uploads, lowering `concurrent` reduces the number of chunks in flight
and can lower peak memory use at the cost of throughput.

The destination closes when the `with` block exits, even when reading the local
file raises an exception. Closing can commit the bytes already written.

## Upload concurrently

For large objects on services with multipart support, upload parts in parallel:

```python
op.write("big.bin", data, concurrent=8)
```

## Check existence and metadata

```python
if op.exists("path/to/file"):
    meta = op.stat("path/to/file")
    print(meta.content_length, meta.is_dir)
```

## List a directory

`list` yields the entries under a path:

```python
for entry in op.list("dir/"):
    print(entry.path, entry.metadata.mode)
```

## Walk a tree recursively

```python
for entry in op.list("dir/", recursive=True):
    print(entry.path)
```

## Delete a file or a whole tree

```python
op.delete("path/to/file")    # single path; succeeds even if missing
op.remove_all("a/")
```

Pass a trailing slash to limit recursive deletion to the directory. On
prefix-based services, `"a"` can also match entries under `"ab/"`.

## Create a directory

```python
op.create_dir("path/to/dir/")   # the trailing slash is required
```

## Copy and rename

```python
op.copy("from.txt", "to.txt")
op.rename("old.txt", "new.txt")
```

Both require a service that supports them; see
[capability checks](./05-production.md#capability-checks).

## Generate a presigned URL

Hand a time-limited URL to a third party so they can access an object without
your credentials. Presigning is part of the **async** API:

```python
import asyncio
import opendal

async def main():
    op = opendal.AsyncOperator("s3", bucket="my-bucket", region="us-east-1")
    req = await op.presign_read("path/to/file", 3600)  # expires in 3600s
    print(req.method, req.url)
    print(req.headers)

asyncio.run(main())
```
