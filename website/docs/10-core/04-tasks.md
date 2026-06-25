---
title: Common tasks
sidebar_label: Common tasks
description: Task-oriented recipes for OpenDAL in Rust — read, write, stream, list, delete, copy, and presign.
---

# Common tasks

Recipes for the things you actually do with storage. Each works on every
service. They assume an `op: Operator` built as in
[Getting started](./02-getting-started.md). For full method signatures and
options, follow the links to the [API reference][operator].

A few conventions used throughout:

- Paths are relative to the operator's root; a trailing `/` means a directory
  (`logs/app/` is a directory, `logs/app` is a file).
- Reads return a [`Buffer`]; writes accept anything that converts into one
  (`&str`, `String`, `Vec<u8>`, `Bytes`).
- Most verbs have a `*_with` companion (`read_with`, `write_with`, …) for extra
  options like ranges, content type, and concurrency.

## Read a whole file

```rust
let bytes = op.read("path/to/file").await?;
let text = String::from_utf8(bytes.to_vec())?;
```

## Read part of a file

```rust
let bytes = op.read_with("path/to/file").range(0..1024).await?;
```

## Stream a large file

Don't buffer gigabytes in memory — stream chunks instead:

```rust
use futures::TryStreamExt;

let mut stream = op.reader("big.bin").await?.into_bytes_stream(..).await?;
while let Some(chunk) = stream.try_next().await? {
    // process chunk (bytes::Bytes)
}
```

## Write a whole file

```rust
let _meta = op.write("path/to/file", "Hello, World!").await?;
let _meta = op.write("path/to/file", vec![0u8; 1024]).await?;
```

## Stream a large upload

Use a [`Writer`] for data produced incrementally. Call `write` repeatedly, then
`close` to commit (use `abort` to discard):

```rust
let mut writer = op.writer("big.bin").await?;
writer.write(first_chunk).await?;
writer.write(second_chunk).await?;
let _meta = writer.close().await?;
```

## Upload concurrently

For large objects on services with multipart support, upload parts in parallel:

```rust
let mut writer = op.writer_with("big.bin").concurrent(8).await?;
writer.write(data).await?;
writer.close().await?;
```

## Check existence and metadata

```rust
if op.exists("path/to/file").await? {
    let meta = op.stat("path/to/file").await?;
    println!("{} bytes, dir = {}", meta.content_length(), meta.is_dir());
}
```

## List a directory

`list` returns the direct children of a directory:

```rust
for entry in op.list("dir/").await? {
    println!("{} ({:?})", entry.path(), entry.metadata().mode());
}
```

For large directories, stream entries with a [`Lister`] instead of collecting
them into a `Vec`:

```rust
use futures::TryStreamExt;

let mut lister = op.lister("dir/").await?;
while let Some(entry) = lister.try_next().await? {
    println!("{}", entry.path());
}
```

## Walk a tree recursively

```rust
let entries = op.list_with("dir/").recursive(true).await?;
```

`lister_with("dir/").recursive(true)` gives the streaming equivalent.

## Delete a file or a whole tree

```rust
op.delete("path/to/file").await?;   // single path; idempotent
op.remove_all("dir/").await?;       // a path and everything under it
```

`delete` succeeds even if the path does not exist.

## Create a directory

```rust
op.create_dir("path/to/dir/").await?;   // the trailing slash is required
```

## Copy and rename

```rust
let _meta = op.copy("from.txt", "to.txt").await?;
op.rename("old.txt", "new.txt").await?;
```

Both operate within a single operator and require a service that supports them;
see [capability checks](./05-production.md#capability-checks).

## Generate a presigned URL

Hand a time-limited URL to a third party so they can access an object without
your credentials:

```rust
use std::time::Duration;

let req = op.presign_read("path/to/file", Duration::from_secs(3600)).await?;
// req.method(), req.uri(), req.header() describe the HTTP request to make
```

[operator]: https://docs.rs/opendal/latest/opendal/struct.Operator.html
[`Buffer`]: https://docs.rs/opendal/latest/opendal/struct.Buffer.html
[`Writer`]: https://docs.rs/opendal/latest/opendal/struct.Writer.html
[`Lister`]: https://docs.rs/opendal/latest/opendal/struct.Lister.html
