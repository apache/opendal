# OpenDAL &emsp; [![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io]

[Build Status]: https://img.shields.io/github/workflow/status/datafuselabs/opendal/CI/main
[actions]: https://github.com/datafuselabs/opendal/actions?query=branch%3Amain
[Latest Version]: https://img.shields.io/crates/v/opendal.svg
[crates.io]: https://crates.io/crates/opendal
[Crate Downloads]: https://img.shields.io/crates/d/opendal.svg

**Open** **D**ata **A**ccess **L**ayer: Access data freely, painless, and efficiently

---

You may be looking for:

- [Documentation](https://opendal.databend.rs)
- [API Reference](https://opendal.databend.rs/opendal/)
- [Release notes](https://github.com/datafuselabs/opendal/releases)

## Services

- [azblob](https://docs.rs/opendal/latest/opendal/services/azblob/index.html): Azure Storage Blob services.
- [fs](https://docs.rs/opendal/latest/opendal/services/fs/index.html): POSIX alike file system.
- [hdfs](https://docs.rs/opendal/latest/opendal/services/hdfs/index.html): Hadoop Distributed File System(HDFS).
- [http](https://docs.rs/opendal/latest/opendal/services/http/index.html): HTTP read-only services.
- [memory](https://docs.rs/opendal/latest/opendal/services/memory/index.html): In memory backend.
- [s3](https://docs.rs/opendal/latest/opendal/services/s3/index.html): AWS S3 alike services.

## Features

- Access different storage system in the same way
- Native decompress support
- Native service-side encryption support
- Powerful [`Layer`](https://docs.rs/opendal/latest/opendal/trait.Layer.html)
- **100%** documents covered
- Behavior tests for all services

## Quickstart

```rust
use anyhow::Result;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::DirEntry;
use opendal::DirStreamer;
use opendal::Object;
use opendal::ObjectMetadata;
use opendal::ObjectMode;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // Init Operator
    let op = Operator::from_env(Scheme::Fs)?;

    // Create object handler.
    let o = op.object("test_file");

    // Write data info object;
    o.write("Hello, World!").await?;

    // Read data from object;
    let bs = o.read().await?;

    // Read range from object;
    let bs = o.range_read(1..=11).await?;

    // Get object's path
    let name = o.name();
    let path = o.path();

    // Fetch more meta about object.
    let meta = o.metadata().await?;
    let mode = meta.mode();
    let length = meta.content_length();
    let content_md5 = meta.content_md5();
    let etag = meta.etag();

    // Delete object.
    o.delete().await?;

    // List dir object.
    let o = op.object("test_dir/");
    let mut ds = o.list().await?;
    while let Some(entry) = ds.try_next().await? {
        let path = entry.path();
        let mode = entry.mode();
    }

    Ok(())
}
```

More examples could be found at [Documentation](https://opendal.databend.rs).

## Projects

- [databend](https://github.com/datafuselabs/databend/): A modern Elasticity and Performance cloud data warehouse.

## Contributing

Check out the [CONTRIBUTING.md](./CONTRIBUTING.md) guide for more details on getting started with contributing to this project.

## Getting help

Submit [issues](https://github.com/datafuselabs/opendal/issues/new/choose) for bug report or asking questions in [discussion](https://github.com/datafuselabs/opendal/discussions/new?category=q-a). 

#### License

<sup>
Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.
</sup>
