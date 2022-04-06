# OpenDAL &emsp; [![Build Status]][actions] [![Latest Version]][crates.io]

[Build Status]: https://img.shields.io/github/workflow/status/datafuselabs/opendal/CI/main
[actions]: https://github.com/datafuselabs/opendal/actions?query=branch%3Amain
[Latest Version]: https://img.shields.io/crates/v/opendal.svg
[crates.io]: https://crates.io/crates/opendal

**Open **D**ata **A**ccess **L**ayer that connect the whole world together.**

---

You may be looking for:

- [Examples](./examples)
- [API documentation](https://opendal.databend.rs/opendal/)
- [Release notes](https://github.com/datafuselabs/opendal/releases)

## Status

OpenDAL is in **alpha** stage and has been early adopted by [databend](https://github.com/datafuselabs/databend/). Welcome any feedback at [Discussions](https://github.com/datafuselabs/opendal/discussions)!

## Supported Services

- [fs](https://docs.rs/opendal/latest/opendal/services/fs/index.html): POSIX alike file system.
- [memory](https://docs.rs/opendal/latest/opendal/services/memory/index.html): In memory backend support.
- [s3](https://docs.rs/opendal/latest/opendal/services/s3/index.html): AWS S3 alike services.

## Quickstart

```rust
use anyhow::Result;
use futures::StreamExt;
use opendal::services::fs;
use opendal::ObjectMode;
use opendal::Operator;
use opendal::Metadata;
use opendal::Object;
use opendal::ObjectStreamer;

#[tokio::main]
async fn main() -> Result<()> {
    // Init Operator
    let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);

    // Create object handler.
    let o: Object = op.object("test_file");

    // Write data info object;
    let _: () = o.write("Hello, World!").await?;

    // Read data from object;
    let bs: Vec<u8> = o.read().await?;

    // Read range from object;
    let bs: Vec<u8> = o.range_read(1..=11).await?;

    // Get object's Metadata
    let meta: Metadata = o.metadata().await?;
    let path: &str = meta.path();
    let mode: ObjectMode = meta.mode();
    let length: u64 = meta.content_length();
    let content_md5: Option<String> = meta.content_md5();

    // Delete object.
    let _: () = o.delete().await?;

    // List dir object.
    let o: Object = op.object("test_dir/");
    let mut obs: ObjectStreamer = o.list().await?;
    while let Some(entry) = obs.next().await {
        let entry: Object = entry?;
    }

    Ok(())
}
```

More examples could be found at [examples](./examples).

## Contributing

Check out the [CONTRIBUTING.md](./CONTRIBUTING.md) guide for more details on getting started with contributing to this project.

## Getting help

Submit [issues](https://github.com/datafuselabs/opendal/issues/new/choose) for bug report or asking questions in [discussion](https://github.com/datafuselabs/opendal/discussions/new?category=q-a). 

#### License

<sup>
Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.
</sup>
