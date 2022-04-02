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

## Quickstart

```rust
use anyhow::Result;
use futures::AsyncReadExt;
use futures::StreamExt;
use opendal::services::fs;
use opendal::ObjectMode;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);

    let o = op.object("test_file");

    // Write data info file;
    let _ = o.write("Hello, World!").await?;

    // Read data from file;
    let bs = o.read().await?;

    // Read range from file;
    let bs = o.range_read(1..=11).await?;

    // Get file's Metadata
    let meta = o.metadata().await?;

    // List dir.
    let mut obs = op.objects("").await?.map(|o| o.expect("list object"));
    while let Some(o) = obs.next().await {
        let meta = o.metadata().await?;
        let path = meta.path();
        let mode = meta.mode();
        let length = meta.content_length();
    }

    // Delete file.
    o.delete().await?;

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
