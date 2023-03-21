# OpenDAL &emsp; [![Build Status]][actions] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/incubator-opendal/ci.yml?branch=main
[actions]: https://github.com/apache/incubator-opendal/actions?query=branch%3Amain
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://discord.gg/XQy8yGR2dg

**Open** **D**ata **A**ccess **L**ayer: Access data freely, painlessly, and efficiently

![](https://user-images.githubusercontent.com/5351546/222356748-14276998-501b-4d2a-9b09-b8cff3018204.png)

## Components

- [core](core): OpenDAL Rust Core
  - Documentation: [stable](https://docs.rs/opendal/) | [main](https://opendal.apache.org/docs/rust/opendal/)
- [binding-python](bindings/python): OpenDAL Python Binding
  - Documentation: [main](https://opendal.apache.org/docs/python/)
- [binding-nodejs](bindings/nodejs): OpenDAL Node.js Binding
  - Documentation: [main](https://opendal.apache.org/docs/nodejs/)
- binaries
  - [oli](bin/oli): OpenDAL Command Line Interface

## Quickstart

### Rust

```rust
use opendal::Result;
use opendal::layers::LoggingLayer;
use opendal::services;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Pick a builder and configure it.
    let mut builder = services::S3::default();
    builder.bucket("test");

    // Init an operator
    let op = Operator::new(builder)?
        // Init with logging layer enabled.
        .layer(LoggingLayer::default())
        .finish();

    // Write data
    op.write("hello.txt", "Hello, World!").await?;

    // Read data
    let bs = op.read("hello.txt").await?;

    // Fetch metadata
    let meta = op.stat("hello.txt").await?;
    let mode = meta.mode();
    let length = meta.content_length();

    // Delete
    op.delete("hello.txt").await?;

    Ok(())
}
```

### Python

```python
import asyncio

async def main():
    op = opendal.AsyncOperator("fs", root="/tmp")
    await op.write("test.txt", b"Hello World")
    print(await op.read("test.txt"))

asyncio.run(main())
```

### Node.js

```js
import { Operator } from "opendal";

async function main() {
  const op = new Operator("fs", { root: "/tmp" });
  await op.write("test", "Hello, World!");
  const bs = await op.read("test");
  console.log(new TextDecoder().decode(bs));
  const meta = await op.stat("test");
  console.log(`contentLength: ${meta.contentLength}`);
}
```

## Projects

- [Databend](https://github.com/datafuselabs/databend/): A modern Elasticity and Performance cloud data warehouse.
- [GreptimeDB](https://github.com/GreptimeTeam/greptimedb): An open-source, cloud-native, distributed time-series database.
- [deepeth/mars](https://github.com/deepeth/mars): The powerful analysis platform to explore and visualize data from blockchain.
- [mozilla/sccache](https://github.com/mozilla/sccache/): sccache is ccache with cloud storage
- [risingwave](https://github.com/risingwavelabs/risingwave): A Distributed SQL Database for Stream Processing
- [Vector](https://github.com/vectordotdev/vector): A high-performance observability data pipeline.

## Getting help

Submit [issues](https://github.com/apache/incubator-opendal/issues/new) for bug report or asking questions in the [Discussions forum](https://github.com/apache/incubator-opendal/discussions/new?category=q-a).

Talk to develops at [discord].

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
