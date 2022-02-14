# OpenDAL

Open **D**ata **A**ccess **L**ayer that connect the whole world together.

## Status

OpenDAL is in **alpha** stage and has been early adopted by [databend](https://github.com/datafuselabs/databend/). Welcome any feedback at [Discussions](https://github.com/datafuselabs/opendal/discussions)!

## Quickstart

```rust
use opendal::Operator;
use opendal::services::fs;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()>{
    let mut op = Operator::new(fs::Backend::build().finish().await?);
    
    // Write data into file.
    let n = op.write("/path/to/file.txt", 1024).run(buf).await?;
    
    // Read data from file.
    let mut r = op.read("/path/to/file.txt").run().await?;
    r.read_to_end(&mut buf).await?;
    
    // Get file metadata.
    let o = op.stat("/path/to/file.txt").run().await?;
    
    // Delete file.
    op.delete("/path/to/file.txt").run().await?;
}
```

## License

OpenDAL is licensed under [Apache 2.0](./LICENSE).
