# Read compressed files

OpenDAL has native decompress support.

To enable decompress features, we need to specify it in `Cargo.toml`

```toml
opendal = { version = "0.7", features = ["compress"]}
```

## Read into bytes

Use [decompress_read](/opendal/struct.Object.html#method.decompress_read) to read compressed file into bytes.

```rust
use opendal::services::memory;
use std::io::Result;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // let op = Operator::new(memory::Backend::build().finish().await?);
    let o = op.object("path/to/file.gz");
    let bs: Option<Vec<u8>> = o.decompress_read().await?;
    Ok(())
}
```

Or, specify the compress algorithm instead by [decompress_read_with](/opendal/struct.Object.html#method.decompress_read_with):

```rust
use opendal::services::memory;
use std::io::Result;
use opendal::Operator;
use opendal::io_util::CompressAlgorithm;

#[tokio::main]
async fn main() -> Result<()> {
    // let op = Operator::new(memory::Backend::build().finish().await?);
    let o = op.object("path/to/file.gz");
    let bs: Vec<u8> = o.decompress_read_with(CompressAlgorithm::Gzip).await?;
    Ok(())
}
```

## Read as reader

Use [decompress_reader](/opendal/struct.Object.html#method.decompress_reader) to read compressed file as reader.

```rust
use opendal::services::memory;
use std::io::Result;
use opendal::Operator;
use opendal::BytesReader;

#[tokio::main]
async fn main() -> Result<()> {
    // let op = Operator::new(memory::Backend::build().finish().await?);
    let o = op.object("path/to/file.gz");
    let r: Option<BytesReader> = o.decompress_reader().await?;
    Ok(())
}
```

Or, specify the compress algorithm instead by [decompress_reader_with](/opendal/struct.Object.html#method.decompress_reader_with):

```rust
use opendal::services::memory;
use std::io::Result;
use opendal::Operator;
use opendal::BytesReader;
use opendal::io_util::CompressAlgorithm;

#[tokio::main]
async fn main() -> Result<()> {
    // let op = Operator::new(memory::Backend::build().finish().await?);
    let o = op.object("path/to/file.gz");
    let bs: BytesReader = o.decompress_reader_with(CompressAlgorithm::Gzip).await?;
    Ok(())
}
```
