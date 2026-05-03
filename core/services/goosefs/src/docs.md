## Capabilities

This service can be used to:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [x] rename
- [ ] presign

## Notes

GooseFS service uses native gRPC protocol (not REST API like Alluxio),
which means it connects directly to GooseFS Master (port 9200) and
Worker (port 9203) without requiring a Proxy component.

Features:
- **HA support**: Comma-separated master addresses for automatic Primary Master discovery.
- **Block-level I/O**: Data reads/writes go through block-level gRPC bidirectional streaming.
- **Consistent hash routing**: Worker selection uses consistent hashing on block IDs.
- **All WriteTypes**: Supports MUST_CACHE, CACHE_THROUGH, THROUGH, and ASYNC_THROUGH.

## Configuration

- `root`: Set the work directory for backend
- `master_addr`: GooseFS Master address (`host:port`), supports comma-separated for HA
- `block_size`: Block size for new files (default: 64 MiB)
- `chunk_size`: Chunk size for streaming RPCs (default: 1 MiB)
- `write_type`: Default write type (`must_cache`, `cache_through`, `through`, `async_through`)
- `auth_type`: Authentication type (`nosasl`, `simple`). Default: `simple`
- `auth_username`: Authentication username for SIMPLE mode. Default: current OS user

You can refer to [`GooseFsBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use opendal::Operator;
use opendal::Result;
use opendal::services::GooseFs;

#[tokio::main]
async fn main() -> Result<()> {
    // Single master
    let builder = GooseFs::default()
        .root("/data")
        .master_addr("10.0.0.1:9200");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```

### Via URI

```rust,no_run
use opendal::Operator;
use opendal::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::from_uri("goosefs://10.0.0.1:9200/data")?;
    Ok(())
}
```

### HA Mode

```rust,no_run
use opendal::Operator;
use opendal::Result;
use opendal::services::GooseFs;

#[tokio::main]
async fn main() -> Result<()> {
    let builder = GooseFs::default()
        .root("/data")
        .master_addr("10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200")
        .write_type("cache_through");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```

### With Authentication

```rust,no_run
use opendal::Operator;
use opendal::Result;
use opendal::services::GooseFs;

#[tokio::main]
async fn main() -> Result<()> {
    // SIMPLE authentication (default) with custom username
    let builder = GooseFs::default()
        .root("/data")
        .master_addr("10.0.0.1:9200")
        .auth_type("simple")
        .auth_username("myuser");

    let op: Operator = Operator::new(builder)?.finish();

    // No authentication (NOSASL mode)
    let builder = GooseFs::default()
        .root("/data")
        .master_addr("10.0.0.1:9200")
        .auth_type("nosasl");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
