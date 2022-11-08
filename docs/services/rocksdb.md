# Use rocksdb as backend

These docs provide a detailed examples for using rocksdb as backend.

We can run this example via:

```shell
OPENDAL_ROCKSDB_DATADIR=/tmp/rocksdb cargo run --example=rocksdb --features=services-rocksdb
```

## Example

### Via Environment

All config could be passed via environment:

- `OPENDAL_ROCKSDB_DATADIR` the path to the rocksdb data directory (required)
- `OPENDAL_ROCKSDB_ROOT` working directory of opendal, default is `/`

```rust
use anyhow::Result;
use opendal::Object;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::from_env(Scheme::Rocksdb)?;

    // create an object handler to start operation on rocksdb!
    let _op: Object = op.object("hello_rocksdb!");

    Ok(())
}
```

### Via Builder

```rust
{{#include ../../examples/rocksdb.rs:15:}}
```
