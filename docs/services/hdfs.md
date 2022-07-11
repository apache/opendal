# Hdfs

These docs provide a detailed examples for using hdfs as backend.

We can run this example via:

```shell
cargo run --example hdfs --features services-hdfs
```

All config could be passed via environment:

- `OPENDAL_HDFS_ROOT`: root path, default: `/tmp`
- `OPENDAL_HDFS_NAME_NODE`: name node for hdfs, default: default

## Example

Before running this example, please make sure the following env set correctly:

- `JAVA_HOME`
- `HADOOP_HOME`

### Via Environment

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // Init Operator from env.
    let op = Operator::from_env(Scheme::Hdfs).await?;
}
```

### Via Builder

```rust
{{#include ../../examples/hdfs.rs:15:}}
```
