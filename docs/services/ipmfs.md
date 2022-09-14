# Use IPMFS as backend

This page provides some examples for using IPMFS as backend.

We can run this example via:

```shell
cargo run --example ipmfs
```

## Example

### Via Environment Variables

Available environment variables:

- `OPENDAL_IPMFS_ROOT`: root path, default: /
- `OPENDAL_IPMFS_ENDPOINT`: endpoint of ipfs.

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // init operator from env vars
    let _op = Operator::from_env(Scheme::Ipmfs)?;
}
```

### Via Builder

```rust
{{#include ../../examples/ipmfs.rs:15:}}
```
