# Use IPFS as backend

This page provides some examples for using IPFS as backend.

We can run this example via:

```shell
cargo run --example ipfs --features services-ipfs
```

## Example

### Via Environment Variables

Available environment variables:

- `OPENDAL_IPFS_ROOT`: root path, like `/ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ`
- `OPENDAL_IPFS_ENDPOINT`: endpoint of ipfs, like `https://ipfs.io`

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // init operator from env vars
    let _op = Operator::from_env(Scheme::Ipfs)?;
}
```

### Via Builder

```rust
{{#include ../../examples/ipfs.rs:15:}}
```
