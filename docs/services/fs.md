# Use fs as backend

These docs provide a detailed examples for using fs as backend.

We can run this example via:

```shell
cargo run --example fs
```

All config could be passed via environment:

- `OPENDAL_FS_ROOT`: root path, default: `/tmp`

## Example

### Via Environment

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // Init Operator from env.
    let op = Operator::from_env(Scheme::Fs).await?;
}
```

### Via Builder

```rust
{{#include ../../examples/fs.rs:15:}}
```
