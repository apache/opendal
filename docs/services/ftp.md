# Use ftp as backend

These docs provide a detailed examples for using ftp as backend.

We can run this example via:

```shell
cargo run --example ftp --features services-ftp
```

## Example

### Via Environment

All config could be passed via environment:

- `OPENDAL_FTP_ENDPOINT` endpoint to ftp services, required.
- `OPENDAL_FTP_ROOT` root dir of this ftp services, default to `/`
- `OPENDAL_FTP_USER`
- `OPENDAL_FTP_PASSWORD`

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // Init Operator from env.
    let op = Operator::from_env(Scheme::Ftp)?;
}
```

### Via Builder

```rust
{{#include ../../examples/ftp.rs:15:}}
```
