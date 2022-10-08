# Use OSS as backend

This page provides some examples for using Aliyun OSS as backend.

We can run this example via:

```shell
cargo run --example oss
```

## Example

### Via Environment Variables

Available environment variables:

- `OPENDAL_OSS_ROOT`: root path, default: /
- `OPENDAL_OSS_BUCKET`: bukcet name, required.
- `OPENDAL_OSS_ENDPOINT`: endpoint of oss service
- `OPENDAL_OSS_ACCESS_KEY_ID`: access key id of oss service, could be auto detected.
- `OPENDAL_OSS_ACCESS_KEY_SECRET`: secret access key of oss service, could be auto detected.

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // init operator from env vars
    let _op = Operator::from_env(Scheme::Oss)?;
}
```

### Via Builder

```rust
{{#include ../../examples/oss.rs:15:}}
```
