# Use OBS as backend

This page provides some examples for using Huaweicloud OBS as backend.

We can run this example via:

```shell
cargo run --example obs
```

## Example

### Via Environment Variables

Available environment variables:

- `OPENDAL_OBS_ROOT`: root path, default: /
- `OPENDAL_OBS_BUCKET`: bukcet name, required.
- `OPENDAL_OBS_ENDPOINT`: endpoint of obs service
- `OPENDAL_OBS_ACCESS_KEY_ID`: access key id of obs service, could be auto detected.
- `OPENDAL_OBS_SECRET_ACCESS_KEY`: secret access key of obs service, could be auto detected.

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // init operator from env vars
    let _op = Operator::from_env(Scheme::Obs)?;
}
```

### Via Builder

```rust
{{#include ../../examples/obs.rs:15:}}
```
