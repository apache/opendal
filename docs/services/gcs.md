# Use GCS as backend

This page provides some examples for using Google Cloud Storage as backend.

All config could be passed via environment variables:
- `OPENDAL_GCS_BUCKET` bucket used for storing data, required
- `OPENDAL_GCS_ROOT` working directory inside the bucket, default is "/"
- `OPENDAL_GCS_CREDENTIAL` base64 OAUTH2 token used for authentication, required

## Example

### Via Environment Variables

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // init operator from env vars
    let _op = Operator::from_env(Scheme::Gcs)?;
}
```

### Via Builder

```rust
{{#include ../../examples/gcs.rs:15:}}
```
