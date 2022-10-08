# Use redis as backend

These docs provide a detailed examples for using redis as backend.

We can run this example via:

```shell
cargo run --example redis --features services-redis
```

## Example

### Via Environment

All config could be passed via environment:

- `OPENDAL_REDIS_ENDPOINT` endpoint to redis services, required.
- `OPENDAL_REDIS_ROOT` root dir of this ftp services, default to `/`
- `OPENDAL_REDIS_USERNAME`
- `OPENDAL_REDIS_PASSWORD`
- `OPENDAL_REDIS_DB`

```rust
use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    // Init Operator from env.
    let op = Operator::from_env(Scheme::Redis)?;
}
```

### Via Builder

```rust
{{#include ../../examples/redis.rs:15:}}
```
