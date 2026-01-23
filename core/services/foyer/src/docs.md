# Foyer Service

[foyer](https://github.com/foyer-rs/foyer) is a high-performance hybrid cache library
that supports both in-memory and on-disk caching.

This service provides foyer as a volatile KV storage backend. Data stored may be evicted 
when the cache is full.

## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] delete
- [ ] list (not supported)
- [ ] blocking (not supported)

## Configuration

Foyer service requires a pre-configured `HybridCache` instance. It cannot be
constructed from configuration alone because the cache setup involves async
operations and complex configuration.

## Example

### Via Builder

```rust,no_run
use opendal::Operator;
use opendal_service_foyer::Foyer;
use opendal_service_foyer::FoyerKey;
use opendal_service_foyer::FoyerValue;
use foyer::{HybridCacheBuilder, Engine};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a foyer HybridCache
    let cache = HybridCacheBuilder::new()
        .memory(64 * 1024 * 1024) // 64MB memory cache
        .storage(Engine::Large(Default::default()))
        .build()
        .await?;

    // Create operator
    let op = Operator::new(Foyer::new(cache))?
        .finish();

    // Use it like any other OpenDAL operator
    op.write("key", "value").await?;
    let value = op.read("key").await?;

    Ok(())
}
```

## Notes

- **Data Volatility**: Foyer is a cache, not persistent storage. Data may be
  evicted at any time when the cache reaches its capacity limit.
- **No List Support**: Foyer does not support efficient key iteration, so the
  `list` operation is not available.
- **Async Only**: Foyer is async-only, blocking operations are not supported.
