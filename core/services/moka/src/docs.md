## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [ ] rename
- [ ] presign

## Configuration

- `name`: Set the name for this cache instance.
- `max_capacity`: Set the max capacity of the cache.
- `time_to_live`: Set the time to live of the cache.
- `time_to_idle`: Set the time to idle of the cache.

You can refer to [`MokaBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_moka::Moka;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Moka::default()
        .name("opendal");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
