## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [ ] list
- [ ] presign
- [ ] blocking

## Configuration

- `name`: Set the name for this cache instance.
- `max_capacity`: Set the max capacity of the cache.
- `time_to_live`: Set the time to live of the cache.
- `time_to_idle`: Set the time to idle of the cache.
- `num_segments`: Set the segments number of the cache.

You can refer to [`MokaBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Moka;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Moka::default()
        .name("opendal");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
