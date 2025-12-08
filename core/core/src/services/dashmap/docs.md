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

- `root`: Set the root path for this dashmap instance.

You can refer to [`DashmapBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal_core::services::Dashmap;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Dashmap::default()
        .root("/");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
