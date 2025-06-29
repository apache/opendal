## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [ ] presign

## Configuration

- `root`: Set the root path for this dashmap instance.

You can refer to [`DashmapBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Dashmap;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Dashmap::default()
        .root("/");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
