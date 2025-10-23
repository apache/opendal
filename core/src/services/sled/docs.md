## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [ ] copy
- [ ] rename
- [x] list
- [ ] ~~presign~~

## Configuration

- `datadir`: Set the path to the sled data directory

You can refer to [`SledBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Sled;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Sled::default()
        .datadir("/tmp/opendal/sled");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
