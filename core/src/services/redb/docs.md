## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [ ] ~~list~~
- [ ] ~~presign~~
- [x] blocking

## Configuration

- `datadir`: Set the path to the redb data directory

You can refer to [`RedbBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Redb;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Redb::default()
        .datadir("/tmp/opendal/redb")
        .table("opendal-redb");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
