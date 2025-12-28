## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [ ] list
- [ ] copy
- [ ] rename
- [ ] ~~presign~~

## Configuration

- `datadir`: Set the path to the redb data directory.
- `table`: Set the table name for Redb.

You can refer to [`RedbBuilder`]'s docs for more information.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_redb::Redb;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Redb::default()
        .datadir("/tmp/opendal/redb")
        .table("opendal-redb");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
