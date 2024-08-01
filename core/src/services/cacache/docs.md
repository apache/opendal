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
- [ ] ~~presign~~
- [x] blocking

## Configuration

- `datadir`: Set the path to the cacache data directory

You can refer to [`CacacheBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Cacache;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Cacache::default().datadir("/tmp/opendal/cacache");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
