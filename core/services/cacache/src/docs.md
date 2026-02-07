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

- `datadir`: Set the path to the cacache data directory

You can refer to [`CacacheBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_cacache::Cacache;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Cacache::default().datadir("/tmp/opendal/cacache");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
