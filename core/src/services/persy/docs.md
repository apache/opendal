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

- `datafile`: Set the path to the persy data file. The directory in the path must already exist.
- `segment`: Set the name of the persy segment.
- `index`: Set the name of the persy index.

You can refer to [`PersyBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Persy;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Persy::default()
        .datafile("./test.persy")
        .segment("data")
        .index("index");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
