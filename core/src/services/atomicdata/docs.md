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
- [ ] scan
- [ ] ~~presign~~
- [x] blocking


## Configuration

- `root`: Set the working directory of `OpenDAL`

You can refer to [`AtomicdataBuilder`]'s docs for more information.

## Example

### Via Builder

```rust
use anyhow::Result;
use opendal::services::Atomicdata;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Atomicdata::default();

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
