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
- [ ] blocking


## Configuration

- `root`: Set the working directory of `OpenDAL`
- `endpoint`: Set the server address for `AtomicData`

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

    // Set the server address for AtomicData
    builder.endpoint("https://atomicdata.dev");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
