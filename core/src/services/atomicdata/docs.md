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
- `private_key`: Set the private key for agent used for `Atomicdata`
- `public_key`: Set the public key for agent used for `AtomicData`
- `parent_resource_id`:  Set the parent resource id (url) that `AtomicData` uses to store resources under

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
    builder.endpoint("http://localhost:9883");
    // Set the public/private key for agent for AtomicData
    builder.private_key("<private_key>");
    builder.public_key("<public_key>");
    // Set the parent resource id for AtomicData. In this case
    // We are using the root resource (Drive)
    builder.parent_resource_id("http://localhost:9883");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
