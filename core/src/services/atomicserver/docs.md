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
- [ ] blocking


## Configuration

- `root`: Set the working directory of `OpenDAL`
- `endpoint`: Set the server address for `Atomicserver`
- `private_key`: Set the private key for agent used for `Atomicserver`
- `public_key`: Set the public key for agent used for `Atomicserver`
- `parent_resource_id`:  Set the parent resource id (url) that `Atomicserver` uses to store resources under

You can refer to [`AtomicserverBuilder`]'s docs for more information.

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Atomicserver;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Atomicserver::default()
        // Set the server address for Atomicserver
        .endpoint("http://localhost:9883")
        // Set the public/private key for agent for Atomicserver
        .private_key("<private_key>")
        .public_key("<public_key>")
        // Set the parent resource id for Atomicserver. In this case
        // We are using the root resource (Drive)
        .parent_resource_id("http://localhost:9883");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
