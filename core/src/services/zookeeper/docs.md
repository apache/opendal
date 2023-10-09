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
- [ ] ~~blocking~~

## Configuration

- `endpoint`: Set the endpoint to the zookeeper cluster
- `user`: Set the user to connect to zookeeper service for ACL
- `password`: Set the password to connect to zookeeper service for ACL

You can refer to [`ZookeeperBuilder`]'s docs for more information

## Example

### Via Builder

```rust
use anyhow::Result;
use opendal::services::Tikv;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Zookeeper::default();
    builder.endpoint("127.0.0.1:2181");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
