## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [ ] ~~list~~
- [ ] scan
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `endpoint`: Set the network address of memcached server
- `default_ttl`: Set the ttl for memcached service.

You can refer to [`MemcachedBuilder`]'s docs for more information

## Example

### Via Builder

```rust
use anyhow::Result;
use opendal::services::Memcached;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create memcached backend builder
    let mut builder = Memcached::default();

    builder.endpoint("tcp://127.0.0.1:11211");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```