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

- `root`: Set the working directory of `OpenDAL`
- `username`: Set the username for authentication.
- `password`: Set the password for authentication.
- `endpoint`: Set the network address of memcached server
- `default_ttl`: Set the ttl for memcached service.

You can refer to [`MemcachedBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_memcached::Memcached;

#[tokio::main]
async fn main() -> Result<()> {
    // create memcached backend builder
    let mut builder = Memcached::default()
        .endpoint("tcp://127.0.0.1:11211");
        // if you enable authentication, set username and password for authentication
        // builder.username("admin")
        // builder.password("password");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
