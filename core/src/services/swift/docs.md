This service will visit the [Swift API](https://docs.openstack.org/api-ref/object-store/) supported by [OpenStack Object Storage](https://docs.openstack.org/swift/latest/).

## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [ ] ~~rename~~
- [x] list
- [ ] ~~scan~~
- [ ] ~~presign~~
- [ ] blocking

## Configurations

- `endpoint`: Set the endpoint for backend.
- `account_name`: Name of Swift account.
- `container`: Swift container.
- `token`: Swift personal access token.

Refer to [`SwiftBuilder`]'s public API docs for more information.

## Examples

### Via Builder

```rust
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Swift;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create Swift backend builder
    let mut builder = Swift::default();
    
    // Set the root for swift, all operations will happen under this root
    builder.root("/path/to/dir");
    // set the endpoint of Swift backend
    builder.endpoint("https://openstack-controller.example.com:8080");
    // set the account name of Swift workspace
    builder.account_name("account");
    // set the container name of Swift workspace
    builder.container("container");
    // set the auth token for builder
    builder.token("token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
