## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [ ] ~~rename~~
- [ ] ~~presign~~

## Configurations

- `endpoint`: Set the endpoint for backend.
- `container`: Swift container.
- `token`: Swift personal access token.

Refer to [`SwiftBuilder`]'s public API docs for more information.

## Examples

### Via Builder

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Swift;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create Swift backend builder
    let mut builder = Swift::default() 
        // Set the root for swift, all operations will happen under this root
        .root("/path/to/dir")
        // set the endpoint of Swift backend
        .endpoint("https://openstack-controller.example.com:8080/v1/account")
        // set the container name of Swift workspace
        .container("container")
        // set the auth token for builder
        .token("token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
