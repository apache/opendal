## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [ ] rename
- [ ] presign

## Configuration

- `root`: Set the work directory for backend.
- `bucket`: Set the bucket name for backend.
- `endpoint`: Set the endpoint for backend.
- `region`: Set the region for backend.
- `access_key_id`: Set the access_key_id for backend.
- `secret_access_key`: Set the secret_access_key for backend.
- `security_token`: Set the security token for backend.

Refer to [`TosBuilder`]'s public API docs for more information.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_tos::Tos;

fn main() -> Result<()> {
    let builder = Tos::default()
        // Set the root for TOS, all operations will happen under this root.
        .root("/path/to/dir")
        // Set the bucket name. This is required.
        .bucket("test")
        // Set the endpoint.
        //
        // For example:
        // - "https://tos-cn-beijing.volces.com"
        // - "https://tos-cn-shanghai.volces.com"
        .endpoint("https://tos-cn-beijing.volces.com")
        // Set the region.
        .region("cn-beijing")
        // Set the access_key_id and secret_access_key.
        //
        // OpenDAL will try to load credentials from the environment if
        // credentials are not set explicitly.
        .access_key_id("access_key_id")
        .secret_access_key("secret_access_key");

    let _op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
