## Capabilities

This service can be used to:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [ ] rename
- [ ] ~~presign~~

## Configuration

- `endpoint`: Set the SMB server endpoint, for example `127.0.0.1` or `127.0.0.1:445`
- `share`: Set the share name
- `root`: Set the work directory inside the share
- `user`: Set the login user
- `password`: Set the login password

You can refer to [`SmbBuilder`]'s docs for more information.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_smb::Smb;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Smb::default()
        .endpoint("127.0.0.1")
        .share("public")
        .user("alice")
        .password("secret");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
