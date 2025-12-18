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

- `endpoint`: Set the endpoint for connection
- `root`: Set the work directory for backend
- `user`: Set the login user
- `password`: Set the login password

You can refer to [`FtpBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_ftp::Ftp;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Ftp::default()
        .endpoint("127.0.0.1");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
