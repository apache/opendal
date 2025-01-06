## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [x] list
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `endpoint`: Set the endpoint for connection
- `root`: Set the work directory for backend
- `user`: Set the login user
- `password`: Set the login password

You can refer to [`FtpBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Ftp;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Ftp::default()
        .endpoint("127.0.0.1");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
