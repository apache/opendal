## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] append
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `endpoint`: Set the endpoint for connection. The format is same as `openssh`, using either `[user@]hostname` or `ssh://[user@]hostname[:port]`. A username or port that is specified in the endpoint overrides the one set in the builder (but does not change the builder).
- `root`: Set the work directory for backend. It uses the default directory set by the remote `sftp-server` as default
- `user`: Set the login user
- `key`: Set the public key for login
- `known_hosts_strategy`: Set the strategy for known hosts, default to `Strict`
- `enable_copy`: Set whether the remote server has copy-file extension

For security reasons, it doesn't support password login, you can use public key or ssh-copy-id instead.

You can refer to [`SftpBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Sftp;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Sftp::default()
        .endpoint("127.0.0.1")
        .user("test")
        .key("test_key");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
