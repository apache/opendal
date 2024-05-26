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
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `endpoints`: Set the network address of etcd servers
- `username`: Set the username of Etcd
- `password`: Set the password for authentication
- `ca_path`: Set the ca path to the etcd connection
- `cert_path`: Set the cert path to the etcd connection
- `key_path`: Set the key path to the etcd connection

You can refer to [`EtcdBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Etcd;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Etcd::default();

    // this will build a Operator accessing etcd which runs on http://127.0.0.1:2379
    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
