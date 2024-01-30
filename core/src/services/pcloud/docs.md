## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [x] scan
- [ ] presign
- [ ] blocking

## Configuration

- `root`: Set the work directory for backend
- `endpoint`: Pcloud bucket name
- `username` Pcloud username
- `password` Pcloud password

You can refer to [`PcloudBuilder`]'s docs for more information

## Example

### Via Builder

```rust
use anyhow::Result;
use opendal::services::Pcloud;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Pcloud::default();

    // set the storage bucket for OpenDAL
    builder.root("/");
    // set the bucket for OpenDAL
    builder.endpoint("[https](https://api.pcloud.com)");
    // set the username for OpenDAL
    builder.username("opendal@gmail.com");
    // set the password name for OpenDAL
    builder.password("opendal");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
