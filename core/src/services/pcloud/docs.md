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

```rust,no_run
use anyhow::Result;
use opendal::services::Pcloud;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Pcloud::default()
        // set the storage bucket for OpenDAL
        .root("/")
        // set the bucket for OpenDAL
        .endpoint("[https](https://api.pcloud.com)")
        // set the username for OpenDAL
        .username("opendal@gmail.com")
        // set the password name for OpenDAL
        .password("opendal");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
