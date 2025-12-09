## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [ ] rename
- [ ] presign

## Configuration

- `root`: Set the work directory for backend
- `endpoint`: Seafile endpoint address
- `username` Seafile username
- `password` Seafile password
- `repo_name` Seafile repo name

You can refer to [`SeafileBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal_core::services::Seafile;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Seafile::default()
        // set the storage bucket for OpenDAL
        .root("/")
        // set the endpoint for OpenDAL
        .endpoint("http://127.0.0.1:80")
        // set the username for OpenDAL
        .username("xxxxxxxxxx")
        // set the password name for OpenDAL
        .password("opendal")
        // set the repo_name for OpenDAL
        .repo_name("xxxxxxxxxxxxx");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
