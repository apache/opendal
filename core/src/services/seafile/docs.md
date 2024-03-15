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
- [x] scan
- [ ] presign
- [ ] blocking

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
use opendal::services::Seafile;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Seafile::default();

    // set the storage bucket for OpenDAL
    builder.root("/");
    // set the endpoint for OpenDAL
    builder.endpoint("http://127.0.0.1:80");
    // set the username for OpenDAL
    builder.username("xxxxxxxxxx");
    // set the password name for OpenDAL
    builder.password("opendal");
    // set the repo_name for OpenDAL
    builder.repo_name("xxxxxxxxxxxxx");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
