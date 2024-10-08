## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [x] rename
- [x] list
- [ ] presign
- [ ] blocking

## Configuration

- `root`: Set the work directory for backend
- `endpoint`: Customizable endpoint setting

You can refer to [`AlluxioBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Alluxio;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Alluxio::default()
        // set the storage bucket for OpenDAL
        .root("/")
        // set the endpoint for OpenDAL
        .endpoint("http://127.0.0.1:39999");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
