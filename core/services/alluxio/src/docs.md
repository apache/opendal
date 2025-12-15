## Capabilities

This service can be used to:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [x] rename
- [ ] presign

## Configuration

- `root`: Set the work directory for backend
- `endpoint`: Customizable endpoint setting

You can refer to [`AlluxioBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal_service_alluxio::Alluxio;
use opendal_core::Operator;

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
