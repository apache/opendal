## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [ ] rename
- [ ] presign

## Configuration

- `root`: Set the work directory for backend
- `token`: VercelBlob token, environment var `BLOB_READ_WRITE_TOKEN`

You can refer to [`VercelBlobBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal_core::Operator;
use opendal_service_vercel_blob::VercelBlob;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = VercelBlob::default()
        // set the storage bucket for OpenDAL
        .root("/")
        // set the token for OpenDAL
        .token("you_token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
