## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [ ] rename
- [x] list
- [x] presign
- [ ] blocking

## Configuration

- `root`: Set the work directory for backend
- `key_id`: B2 application key keyID
- `application_key` B2 application key applicationKey
- `bucket` B2 bucket name
- `bucket_id` B2 bucket_id

You can refer to [`B2Builder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::B2;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = B2::default()
        // set the storage bucket for OpenDAL
        .root("/")
        // set the key_id for OpenDAL
        .application_key_id("xxxxxxxxxx")
        // set the key_id for OpenDAL
        .application_key("xxxxxxxxxx")
        // set the     bucket name for OpenDAL
        .bucket("opendal")
        // set the bucket_id for OpenDAL
        .bucket_id("xxxxxxxxxxxxx");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
