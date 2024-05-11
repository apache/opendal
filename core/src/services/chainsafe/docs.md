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
- [ ] presign
- [ ] blocking

## Configuration

- `root`: Set the work directory for backend
- `bucket_id` Chainsafe bucket_id
- `api_key` Chainsafe api_key

You can refer to [`ChainsafeBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Chainsafe;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Chainsafe::default();

    // set the storage root for OpenDAL
    builder.root("/");
    // set the bucket_id for OpenDAL
    builder.bucket_id("opendal");
    // set the api_key for OpenDAL
    builder.api_key("xxxxxxxxxxxxx");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
