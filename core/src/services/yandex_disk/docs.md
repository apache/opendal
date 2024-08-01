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
- `access_token` YandexDisk oauth access_token

You can refer to [`YandexDiskBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::YandexDisk;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = YandexDisk::default()
        // set the storage bucket for OpenDAL
        .root("/")
        // set the access_token for OpenDAL
        .access_token("test");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
