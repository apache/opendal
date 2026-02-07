## Capabilities

This service can be used to:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [x] rename
- [ ] presign

## Configuration

- `root`: Set the work directory for backend
- `access_token` YandexDisk oauth access_token

You can refer to [`YandexDiskBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_yandex_disk::YandexDisk;

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
