## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [ ] ~~list~~
- [ ] ~~presign~~
- [ ] blocking

**Note**: As for [Known Limitations - FoundationDB](https://apple.github.io/foundationdb/known-limitations), keys cannot exceed 10,000 bytes in size, and values cannot exceed 100,000 bytes in size. Errors will be raised by OpenDAL if these limits are exceeded.

## Configuration

- `root`: Set the work directory for this backend.
- `config_path`: Set the configuration path for foundationdb. If not provided, the default configuration path will be used.

You can refer to [`FoundationdbBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Foundationdb;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Foundationdb::default()
        .config_path("/etc/foundationdb/foundationdb.conf");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
