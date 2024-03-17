## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [ ] ~~list~~
- [ ] scan
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `connection_string`: Set the connection string of mongodb server
- `database`: Set the database of mongodb
- `bucket`: Set the bucket of mongodb gridfs
- `chunk_size`: Set the chunk size of mongodb gridfs

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Gridfs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Gridfs::default();
    builder.root("/");
    builder.connection_string("mongodb://myUser:myPassword@localhost:27017/myAuthDB");
    builder.database("your_database");
    builder.bucket("your_bucket");
    // The chunk size in bytes used to break the user file into chunks.
    builder.chunk_size(255);

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
