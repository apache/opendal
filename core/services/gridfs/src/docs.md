## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [ ] list
- [ ] copy
- [ ] rename
- [ ] ~~presign~~

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `connection_string`: Set the connection string of mongodb server
- `database`: Set the database of mongodb
- `bucket`: Set the bucket of mongodb gridfs
- `chunk_size`: Set the chunk size of mongodb gridfs

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_gridfs::Gridfs;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Gridfs::default()
        .root("/")
        .connection_string("mongodb://myUser:myPassword@localhost:27017/myAuthDB")
        .database("your_database")
        .bucket("your_bucket")
        // The chunk size in bytes used to break the user file into chunks.
        .chunk_size(255);

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
