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
- `collection`: Set the collection of mongodb
- `key_field`: Set the key field of mongodb
- `value_field`: Set the value field of mongodb

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Mongodb;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Mongodb::default();
    builder.root("/");
    builder.connection_string("mongodb://myUser:myPassword@localhost:27017/myAuthDB");
    builder.database("your_database");
    builder.collection("your_collection");
    // key field type in the table should be compatible with Rust's &str like text
    builder.key_field("key");
    // value field type in the table should be compatible with Rust's Vec<u8> like bytea
    builder.value_field("value");

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
