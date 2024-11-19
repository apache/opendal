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
- [ ] blocking

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `connection_string`: Set the connection string of sqlite database
- `table`: Set the table of sqlite
- `key_field`: Set the key field of sqlite
- `value_field`: Set the value field of sqlite

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Sqlite;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Sqlite::default()
        .root("/")
        .connection_string("file//abc.db")
        .table("your_table")
        // key field type in the table should be compatible with Rust's &str like text
        .key_field("key")
        // value field type in the table should be compatible with Rust's Vec<u8> like bytea
        .value_field("value");

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
