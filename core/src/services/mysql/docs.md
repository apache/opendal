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
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `connection_string`: Set the connection string of mysql server
- `table`: Set the table of mysql
- `key_field`: Set the key field of mysql
- `value_field`: Set the value field of mysql

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Mysql;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Mysql::default()
        .root("/")
        .connection_string("mysql://you_username:your_password@127.0.0.1:5432/your_database")
        .table("your_table")
        // key field type in the table should be compatible with Rust's &str like text
        .key_field("key")
        // value field type in the table should be compatible with Rust's Vec<u8> like bytea
        .value_field("value");

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
