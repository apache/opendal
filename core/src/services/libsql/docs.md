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
- `connection_string`: Set the connection string for libsql server
- `auth_token`: Set the authentication token for libsql server
- `table`: Set the table of libsql
- `key_field`: Set the key field of libsql
- `value_field`: Set the value field of libsql

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Libsql;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Libsql::default();
    builder.root("/");
    builder.connection_string("https://example.com/db");
    builder.auth_token("secret");
    builder.table("your_table");
    // key field type in the table should be compatible with Rust's &str like text
    builder.key_field("key");
    // value field type in the table should be compatible with Rust's Vec<u8> like bytea
    builder.value_field("value");

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
