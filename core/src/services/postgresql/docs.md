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
- `connection_string`: Set the connection string of postgres server
- `table`: Set the table of postgresql
- `key_field`: Set the key field of postgresql
- `value_field`: Set the value field of postgresql

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Postgresql;
use opendal::Operator;
use opendal::raw::oio::Read;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Postgresql::default()
        .root("/")
        // postgresql://username:password@host:port/database
        // About more connection url's format
        // Check https://www.postgresql.org/docs/current/libpq-connect.html
        .connection_string("postgresql://username:password@host:port/database")
        .table("table_name")
        // key field type in the table should be compatible with Rust's &str like text
        .key_field("key_in_table")
        // value field type in the table should be compatible with Rust's Vec<u8> like bytea
        .value_field("value");

    let op = Operator::new(builder)?.finish();
    
    // write data
    op.write("hello","world").await?;

    // read data
    let result= op.read("hello").await?.read_all().await?.to_bytes();
    println!("read data: {:?}", String::from_utf8(result.to_vec()));

    Ok(())
}
```
