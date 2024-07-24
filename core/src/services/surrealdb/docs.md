## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [ ] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [ ] ~~list~~
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `connection_string`: Set the connection string of surrealdb server
- `username`: set the username of surrealdb
- `password`: set the password of surrealdb
- `namespace`: set the namespace of surrealdb
- `database`: set the database of surrealdb
- `table`: Set the table of surrealdb
- `key_field`: Set the key field of surrealdb
- `value_field`: Set the value field of surrealdb
-

## Example

### Via Builder

```rust
use anyhow::Result;
use opendal::services::Surrealdb;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Surrealdb::default()
        .root("/")
        .connection_string("ws://127.0.0.1:8000")
        .username("username")
        .password("password")
        .namespace("namespace")
        .database("database")
        .table("table")
        .key_field("key")
        .value_field("value");

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
