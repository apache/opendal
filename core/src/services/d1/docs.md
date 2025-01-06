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
- `token`: Set the token of cloudflare api
- `account_id`: Set the account id of cloudflare api
- `database_id`: Set the database id of cloudflare api
- `table`: Set the table of D1 Database
- `key_field`: Set the key field of D1 Database
- `value_field`: Set the value field of D1 Database

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::D1;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = D1::default()
        .token("token")
        .account_id("account_id")
        .database_id("database_id")
        .table("table")
        .key_field("key_field")
        .value_field("value_field");

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
