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
- `token`: Set the token of cloudflare api
- `account_identifier`: Set the account identifier of d1
- `database_identifier`: Set the database identifier of d1
- `endpoint`: Set the endpoint of d1 service
- `table`: Set the table name of the d1 service to read/write
- `key_field`: Set the key field of d1
- `value_field`: Set the value field of d1

## Example

### Via Builder

```rust
use anyhow::Result;
use opendal::services::D1;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = D1::default();
    builder
        .token("token")
        .account_id("account_id")
        .database_id("database_id")
        .table("table")
        .key_field("key_field")
        .value_field("value_field");

    let op = Operator::new(builder)?.finish();
    let source_path = "ALFKI";
    // set value to d1 "opendal test value" as Vec<u8>
    let value = "opendal test value".as_bytes();
    // write value to d1, the key is source_path
    op.write(source_path, value).await?;
    // read value from d1, the key is source_path
    let v = op.read(source_path).await?;
    assert_eq!(v, value);
    // delete value from d1, the key is source_path
    op.delete(source_path).await?;
    Ok(())
}
```
