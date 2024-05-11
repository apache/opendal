## Capabilities

This service can be used to:

- [x] read
- [x] write
- [x] list
- [ ] copy
- [ ] rename
- [ ] ~~presign~~
- [ ] blocking

## Notes

Currently, only OneDrive Personal is supported.

## Configuration

- `access_token`: set the access_token for Graph API
- `root`: Set the work directory for backend

You can refer to [`OnedriveBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Onedrive;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Onedrive::default();

    builder.access_token("xxx").root("/path/to/root");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}

