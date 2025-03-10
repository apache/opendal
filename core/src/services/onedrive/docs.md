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

Currently, OpenDAL supports OneDrive Personal only.

## Configuration

- `access_token`: set a short-live access token for Microsoft Graph API (also, OneDrive API)
- `refresh_token`: set a long term access token for Microsoft Graph API
- `client_id`: set the client ID for a Microsoft Graph API application (available though Azure's registration portal)
- `client_secret`: set the client secret for a Microsoft Graph API application
- `root`: Set the work directory for OneDrive backend

Read more at [`OnedriveBuilder`].

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Onedrive;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Onedrive::default()
        .access_token("xxx")
        .root("/path/to/root");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}

