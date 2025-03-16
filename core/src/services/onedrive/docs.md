## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [ ] append
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [ ] presign
- [ ] blocking

## Notes

Currently, OpenDAL supports OneDrive Personal only.

### Write Operations and OneDrive Behavior

For write-related operations, such as:

- write
- rename
- copy
- create_dir

OpenDAL's OneDrive service replaces the destination folder instead of rename it.

### Consistency Issues with Concurrent Requests

OneDrive does not guarantee consistency when handling a large number of concurrent requests write operations.

In some extreme cases, OneDrive may acknowledge an operation as successful but fail to commit the changes.
This inconsistency can cause subsequent operations to fail, returning errors like:

- 400 Bad Request: OneDrive considers folders in the path are not there yet
- 404 Not Found: OneDrive doesn't recognize the created folder
- 409 Conflict: OneDrive can't replace an existing folder

You should consider [`RetryLayer`] and monitor your operations carefully.

## Configuration

- `access_token`: set a short-live access token for Microsoft Graph API (also, OneDrive API)
- `refresh_token`: set a long term access token for Microsoft Graph API
- `client_id`: set the client ID for a Microsoft Graph API application (available though Azure's registration portal)
- `client_secret`: set the client secret for a Microsoft Graph API application
- `root`: Set the work directory for OneDrive backend

Read more at [`OnedriveBuilder`].

## Example

### Via Builder

When you have a current access token: 

```rust,no_run
use anyhow::Result;
use opendal::services::Onedrive;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Onedrive::default()
        .access_token("my_access_token")
        .root("/root/folder/for/operator");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```

When you have an Application with a refresh token:

```rust,no_run
use anyhow::Result;
use opendal::services::Onedrive;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Onedrive::default()
        .refresh_token("my_refresh_token")
        .client_id("my_client_id")
        .root("/root/folder/for/operator");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```


[conflict-behavior]: https://learn.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0#instance-attributes
