## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [ ] presign
- [ ] blocking

## Configuration

- `root`: Set the work dir for backend.
- `access_token`: Set the access_token for backend.
- `client_id`: Set the client_id for backend.
- `client_secret`: Set the client_secret for backend.
- `refresh_token`: Set the refresh_token for backend.
- `drive_type`: Set the drive_type for backend.

Refer to [`AliyunDriveBuilder`]`s  public API docs for more information.

## Example

### Basic Setup

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::AliyunDrive;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create aliyun-drive backend builder.
    let mut builder = AliyunDrive::default()
        // Set the root for aliyun-drive, all operations will happen under this root.
        //
        // NOTE: the root must be absolute path.
        .root("/path/to/dir")
        // Set the client_id. This is required.
        .client_id("client_id")
        // Set the client_secret. This is required.
        .client_secret("client_secret")
        // Set the refresh_token. This is required.
        .refresh_token("refresh_token")
        // Set the drive_type. This is required.
        //
        // Fallback to the default type if no other types found.
        .drive_type("resource");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
