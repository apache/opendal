As known as `abfs`, `azdls` or `azdls`.

This service will visit the [ABFS](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver) URI supported by [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction).

## Notes

`azdls` is different from `azfile` service which used to visit [Azure File Storage](https://azure.microsoft.com/en-us/services/storage/files/).

## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [x] rename
- [x] list
- [ ] presign
- [ ] blocking

## Configuration

- `root`: Set the work dir for backend.
- `filesystem`: Set the filesystem name for backend.
- `endpoint`: Set the endpoint for backend.
- `account_name`: Set the account_name for backend.
- `account_key`: Set the account_key for backend.

Refer to public API docs for more information.

## Example

### Via Builder

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Azdls;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create azdls backend builder.
    let mut builder = Azdls::default()
        // Set the root for azdls, all operations will happen under this root.
        //
        // NOTE: the root must be absolute path.
        .root("/path/to/dir")
        // Set the filesystem name, this is required.
        .filesystem("test")
        // Set the endpoint, this is required.
        //
        // For examples:
        // - "https://accountname.dfs.core.windows.net"
        .endpoint("https://accountname.dfs.core.windows.net")
        // Set the account_name and account_key.
        //
        // OpenDAL will try load credential from the env.
        // If credential not set and no valid credential in env, OpenDAL will
        // send request without signing like anonymous user.
        .account_name("account_name")
        .account_key("account_key");

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
