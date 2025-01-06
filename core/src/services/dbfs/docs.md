This service will visit the [DBFS API](https://docs.databricks.com/api/azure/workspace/dbfs) supported by [Databricks File System](https://docs.databricks.com/en/dbfs/index.html).

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
- [ ] ~~presign~~
- [ ] blocking

## Configurations

- `root`: Set the work directory for backend.
- `endpoint`: Set the endpoint for backend.
- `token`: Databricks personal access token.

Refer to [`DbfsBuilder`]'s public API docs for more information.

## Examples

### Via Builder

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Dbfs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Dbfs::default()
        // set the root for Dbfs, all operations will happen under this root
        //
        // Note:
        // if the root is not exists, the builder will automatically create the
        // root directory for you
        // if the root exists and is a directory, the builder will continue working
        // if the root exists and is a folder, the builder will fail on building backend
        .root("/path/to/dir")
        // set the endpoint of Dbfs workspace
        .endpoint("https://adb-1234567890123456.78.azuredatabricks.net")
        // set the personal access token for builder
        .token("access_token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
