There two implementations of WebHDFS REST API:

- Native via HDFS Namenode and Datanode, data are transferred between nodes directly.
- [HttpFS](https://hadoop.apache.org/docs/stable/hadoop-hdfs-httpfs/index.html) is a gateway before hdfs nodes, data are proxied.

## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [x] list
- [ ] ~~scan~~
- [ ] ~~presign~~
- [ ] blocking

## Differences with HDFS

[Hdfs][crate::services::Hdfs] is powered by HDFS's native java client. Users need to set up the HDFS services correctly. But webhdfs can access from HTTP API and no extra setup needed.

## Configurations

- `root`: The root path of the WebHDFS service.
- `endpoint`: The endpoint of the WebHDFS service.
- `delegation`: The delegation token for WebHDFS.
- `atomic_write_dir`: The tmp write dir of multi write for WebHDFS.

Refer to [`Builder`]'s public API docs for more information.

## Examples

### Via Builder

```rust
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Webhdfs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Webhdfs::default();
    // set the root for WebHDFS, all operations will happen under this root
    //
    // Note:
    // if the root is not exists, the builder will automatically create the
    // root directory for you
    // if the root exists and is a directory, the builder will continue working
    // if the root exists and is a folder, the builder will fail on building backend
    builder.root("/path/to/dir");
    // set the endpoint of webhdfs namenode, controlled by dfs.namenode.http-address
    // default is http://127.0.0.1:9870
    builder.endpoint("http://127.0.0.1:9870");
    // set the delegation_token for builder
    builder.delegation("delegation_token");
    // set atomic_write_dir for builder
    builder.atomic_write_dir(".opendal_tmp/");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
