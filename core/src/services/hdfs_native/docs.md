A distributed file system that provides high-throughput access to application data.

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
- [ ] ~~scan~~
- [ ] ~~presign~~
- [x] blocking
- [x] append

## Differences with webhdfs

[Webhdfs][crate::services::Webhdfs] is powered by hdfs's RESTful HTTP API.

## Features

Native HDFS support needs to enable feature `services-native-hdfs`.

## Configuration

- `root`: Set the work dir for backend.
- `name_node`: Set the name node for backend.
- `enable_append`: enable the append capacity. Default is false.

Refer to [`HdfsNativeBuilder`]'s public API docs for more information.

## Environment

If HDFS has High Availability (HA) enabled with multiple available NameNodes, some configuration is required:
1. Obtain the entire HDFS config folder (usually located at HADOOP_HOME/etc/hadoop).
2. Set the environment variable HADOOP_CONF_DIR to the path of this folder.
```shell
export HADOOP_CONF_DIR=<path of the config folder>
```
3. Use the `cluster_name` specified in the `core-site.xml` file (located in the HADOOP_CONF_DIR folder) to replace namenode:port.

```rust
builder.name_node("hdfs://cluster_name");
```

## Example

### Via Builder

```rust
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Hdfs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create native hdfs backend builder.
    let mut builder = HdfsNative::default();
    // Set the url for hdfs.
    builder.name_node("hdfs://127.0.0.1:9000");
    // Set the root for hdfs, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root("/tmp");
    
    // Enable the append capacity for hdfs. 
    // 
    // Note: HDFS run in non-distributed mode doesn't support append.
    builder.enable_append(true);

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
