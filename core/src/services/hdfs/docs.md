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
- [ ] ~~presign~~
- [x] blocking
- [x] append

## Differences with webhdfs

[Webhdfs][crate::services::Webhdfs] is powered by hdfs's RESTful HTTP API.

## Features

HDFS support needs to enable feature `services-hdfs`.

## Configuration

- `root`: Set the work dir for backend.
- `name_node`: Set the name node for backend.
- `kerberos_ticket_cache_path`: Set the kerberos ticket cache path for backend, this should be gotten by `klist` after `kinit`
- `user`: Set the user for backend
- `enable_append`: enable the append capacity. Default is false. 

Refer to [`HdfsBuilder`]'s public API docs for more information.

## Environment

HDFS needs some environment set correctly.

- `JAVA_HOME`: the path to java home, could be found via `java -XshowSettings:properties -version`
- `HADOOP_HOME`: the path to hadoop home, opendal relays on this env to discover hadoop jars and set `CLASSPATH` automatically.

Most of the time, setting `JAVA_HOME` and `HADOOP_HOME` is enough. But there are some edge cases:

- If meeting errors like the following:

```shell
error while loading shared libraries: libjvm.so: cannot open shared object file: No such file or directory
```

Java's lib are not including in pkg-config find path, please set `LD_LIBRARY_PATH`:

```shell
export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
```

The path of `libjvm.so` could be different, please keep an eye on it.

- If meeting errors like the following:

```shell
(unable to get stack trace for java.lang.NoClassDefFoundError exception: ExceptionUtils::getStackTrace error.)
```

`CLASSPATH` is not set correctly or your hadoop installation is incorrect.

To set `CLASSPATH`:
```shell
export CLASSPATH=$(find $HADOOP_HOME -iname "*.jar" | xargs echo | tr ' ' ':'):${CLASSPATH}
```

- If HDFS has High Availability (HA) enabled with multiple available NameNodes, some configuration is required:
1. Obtain the entire HDFS config folder (usually located at HADOOP_HOME/etc/hadoop).
2. Set the environment variable HADOOP_CONF_DIR to the path of this folder.
```shell
export HADOOP_CONF_DIR=<path of the config folder>
```
3. Append the HADOOP_CONF_DIR to the `CLASSPATH`
```shell
export CLASSPATH=$HADOOP_CONF_DIR:$HADOOP_CLASSPATH:$CLASSPATH
```
4. Use the `cluster_name` specified in the `core-site.xml` file (located in the HADOOP_CONF_DIR folder) to replace namenode:port.

```ignore
builder.name_node("hdfs://cluster_name");
```

### macOS Specific Note

If you encounter an issue during the build process on macOS with an error message similar to:

```shell
ld: unknown file type in $HADOOP_HOME/lib/native/libhdfs.so.0.0.0
clang: error: linker command failed with exit code 1 (use -v to see invocation)
```
This error is likely due to the fact that the official Hadoop build includes the libhdfs.so file for the x86-64 architecture, which is not compatible with aarch64 architecture required for MacOS.

To resolve this issue, you can add hdrs as a dependency in your Rust application's Cargo.toml file, and enable the vendored feature:

```toml
[dependencies]
hdrs = { version = "<version_number>", features = ["vendored"] }
```
Enabling the vendored feature ensures that hdrs includes the necessary libhdfs.so library built for the correct architecture.

## Example

### Via Builder

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Hdfs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create fs backend builder.
    let mut builder = Hdfs::default()
        // Set the name node for hdfs.
        // If the string starts with a protocol type such as file://, hdfs://, or gs://, this protocol type will be used.
        .name_node("hdfs://127.0.0.1:9000")
        // Set the root for hdfs, all operations will happen under this root.
        //
        // NOTE: the root must be absolute path.
        .root("/tmp")
        
        // Enable the append capacity for hdfs. 
        // 
        // Note: HDFS run in non-distributed mode doesn't support append.
        .enable_append(true);

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
