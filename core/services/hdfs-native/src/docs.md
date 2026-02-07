A distributed file system that provides high-throughput access to application data.
Using [Native Rust HDFS client](https://github.com/Kimahriman/hdfs-native).

## Capabilities

This service can be used to:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [x] rename
- [ ] ~~presign~~

## Configuration

- `root`: Set the work dir for backend.
- `name_node`: Set the name node for backend.
- `enable_append`: enable the append capacity. Default is false.
