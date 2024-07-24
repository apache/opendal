## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [ ] ~~list~~
- [ ] ~~presign~~
- [x] blocking

## Note

OpenDAL will build rocksdb from source by default.

To link with existing rocksdb lib, please set one of the following:

- `ROCKSDB_LIB_DIR` to the dir that contains `librocksdb.so`
- `ROCKSDB_STATIC` to the dir that contains `librocksdb.a`

If the version of RocksDB is below 6.0, you may encounter compatibility
issues. It is advisable to follow the steps provided in the [`INSTALL`](https://github.com/facebook/rocksdb/blob/main/INSTALL.md)
file to build rocksdb, rather than relying on system libraries that
may be outdated and incompatible.

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `datadir`: Set the path to the rocksdb data directory

You can refer to [`RocksdbBuilder`]'s docs for more information.

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Rocksdb;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Rocksdb::default()
        .datadir("/tmp/opendal/rocksdb");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
