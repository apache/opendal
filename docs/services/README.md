# Services

OpenDAL supports many services, and more services are coming with [Tracking issue of more underlying storage support](https://github.com/datafuselabs/opendal/issues/5).

But services may implement differnt feature sets:

| services  | read | write | list | presign | multipart | blocking |
|-----------|------|-------|------|---------|-----------|----------|
| azblob    | Y    | Y     | Y    | N       | N         | N        |
| azdfs     | Y    | Y     | Y    | N       | N         | N        |
| fs        | Y    | Y     | Y    | X       | X         | Y        |
| ftp       | Y    | Y     | Y    | X       | X         | N        |
| gcs       | Y    | Y     | Y    | N       | N         | N        |
| ghac      | Y    | Y     | N    | X       | X         | N        |
| hdfs      | Y    | Y     | Y    | X       | X         | Y        |
| http      | Y    | Y     | N    | N       | X         | N        |
| ipfs      | Y    | Y     | Y    | Y       | X         | N        |
| ipmfs     | Y    | Y     | Y    | Y       | X         | N        |
| memcached | Y    | Y     | X    | X       | X         | N        |
| memory    | Y    | Y     | X    | X       | X         | N        |
| moka      | Y    | Y     | X    | X       | X         | N        |
| obs       | Y    | Y     | Y    | N       | N         | N        |
| oss       | Y    | Y     | Y    | N       | N         | N        |
| redis     | Y    | Y     | X    | X       | X         | N        |
| rocksdb   | Y    | Y     | X    | X       | X         | N        |
| s3        | Y    | Y     | Y    | Y       | Y         | N        |
| webdav    | Y    | Y     | Y    | X       | X         | N        |

- `Y` means the feature has been implemented.
- `N` means the feature is not implemented for now. Please feel free to open an issue to request it.
- `X` means the feature can't be implemented. Please report an issue if you think it's wrong.
