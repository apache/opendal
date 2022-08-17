# Services

This section will demonstrate how to init and start using a service.

OpenDAL can init services via environment or builder.

- Via Builder: use different backends' `Builder` API.
- Via Environment: use `Operator::from_env()` API.

OpenDAL supports the following services:

- [azblob](./azblob.md): Azure blob storage service
- [fs](./fs.md): POSIX alike file system
- [hdfs](./hdfs.md): Hadoop Distributed File System
- [s3](./s3.md): AWS S3 compatible storage service
- [gcs](./gcs.md): Google Cloud Storage service
