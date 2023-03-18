# OpenDAL &emsp; [![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/incubator-opendal/ci.yml?branch=main
[actions]: https://github.com/apache/incubator-opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/opendal.svg
[crates.io]: https://crates.io/crates/opendal
[crate downloads]: https://img.shields.io/crates/d/opendal.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://discord.gg/XQy8yGR2dg

**Open** **D**ata **A**ccess **L**ayer: Access data freely, painlessly, and efficiently

- Documentation: [stable](https://docs.rs/opendal/) | [main](https://opendal.apache.org/docs/rust/opendal/)
- [Release notes](https://docs.rs/opendal/latest/opendal/docs/changelog/index.html)

![](https://user-images.githubusercontent.com/5351546/222356748-14276998-501b-4d2a-9b09-b8cff3018204.png)

## Services

- [azblob](https://docs.rs/opendal/latest/opendal/services/struct.Azblob.html): [Azure Storage Blob](https://azure.microsoft.com/en-us/services/storage/blobs/) services.
- [azdfs](https://docs.rs/opendal/latest/opendal/services/struct.Azdfs.html): [Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/) services. (As known as [abfs](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver))
- [dashmap](https://docs.rs/opendal/latest/opendal/services/struct.Dashmap.html): [dashmap](https://github.com/xacrimon/dashmap) backend support.
- [fs](https://docs.rs/opendal/latest/opendal/services/struct.Fs.html): POSIX alike file system.
- [ftp](https://docs.rs/opendal/latest/opendal/services/struct.Ftp.html): FTP and FTPS support.
- [gcs](https://docs.rs/opendal/latest/opendal/services/struct.Gcs.html): [Google Cloud Storage](https://cloud.google.com/storage) Service.
- [ghac](https://docs.rs/opendal/latest/opendal/services/struct.Ghac.html): [Github Action Cache](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows) Service.
- [hdfs](https://docs.rs/opendal/latest/opendal/services/struct.Hdfs.html): [Hadoop Distributed File System](https://hadoop.apache.org/docs/r3.3.4/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)(HDFS).
- [http](https://docs.rs/opendal/latest/opendal/services/struct.Http.html): HTTP read-only services.
- [ipfs](https://docs.rs/opendal/latest/opendal/services/struct.Ipfs.html): [InterPlanetary File System](https://ipfs.tech/) HTTP Gateway support.
- [ipmfs](https://docs.rs/opendal/latest/opendal/services/struct.Ipmfs.html): [InterPlanetary File System](https://ipfs.tech/) MFS API support.
- [memcached](https://docs.rs/opendal/latest/opendal/services/struct.Memcached.html): [Memcached](https://memcached.org/) service support.
- [memory](https://docs.rs/opendal/latest/opendal/services/struct.Memory.html): In memory backend.
- [moka](https://docs.rs/opendal/latest/opendal/services/struct.Moka.html): [moka](https://github.com/moka-rs/moka) backend support.
- [obs](https://docs.rs/opendal/latest/opendal/services/struct.Obs.html): [Huawei Cloud Object Storage](https://www.huaweicloud.com/intl/en-us/product/obs.html) Service (OBS).
- [oss](https://docs.rs/opendal/latest/opendal/services/struct.Oss.html): [Aliyun Object Storage Service](https://www.aliyun.com/product/oss) (OSS).
- [redis](https://docs.rs/opendal/latest/opendal/services/struct.Redis.html): [Redis](https://redis.io/) services support.
- [rocksdb](https://docs.rs/opendal/latest/opendal/services/struct.Rocksdb.html): [RocksDB](http://rocksdb.org/) services support.
- [s3](https://docs.rs/opendal/latest/opendal/services/struct.S3.html): [AWS S3](https://aws.amazon.com/s3/) alike services.
- [sled](https://docs.rs/opendal/latest/opendal/services/sled/struct.Sled.html): [sled](https://crates.io/crates/sled) services support.
- [webdav](https://docs.rs/opendal/latest/opendal/services/struct.Webdav.html): [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) Service Support.
- [webhdfs](https://docs.rs/opendal/latest/opendal/services/struct.Webhdfs.html): [WebHDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) Service Support.

## Features

Access data **freely**

- Access different storage services in the same way
- Behavior tests for all services
- Cross language/project bindings
  - [Python](../bindings/python/)
  - [Node.js](../bindings/nodejs/)
  - [object_store](../bindings/object_store/)

Access data **painlessly**

- **100%** documents covered
- Powerful [`Layers`](https://docs.rs/opendal/latest/opendal/layers/index.html)
- Automatic [retry](https://docs.rs/opendal/latest/opendal/layers/struct.RetryLayer.html) support
- Full observability: [logging](https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html), [tracing](https://docs.rs/opendal/latest/opendal/layers/struct.TracingLayer.html), [metrics](https://docs.rs/opendal/latest/opendal/layers/struct.MetricsLayer.html).
- [Native chaos testing](https://docs.rs/opendal/latest/opendal/layers/struct.ChaosLayer.html)

Access data **efficiently**

- Zero cost: Maps to API calls directly
- Best effort: Automatically selects best read/seek/next based on services
- Avoid extra calls: Reuses metadata when possible

## Quickstart

```rust
use opendal::Result;
use opendal::layers::LoggingLayer;
use opendal::services;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Pick a builder and configure it.
    let mut builder = services::S3::default();
    builder.bucket("test");

    // Init an operator
    let op = Operator::create(builder)?
        // Init with logging layer enabled.
        .layer(LoggingLayer::default())
        .finish();

    // Write data
    op.write("hello.txt", "Hello, World!").await?;

    // Read data
    let bs = op.read("hello.txt").await?;

    // Fetch metadata
    let meta = op.stat("hello.txt").await?;
    let mode = meta.mode();
    let length = meta.content_length();

    // Delete
    op.delete("hello.txt").await?;

    Ok(())
}
```

## Contributing

Check out the [CONTRIBUTING](CONTRIBUTING.md) guide for more details on getting started with contributing to this project.

## Getting help

Submit [issues](https://github.com/apache/incubator-opendal/issues/new) for bug report or asking questions in the [Discussions forum](https://github.com/apache/incubator-opendal/discussions/new?category=q-a).

Talk to develops at [discord].

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
