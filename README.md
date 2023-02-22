# OpenDAL &emsp; [![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io]

[build status]: https://img.shields.io/github/actions/workflow/status/datafuselabs/opendal/ci.yml?branch=main
[actions]: https://github.com/datafuselabs/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/opendal.svg
[crates.io]: https://crates.io/crates/opendal
[crate downloads]: https://img.shields.io/crates/d/opendal.svg

**Open** **D**ata **A**ccess **L**ayer: Access data freely, painlessly, and efficiently

- Documentation: [stable](https://docs.rs/opendal/) | [main](https://opendal.databend.rs/opendal/)
- [Release notes](https://docs.rs/opendal/latest/opendal/docs/changelog/index.html)

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
- Cross language/project bindings (working on)
  - [Python](./bindings/python/)
  - [Node.js](./bindings/nodejs/)
  - [object_store](./bindings/object_store/)

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

    // Create object handler.
    let o = op.object("test_file");

    // Write data
    o.write("Hello, World!").await?;

    // Read data
    let bs = o.read().await?;

    // Fetch metadata
    let meta = o.stat().await?;
    let mode = meta.mode();
    let length = meta.content_length();

    // Delete
    o.delete().await?;

    Ok(())
}
```

More examples could be found at [Documentation](https://opendal.databend.rs).

## Projects

- [Databend](https://github.com/datafuselabs/databend/): A modern Elasticity and Performance cloud data warehouse.
- [GreptimeDB](https://github.com/GreptimeTeam/greptimedb): An open-source, cloud-native, distributed time-series database.
- [deepeth/mars](https://github.com/deepeth/mars): The powerful analysis platform to explore and visualize data from blockchain.
- [mozilla/sccache](https://github.com/mozilla/sccache/): sccache is ccache with cloud storage
- [risingwave](https://github.com/risingwavelabs/risingwave): A Distributed SQL Database for Stream Processing

## Contributing

Check out the [CONTRIBUTING.md](./CONTRIBUTING.md) guide for more details on getting started with contributing to this project.

## Getting help

Submit [issues](https://github.com/datafuselabs/opendal/issues/new/choose) for bug report or asking questions in [discussion](https://github.com/datafuselabs/opendal/discussions/new?category=q-a).

#### License

<sup>
Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.
</sup>
