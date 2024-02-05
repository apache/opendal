# Apache OpenDAL™ 

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal/ci_core.yml?branch=main
[actions]: https://github.com/apache/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/opendal.svg
[crates.io]: https://crates.io/crates/opendal
[crate downloads]: https://img.shields.io/crates/d/opendal.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://discord.gg/XQy8yGR2dg

**OpenDAL** is a data access layer that allows users to easily and efficiently retrieve data from various storage services in a unified way.

- Documentation: [stable](https://docs.rs/opendal/) | [main](https://opendal.apache.org/docs/rust/opendal/)
- [Release notes](https://docs.rs/opendal/latest/opendal/docs/changelog/index.html)

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Services

<details>
<summary>Standard Storage Protocols (like ftp, webdav)</summary>

- ftp: FTP and FTPS
- http: HTTP read-only services
- sftp: [SFTP](https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02) services *being worked on*
- webdav: [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) Service

</details>

<details>
<summary>Object Storage Services (like s3, gcs, azblob)</summary>

- azblob: [Azure Storage Blob](https://azure.microsoft.com/en-us/services/storage/blobs/) services
- cos: [Tencent Cloud Object Storage](https://www.tencentcloud.com/products/cos) services
- gcs: [Google Cloud Storage](https://cloud.google.com/storage) Service
- obs: [Huawei Cloud Object Storage](https://www.huaweicloud.com/intl/en-us/product/obs.html) Service (OBS)
- oss: [Aliyun Object Storage Service](https://www.aliyun.com/product/oss) (OSS)
- s3: [AWS S3](https://aws.amazon.com/s3/) alike services
- supabase: [Supabase Storage](https://supabase.com/docs/guides/storage) Service *being worked on*
- wasabi: [Wasabi](https://wasabi.com/) Cloud Storage

</details>

<details>
<summary>File Storage Services (like fs, azdls, hdfs)</summary>

- fs: POSIX alike file system
- azdls: [Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/) services (As known as [abfs](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver))
- hdfs: [Hadoop Distributed File System](https://hadoop.apache.org/docs/r3.3.4/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)(HDFS)
- ipfs: [InterPlanetary File System](https://ipfs.tech/) HTTP Gateway
- ipmfs: [InterPlanetary File System](https://ipfs.tech/) MFS API *being worked on*
- webhdfs: [WebHDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) Service

</details>

<details>
<summary>Consumer Cloud Storage Service (like gdrive, onedrive, icloud drive)</summary>

- gdrive: [Google Drive](https://www.google.com/drive/) *being worked on*
- onedrive: [OneDrive](https://www.microsoft.com/en-us/microsoft-365/onedrive/online-cloud-storage) *being worked on*
- icloud: [Icloud Drive](https://www.icloud.com.cn/iclouddrive/) *being worked on*

</details>

<details>
<summary>Key-Value Storage Service (like rocksdb, sled)</summary>

- cacache: [cacache](https://crates.io/crates/cacache) backend
- dashmap: [dashmap](https://github.com/xacrimon/dashmap) backend
- memory: In memory backend
- persy: [persy](https://crates.io/crates/persy) backend
- redis: [Redis](https://redis.io/) services
- rocksdb: [RocksDB](http://rocksdb.org/) services
- sled: [sled](https://crates.io/crates/sled) backend
- redb: [redb](https://crates.io/crates/redb) backend
- tikv: [tikv](https://tikv.org/) backend
- atomicserver: [Atomicserver](https://github.com/atomicdata-dev/atomic-server) services

</details>

<details>
<summary>Cache Storage Service (like memcached, moka)</summary>

- ghac: [GitHub Action Cache](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows) Service
- memcached: [Memcached](https://memcached.org/) service
- mini_moka: [Mini Moka](https://github.com/moka-rs/mini-moka) backend
- moka: [Moka](https://github.com/moka-rs/moka) backend
- vercel_artifacts: [Vercel Remote Caching](https://vercel.com/docs/concepts/monorepos/remote-caching) Service *being worked on*

</details>

> Welcome to add any services that are not currently supported [here](https://github.com/apache/opendal/issues/5).

## Features

Access data **freely**

- Access different storage services in the same way
- Behavior tests for all services

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
    let op = Operator::new(builder)?
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

## Examples

The examples are available at [here](../examples/rust).

## Contributing

Check out the [CONTRIBUTING](CONTRIBUTING.md) guide for more details on getting started with contributing to this project.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
