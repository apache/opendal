# Apache OpenDAL™ Rust Core: One Layer, All Storage.

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal/ci_core.yml?branch=main
[actions]: https://github.com/apache/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/opendal.svg
[crates.io]: https://crates.io/crates/opendal
[crate downloads]: https://img.shields.io/crates/d/opendal.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

Apache OpenDAL™ is an Open Data Access Layer that enables seamless interaction with diverse storage services.

<img src="https://opendal.apache.org/img/architectural.png" alt="OpenDAL Architectural" width="61.8%" />

## Useful Links

- Documentation: [release](https://docs.rs/opendal/) | [dev](https://opendal.apache.org/docs/rust/opendal/)
- [Examples](./examples)
- [Release Notes](https://docs.rs/opendal/latest/opendal/docs/changelog/index.html)
- [Upgrade Guide](https://docs.rs/opendal/latest/opendal/docs/upgrade/index.html)
- [RFC List](https://docs.rs/opendal/latest/opendal/docs/rfcs/index.html)

## Services

OpenDAL supports the following storage [services](https://docs.rs/opendal/latest/opendal/services/index.html):

| Type                           | Services                                                                                                                                 | 
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------| 
| Standard Storage Protocols     | ftp http [sftp] [webdav]                                                                                                                 |
| Object Storage Services        | [azblob] [cos] [gcs] [obs] [oss] [s3] <br> [b2] [openstack_swift] [upyun] [vercel_blob]                                                  |
| File Storage Services          | fs [alluxio] [azdls] [azfile] [compfs] <br> [dbfs] [gridfs] [hdfs] [hdfs_native] [ipfs] [webhdfs]                                        |
| Consumer Cloud Storage Service | [aliyun_drive] [gdrive] [onedrive] [dropbox] [icloud] [koofr] <br> [pcloud] [seafile] [yandex_disk]                                      |
| Key-Value Storage Services     | [cacache] [cloudflare_kv] [dashmap] memory [etcd] <br> [foundationdb] [persy] [redis] [rocksdb] [sled] <br> [redb] [tikv] [atomicserver] |
| Database Storage Services      | [d1] [mongodb] [mysql] [postgresql] [sqlite] [surrealdb]                                                                                 |
| Cache Storage Services         | [ghac] [memcached] [mini_moka] [moka] [vercel_artifacts]                                                                                 |
| Git Based Storage Services     | [huggingface]                                                                                                                            |

[sftp]: https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02
[webdav]: https://datatracker.ietf.org/doc/html/rfc4918

[azblob]: https://azure.microsoft.com/en-us/services/storage/blobs/
[cos]: https://www.tencentcloud.com/products/cos
[gcs]: https://cloud.google.com/storage
[obs]: https://www.huaweicloud.com/intl/en-us/product/obs.html
[oss]: https://www.aliyun.com/product/oss
[s3]: https://aws.amazon.com/s3/
[b2]: https://www.backblaze.com/
[openstack_swift]: https://docs.openstack.org/swift/latest/
[upyun]: https://www.upyun.com/
[vercel_blob]: https://vercel.com/docs/storage/vercel-blob

[alluxio]: https://docs.alluxio.io/os/user/stable/en/api/REST-API.html
[azdls]: https://azure.microsoft.com/en-us/products/storage/data-lake-storage/
[azfile]: https://learn.microsoft.com/en-us/rest/api/storageservices/file-service-rest-api
[compfs]: https://github.com/compio-rs/compio/
[dbfs]: https://docs.databricks.com/en/dbfs/index.html
[gridfs]: https://www.mongodb.com/docs/manual/core/gridfs/
[hdfs]: https://hadoop.apache.org/docs/r3.3.4/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
[hdfs_native]: https://github.com/Kimahriman/hdfs-native
[ipfs]: https://ipfs.tech/
[webhdfs]: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html

[aliyun_drive]: https://www.aliyundrive.com/
[gdrive]: https://www.google.com/drive/
[onedrive]: https://www.microsoft.com/en-us/microsoft-365/onedrive/online-cloud-storage
[dropbox]: https://www.dropbox.com/
[icloud]: https://www.icloud.com/iclouddrive
[koofr]: https://koofr.eu/
[pcloud]: https://www.pcloud.com/
[seafile]: https://www.seafile.com/
[yandex_disk]: https://360.yandex.com/disk/

[cacache]: https://crates.io/crates/cacache
[cloudflare_kv]: https://developers.cloudflare.com/kv/
[dashmap]: https://github.com/xacrimon/dashmap
[etcd]: https://etcd.io/
[foundationdb]: https://www.foundationdb.org/
[persy]: https://crates.io/crates/persy
[redis]: https://redis.io/
[rocksdb]: http://rocksdb.org/
[sled]: https://crates.io/crates/sled
[redb]: https://crates.io/crates/redb
[tikv]: https://tikv.org/
[atomicserver]: https://github.com/atomicdata-dev/atomic-server

[d1]: https://developers.cloudflare.com/d1/
[mongodb]: https://www.mongodb.com/
[mysql]: https://www.mysql.com/
[postgresql]: https://www.postgresql.org/
[sqlite]: https://www.sqlite.org/
[surrealdb]: https://surrealdb.com/

[ghac]: https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows
[memcached]: https://memcached.org/
[mini_moka]: https://github.com/moka-rs/mini-moka
[moka]: https://github.com/moka-rs/moka
[vercel_artifacts]: https://vercel.com/docs/concepts/monorepos/remote-caching

[huggingface]: https://huggingface.co/

## Layers

OpenDAL supports the following storage [layers](https://docs.rs/opendal/latest/opendal/layers/index.html) to extend the behavior:

| Name                      | Depends                  | Description                                                                           |
|---------------------------|--------------------------|---------------------------------------------------------------------------------------|
| [`AsyncBacktraceLayer`]   | [async-backtrace]        | Add Efficient, logical 'stack' traces of async functions for the underlying services. |
| [`AwaitTreeLayer`]        | [await-tree]             | Add a Instrument await-tree for actor-based applications to the underlying services.  |
| [`BlockingLayer`]         | [tokio]                  | Add blocking API support for non-blocking services.                                   |
| [`ChaosLayer`]            | [rand]                   | Inject chaos into underlying services for robustness test.                            |
| [`ConcurrentLimitLayer`]  | [tokio]                  | Add concurrent request limit.                                                         |
| [`DtraceLayer`]           | [probe]                  | Support User Statically-Defined Tracing(aka USDT) on Linux                            |
| [`LoggingLayer`]          | [log]                    | Add log for every operations.                                                         |
| [`MetricsLayer`]          | [metrics]                | Add metrics for every operations.                                                     |
| [`MimeGuessLayer`]        | [mime_guess]             | Add `Content-Type` automatically based on the file extension in the operation path.   |
| [`FastraceLayer`]         | [fastrace]               | Add fastrace for every operations.                                                    |
| [`OtelMetricsLayer`]      | [opentelemetry::metrics] | Add opentelemetry::metrics for every operations.                                      |
| [`OtelTraceLayer`]        | [opentelemetry::trace]   | Add opentelemetry::trace for every operations.                                        |
| [`PrometheusClientLayer`] | [prometheus_client]      | Add prometheus metrics for every operations.                                          |
| [`PrometheusLayer`]       | [prometheus]             | Add prometheus metrics for every operations.                                          | 
| [`RetryLayer`]            | [backon]                 | Add retry for temporary failed operations.                                            |
| [`ThrottleLayer`]         | [governor]               | Add a bandwidth rate limiter to the underlying services.                              |
| [`TimeoutLayer`]          | [tokio]                  | Add timeout for every operations to avoid slow or unexpected hang operations.         |
| [`TracingLayer`]          | [tracing]                | Add tracing for every operations.                                                     |

[`AsyncBacktraceLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.AsyncBacktraceLayer.html
[async-backtrace]: https://github.com/tokio-rs/async-backtrace
[`AwaitTreeLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.AwaitTreeLayer.html
[await-tree]: https://github.com/risingwavelabs/await-tree
[`BlockingLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.BlockingLayer.html
[tokio]: https://github.com/tokio-rs/tokio
[`ChaosLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.ChaosLayer.html
[rand]: https://github.com/rust-random/rand
[`ConcurrentLimitLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.ConcurrentLimitLayer.html
[`DtraceLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.DtraceLayer.html
[probe]: https://github.com/cuviper/probe-rs
[`LoggingLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html
[log]: https://github.com/rust-lang/log
[`MetricsLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.MetricsLayer.html
[metrics]: https://github.com/metrics-rs/metrics
[`MimeGuessLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.MimeGuessLayer.html
[mime_guess]: https://github.com/abonander/mime_guess
[`FastraceLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.FastraceLayer.html
[fastrace]: https://github.com/fastracelabs/fastrace
[`OtelMetricsLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.OtelMetricsLayer.html
[`OtelTraceLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.OtelTraceLayer.html
[opentelemetry::trace]: https://docs.rs/opentelemetry/latest/opentelemetry/trace/index.html
[`PrometheusClientLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.PrometheusClientLayer.html
[prometheus_client]: https://github.com/prometheus/client_rust
[`PrometheusLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.PrometheusLayer.html
[prometheus]: https://github.com/tikv/rust-prometheus
[`RetryLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.RetryLayer.html
[backon]: https://github.com/Xuanwo/backon
[`ThrottleLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.ThrottleLayer.html
[governor]: https://github.com/boinkor-net/governor
[`TimeoutLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.TimeoutLayer.html
[`TracingLayer`]: https://docs.rs/opendal/latest/opendal/layers/struct.TracingLayer.html
[tracing]: https://github.com/tokio-rs/tracing

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

| Name                | Description                                                   |
|---------------------|---------------------------------------------------------------|
| [Basic]             | Show how to use opendal to operate storage service.           |
| [Concurrent Upload] | Show how to perform upload concurrently to a storage service. |
| [Multipart Upload]  | Show how to perform a multipart upload to a storage service.  |

[Basic]: ./examples/basic
[Concurrent Upload]: ./examples/concurrent-upload
[Multipart Upload]: ./examples/multipart-upload

## Contributing

Check out the [CONTRIBUTING](./CONTRIBUTING.md) guide for more details on getting started with contributing to this project.

## Used by

Check out the [users](./users.md) list for more details on who is using OpenDAL.

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
