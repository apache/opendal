# Apache OpenDAL™: *One Layer, All Storage.*

[![GitHub Discussions](https://img.shields.io/github/discussions/apache/opendal)](https://github.com/apache/opendal/discussions)
[![Discord](https://img.shields.io/discord/1081052318650339399?logo=discord&label=discord)](https://opendal.apache.org/discord)
[![DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/apache/opendal)

Apache OpenDAL™ (`/ˈoʊ.pən.dæl/`, pronounced "OH-puhn-dal") is an Open Data Access Layer that gives every language a unified way to access object storage, file storage, cloud SaaS, databases, protocols, and key-value services.

Apache OpenDAL™ is guided by its vision of **One Layer, All Storage** and its core principles: **Open Community**, **Solid Foundation**, **Fast Access**, **Object Storage First**, and **Extensible Architecture**. Read the explained vision at [OpenDAL Vision](https://opendal.apache.org/vision).

<img src="./website/static/img/architectural.png" alt="OpenDAL Architecture" width="100%" />

## At a Glance

- Project: Apache OpenDAL™
- Vision: **One Layer, All Storage**
- Core package: Rust crate [`opendal`][Rust Core Link]
- Main abstraction: `Operator`
- Extension points: language bindings, layers, and services
- Common layers: retry, timeout, logging, tracing, metrics, throttling, and concurrency control
- Access targets: object storage, file systems, cloud SaaS, databases, protocols, and key-value services

## Why OpenDAL

Apache OpenDAL™ turns the vision of **One Layer, All Storage** into a practical data access layer for applications, libraries, and data systems.

- **Zero-cost core**: built in Rust with composable services and layers, so applications only enable the backends and capabilities they use.
- **Production-ready access**: add retry, timeout, logging, tracing, metrics, throttling, and concurrency limits through reusable layers.
- **One API, all storage**: access object storage, file systems, cloud SaaS, databases, protocols, and key-value services through the same interface.
- **Open and extensible**: add new services, layers, and language bindings while keeping the same unified access model.

## Choose Your Language

Start with the binding for your application runtime. Each binding provides access to the same OpenDAL service model while following its language ecosystem.

> **Note**: Each binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to the specific binding's version rather than the core version.

| | | | |
| :---: | :---: | :---: | :---: |
| <img src="./website/static/img/bindings/rust.svg" width="42" height="42" alt="Rust logo"><br>**[Rust Core]**<br>[Package][Rust Core Link] · [Docs][Rust Core Release Docs] · [Dev Docs][Rust Core Dev Docs] | <img src="./website/static/img/bindings/c.svg" width="42" height="42" alt="C logo"><br>**[C Binding]**<br>[Dev Docs][C Binding Dev Docs] | <img src="./website/static/img/bindings/cpp.svg" width="42" height="42" alt="C++ logo"><br>**[Cpp Binding]**<br>[Dev Docs][Cpp Binding Dev Docs] | <img src="./website/static/img/bindings/d.svg" width="42" height="42" alt="D logo"><br>**[D Binding]** |
| <img src="./website/static/img/bindings/dart.svg" width="42" height="42" alt="Dart logo"><br>**[Dart Binding]** | <img src="./website/static/img/bindings/dotnet.svg" width="42" height="42" alt=".NET logo"><br>**[Dotnet Binding]** | <img src="./website/static/img/bindings/go.svg" width="42" height="42" alt="Go logo"><br>**[Go Binding]**<br>[Package][Go Binding Link] · [Docs][Go Release Docs] | <img src="./website/static/img/bindings/haskell.svg" width="42" height="42" alt="Haskell logo"><br>**[Haskell Binding]** |
| <img src="./website/static/img/bindings/java.svg" width="42" height="42" alt="Java logo"><br>**[Java Binding]**<br>[Package][Java Binding Link] · [Docs][Java Binding Release Docs] · [Dev Docs][Java Binding Dev Docs] | <img src="./website/static/img/bindings/lua.svg" width="42" height="42" alt="Lua logo"><br>**[Lua Binding]** | <img src="./website/static/img/bindings/nodejs.svg" width="42" height="42" alt="Node.js logo"><br>**[Node.js Binding]**<br>[Package][Node.js Binding Link] · [Dev Docs][Node.js Binding Dev Docs] | <img src="./website/static/img/bindings/ocaml.svg" width="42" height="42" alt="OCaml logo"><br>**[OCaml Binding]** |
| <img src="./website/static/img/bindings/php.svg" width="42" height="42" alt="PHP logo"><br>**[PHP Binding]** | <img src="./website/static/img/bindings/python.svg" width="42" height="42" alt="Python logo"><br>**[Python Binding]**<br>[Package][Python Binding Link] · [Dev Docs][Python Binding Dev Docs] | <img src="./website/static/img/bindings/ruby.svg" width="42" height="42" alt="Ruby logo"><br>**[Ruby Binding]** | <img src="./website/static/img/bindings/swift.svg" width="42" height="42" alt="Swift logo"><br>**[Swift Binding]** |
| <img src="./website/static/img/bindings/zig.svg" width="42" height="42" alt="Zig logo"><br>**[Zig Binding]** |  |  |  |

## Choose Your Layers

Add layers when your application needs cross-service behavior such as retries, timeouts, observability, or traffic control.

| | | | |
| :---: | :---: | :---: | :---: |
| <img src="./website/static/img/layers/retry.svg" width="42" height="42" alt="Retry icon"><br>**[RetryLayer]**<br>Retry temporary failures. | <img src="./website/static/img/layers/timeout.svg" width="42" height="42" alt="Timeout icon"><br>**[TimeoutLayer]**<br>Bound slow or hanging operations. | <img src="./website/static/img/layers/logging.svg" width="42" height="42" alt="Logging icon"><br>**[LoggingLayer]**<br>Emit structured operation logs. | <img src="./website/static/img/layers/tracing.svg" width="42" height="42" alt="Tracing icon"><br>**[TracingLayer]**<br>Trace requests across systems. |
| <img src="./website/static/img/layers/metrics.svg" width="42" height="42" alt="Metrics icon"><br>**[MetricsLayer]**<br>Export operation metrics. | <img src="./website/static/img/layers/prometheus.svg" width="42" height="42" alt="Prometheus icon"><br>**[PrometheusLayer]**<br>Expose Prometheus metrics. | <img src="./website/static/img/layers/otel.svg" width="42" height="42" alt="OpenTelemetry icon"><br>**[OtelMetricsLayer]**<br>Export OpenTelemetry metrics. | <img src="./website/static/img/layers/traffic-control.svg" width="42" height="42" alt="Traffic control icon"><br>**Traffic Control**<br>[ThrottleLayer] · [ConcurrentLimitLayer] |
| <img src="./website/static/img/layers/content-type.svg" width="42" height="42" alt="Content type icon"><br>**[MimeGuessLayer]**<br>Infer `Content-Type` from paths. | <img src="./website/static/img/layers/route.svg" width="42" height="42" alt="Route icon"><br>**[RouteLayer]**<br>Route operations by path. | <img src="./website/static/img/layers/cache.svg" width="42" height="42" alt="Cache icon"><br>**[FoyerLayer]**<br>Add hybrid cache behavior. | <img src="./website/static/img/layers/layers.svg" width="42" height="42" alt="Layers icon"><br>**[All Layers][Layers Docs]**<br>Explore the full layer list. |

Explore all available layers in the [layers documentation][Layers Docs].

## Choose Your Services

Pick the storage services that your application needs. See the full OpenDAL service configuration docs in the [services documentation][Services Docs].

| | | |
| :---: | :---: | :---: |
| <img src="./website/static/img/services/s3.svg" width="44" height="28" alt="AWS logo"> <img src="./website/static/img/services/gcs.svg" width="30" height="30" alt="Google Cloud Storage logo"> <img src="./website/static/img/services/azure.svg" width="30" height="30" alt="Azure Storage logo"> <img src="./website/static/img/services/oss.svg" width="52" height="24" alt="Alibaba Cloud logo"> <img src="./website/static/img/services/obs.png" width="64" height="24" alt="Huawei Cloud logo"> <img src="./website/static/img/services/cos.svg" width="44" height="30" alt="Tencent Cloud logo"> <img src="./website/static/img/services/b2.svg" width="30" height="30" alt="Backblaze logo"> <img src="./website/static/img/services/swift.svg" width="30" height="30" alt="OpenStack logo"> <img src="./website/static/img/services/tos.svg" width="30" height="30" alt="Volcengine logo"> <img src="./website/static/img/services/vercel.svg" width="30" height="30" alt="Vercel logo"><br>**Object Storage**<br>[s3] · [gcs] · [azblob] · [oss] · [obs] · [cos]<br>[b2] · [swift] · [tos] · [upyun] · [vercel-blob] · [openstack_swift] | <img src="./website/static/img/services/fs.svg" width="30" height="30" alt="Filesystem icon"> <img src="./website/static/img/services/hdfs.svg" width="30" height="30" alt="HDFS logo"> <img src="./website/static/img/services/lakefs.svg" width="30" height="30" alt="lakeFS logo"> <img src="./website/static/img/services/ipfs.svg" width="30" height="30" alt="IPFS logo"> <img src="./website/static/img/services/azure.svg" width="30" height="30" alt="Azure logo"> <img src="./website/static/img/services/alluxio.svg" width="30" height="30" alt="Alluxio logo"> <img src="./website/static/img/services/dbfs.svg" width="30" height="30" alt="DBFS logo"> <img src="./website/static/img/services/mongodb.svg" width="30" height="30" alt="GridFS logo"> <img src="./website/static/img/services/opfs.svg" width="30" height="30" alt="OPFS logo"> <img src="./website/static/img/services/goosefs.svg" width="30" height="30" alt="GooseFS logo"><br>**File Storage**<br>[fs] · [hdfs] · [hdfs-native] · [webhdfs] · [lakefs] · [ipfs] · [ipmfs]<br>[azfile] · [azdls] · [alluxio] · [goosefs] · [dbfs] · [gridfs] · [opfs] · [monoiofs] · [compfs] | <img src="./website/static/img/services/gdrive.svg" width="30" height="30" alt="Google Drive logo"> <img src="./website/static/img/services/dropbox.svg" width="30" height="30" alt="Dropbox logo"> <img src="./website/static/img/services/onedrive.svg" width="30" height="30" alt="OneDrive logo"> <img src="./website/static/img/services/aliyun-drive.svg" width="30" height="30" alt="Alibaba Cloud logo"> <img src="./website/static/img/services/huggingface.svg" width="30" height="30" alt="Hugging Face logo"> <img src="./website/static/img/services/github.svg" width="30" height="30" alt="GitHub logo"> <img src="./website/static/img/services/pcloud.svg" width="30" height="30" alt="pCloud logo"> <img src="./website/static/img/services/seafile.svg" width="30" height="30" alt="Seafile logo"> <img src="./website/static/img/services/yandex-disk.svg" width="30" height="30" alt="Yandex Disk logo"> <img src="./website/static/img/services/koofr.svg" width="30" height="30" alt="Koofr logo"><br>**Cloud SaaS**<br>[gdrive] · [dropbox] · [onedrive] · [aliyun-drive] · [huggingface]<br>[github] · [pcloud] · [koofr] · [seafile] · [yandex-disk] |
| <img src="./website/static/img/services/http.svg" width="30" height="30" alt="HTTP icon"> <img src="./website/static/img/services/ftp.svg" width="30" height="30" alt="FTP icon"> <img src="./website/static/img/services/webdav.svg" width="30" height="30" alt="WebDAV icon"> <img src="./website/static/img/services/sftp.svg" width="30" height="30" alt="SFTP icon"><br>**Standard Protocols**<br>[http] · [ftp] · [webdav] · [sftp] | <img src="./website/static/img/services/sqlite.svg" width="30" height="30" alt="SQLite logo"> <img src="./website/static/img/services/mysql.svg" width="30" height="30" alt="MySQL logo"> <img src="./website/static/img/services/postgresql.svg" width="30" height="30" alt="PostgreSQL logo"> <img src="./website/static/img/services/mongodb.svg" width="30" height="30" alt="MongoDB logo"> <img src="./website/static/img/services/surrealdb.svg" width="30" height="30" alt="SurrealDB logo"> <img src="./website/static/img/services/d1.svg" width="30" height="30" alt="Cloudflare D1 logo"><br>**Databases**<br>[sqlite] · [mysql] · [postgresql] · [mongodb] · [surrealdb] · [d1] | <img src="./website/static/img/services/redis.svg" width="30" height="30" alt="Redis logo"> <img src="./website/static/img/services/etcd.svg" width="30" height="30" alt="etcd logo"> <img src="./website/static/img/services/rocksdb.svg" width="30" height="30" alt="RocksDB logo"> <img src="./website/static/img/services/memcached.svg" width="30" height="30" alt="Memcached logo"> <img src="./website/static/img/services/cloudflare-kv.svg" width="30" height="30" alt="Cloudflare KV logo"> <img src="./website/static/img/services/tikv.svg" width="30" height="30" alt="TiKV logo"> <img src="./website/static/img/services/foundationdb.svg" width="30" height="30" alt="FoundationDB logo"> <img src="./website/static/img/services/moka.svg" width="30" height="30" alt="Moka logo"> <img src="./website/static/img/services/foyer.svg" width="30" height="30" alt="Foyer logo"> <img src="./website/static/img/services/ghac.svg" width="30" height="30" alt="GitHub Actions Cache logo"><br>**Key-Value & Embedded**<br>memory · [redis] · [etcd] · [rocksdb] · [memcached] · [cloudflare-kv] · [tikv]<br>[foundationdb] · [sled] · [redb] · [persy] · [dashmap] · [cacache] · [moka] · [mini-moka] · [foyer] · [ghac] · [vercel-artifacts] |

## Examples

See [examples](./examples/) for runnable usage examples.

## Documentation

- Website: <https://opendal.apache.org>
- Vision: <https://opendal.apache.org/vision>
- Rust release docs: <https://docs.rs/opendal>
- Rust dev docs: <https://opendal.apache.org/docs/rust/opendal/>

## Contribute

OpenDAL is an active open-source project. We are always open to people who want to use it or contribute to it. Here are some ways to go.

- Start with [Contributing Guide](CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/opendal/issues/new) for bug report or feature requests.
- Start [Discussions](https://github.com/apache/opendal/discussions/new?category=q-a) for questions or ideas.
- Talk to community directly at [Discord](https://opendal.apache.org/discord).
- Report security vulnerabilities to [private mailing list](mailto:private@opendal.apache.org)

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: <http://www.apache.org/licenses/LICENSE-2.0>

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.

<!-- Link references -->

<!-- Binding references -->

[Rust Core]: core/README.md
[Rust Core Link]: https://crates.io/crates/opendal
[Rust Core Release Docs]: https://docs.rs/opendal
[Rust Core Dev Docs]: https://opendal.apache.org/docs/rust/opendal/
[C Binding]: bindings/c/README.md
[C Binding Dev Docs]: https://opendal.apache.org/docs/c/
[Cpp Binding]: bindings/cpp/README.md
[Cpp Binding Dev Docs]: https://opendal.apache.org/docs/cpp/
[D Binding]: bindings/d/README.md
[Dart Binding]: bindings/dart/README.md
[Dotnet Binding]: bindings/dotnet/README.md
[Go Binding]: bindings/go/README.md
[Go Binding Link]: https://pkg.go.dev/github.com/apache/opendal/bindings/go
[Go Release Docs]: https://pkg.go.dev/github.com/apache/opendal/bindings/go
[Haskell Binding]: bindings/haskell/README.md
[Java Binding]: bindings/java/README.md
[Java Binding Link]: https://central.sonatype.com/artifact/org.apache.opendal/opendal-java
[Java Binding Release Docs]: https://javadoc.io/doc/org.apache.opendal/opendal-java
[Java Binding Dev Docs]: https://opendal.apache.org/docs/java/
[Lua Binding]: bindings/lua/README.md
[Node.js Binding]: bindings/nodejs/README.md
[Node.js Binding Link]: https://www.npmjs.com/package/opendal
[Node.js Binding Dev Docs]: https://opendal.apache.org/docs/nodejs/
[OCaml Binding]: bindings/ocaml/README.md
[PHP Binding]: bindings/php/README.md
[Python Binding]: bindings/python/README.md
[Python Binding Link]: https://pypi.org/project/opendal/
[Python Binding Dev Docs]: https://opendal.apache.org/docs/python/
[Ruby Binding]: bindings/ruby/README.md
[Swift Binding]: bindings/swift/README.md
[Zig Binding]: bindings/zig/README.md

<!-- Layer references -->

[Layers Docs]: https://opendal.apache.org/docs/rust/opendal/layers/
[RetryLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.RetryLayer.html
[TimeoutLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.TimeoutLayer.html
[LoggingLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.LoggingLayer.html
[TracingLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.TracingLayer.html
[MetricsLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.MetricsLayer.html
[PrometheusLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.PrometheusLayer.html
[OtelMetricsLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.OtelMetricsLayer.html
[ThrottleLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.ThrottleLayer.html
[ConcurrentLimitLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.ConcurrentLimitLayer.html
[MimeGuessLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.MimeGuessLayer.html
[RouteLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.RouteLayer.html
[FoyerLayer]: https://opendal.apache.org/docs/rust/opendal/layers/struct.FoyerLayer.html

<!-- Service references -->

[Services Docs]: https://opendal.apache.org/docs/rust/opendal/services/
[fs]: https://opendal.apache.org/docs/rust/opendal/services/struct.Fs.html
[http]: https://developer.mozilla.org/en-US/docs/Web/HTTP
[ftp]: https://datatracker.ietf.org/doc/html/rfc959
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
[swift]: https://docs.openstack.org/swift/latest/
[tos]: https://www.volcengine.com/product/tos
[upyun]: https://www.upyun.com/
[vercel-blob]: https://vercel.com/docs/storage/vercel-blob
[alluxio]: https://docs.alluxio.io/os/user/stable/en/api/REST-API.html
[azdls]: https://azure.microsoft.com/en-us/products/storage/data-lake-storage/
[azfile]: https://learn.microsoft.com/en-us/rest/api/storageservices/file-service-rest-api
[compfs]: https://github.com/compio-rs/compio/
[dbfs]: https://docs.databricks.com/en/dbfs/index.html
[gridfs]: https://www.mongodb.com/docs/manual/core/gridfs/
[hdfs]: https://hadoop.apache.org/docs/r3.3.4/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
[hdfs-native]: https://github.com/Kimahriman/hdfs-native
[ipfs]: https://ipfs.tech/
[ipmfs]: https://docs.ipfs.tech/concepts/file-systems/
[lakefs]: https://lakefs.io/
[goosefs]: https://github.com/Tencent/GooseFS
[opfs]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system
[monoiofs]: https://github.com/bytedance/monoio
[webhdfs]: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
[aliyun-drive]: https://www.aliyundrive.com/
[gdrive]: https://www.google.com/drive/
[onedrive]: https://www.microsoft.com/en-us/microsoft-365/onedrive/online-cloud-storage
[dropbox]: https://www.dropbox.com/
[koofr]: https://koofr.eu/
[pcloud]: https://www.pcloud.com/
[seafile]: https://www.seafile.com/
[yandex-disk]: https://360.yandex.com/disk/
[github]: https://github.com/
[cacache]: https://crates.io/crates/cacache
[cloudflare-kv]: https://developers.cloudflare.com/kv/
[dashmap]: https://github.com/xacrimon/dashmap
[etcd]: https://etcd.io/
[foundationdb]: https://www.foundationdb.org/
[persy]: https://crates.io/crates/persy
[redis]: https://redis.io/
[rocksdb]: http://rocksdb.org/
[sled]: https://crates.io/crates/sled
[redb]: https://crates.io/crates/redb
[tikv]: https://tikv.org/
[d1]: https://developers.cloudflare.com/d1/
[mongodb]: https://www.mongodb.com/
[mysql]: https://www.mysql.com/
[postgresql]: https://www.postgresql.org/
[sqlite]: https://www.sqlite.org/
[surrealdb]: https://surrealdb.com/
[ghac]: https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows
[memcached]: https://memcached.org/
[mini-moka]: https://github.com/moka-rs/mini-moka
[moka]: https://github.com/moka-rs/moka
[foyer]: https://github.com/foyer-rs/foyer
[vercel-artifacts]: https://vercel.com/docs/concepts/monorepos/remote-caching
[huggingface]: https://huggingface.co/
