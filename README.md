# Apache OpenDAL

**OpenDAL** is a data access layer that allows users to easily and efficiently retrieve data from various storage services in a unified way.

![OpenDAL Architectural](https://github.com/apache/incubator-opendal/assets/5351546/c81013b2-5455-4950-9d31-dbf272b07998)

Major components of the project include:

**Libraries**

- [Rust Core](core/README.md)
- [C Binding](bindings/c/README.md) *not released*
- [Cpp Binding](bindings/cpp/README.md) *not released*
- [Haskell Binding](bindings/haskell/README.md) *not released*
- [Java Binding](bindings/java/README.md)
- [Lua Binding](bindings/lua/README.md) *not released*
- [Node.js Binding](bindings/nodejs/README.md)
- [Python Binding](bindings/python/README.md)
- [Ruby Binding](bindings/ruby/README.md) *not released*
- [Swift Binding](bindings/swift/README.md) *not released*
- [Zig Binding](bindings/zig/README.md) *not released*

**Applications**

- [oli](bin/oli): OpenDAL Command Line Interface
- [oay](bin/oay): OpenDAL Gateway

**Services**

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
- azdls: [Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/) services (As known as [ABFS](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver))
- hdfs: [Hadoop Distributed File System](https://hadoop.apache.org/docs/r3.3.4/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)(HDFS)
- ipfs: [InterPlanetary File System](https://ipfs.tech/) HTTP Gateway
- ipmfs: [InterPlanetary File System](https://ipfs.tech/) MFS API *being worked on*
- webhdfs: [WebHDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) Service

</details>

<details>
<summary>Consumer Cloud Storage Service (like gdrive, onedrive)</summary>

- gdrive: [Google Drive](https://www.google.com/drive/) *being worked on*
- onedrive: [OneDrive](https://www.microsoft.com/en-us/microsoft-365/onedrive/online-cloud-storage) *being worked on*

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

> Welcome to add any services that are not currently supported [here](https://github.com/apache/incubator-opendal/issues/5).

## Examples

The examples are available at [here](./examples/).

## Documentation

The documentation is available at <https://opendal.apache.org>.

## Contribute

OpenDAL is an active open-source project. We are always open to people who want to use it or contribute to it. Here are some ways to go.

- Start with [Contributing Guide](CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/incubator-opendal/issues/new) for bug report or feature requests.
- Discuss at [dev mailing list](mailto:dev-subscribe@opendal.apache.org) ([subscribe](mailto:dev-subscribe@opendal.apache.org?subject=(send%20this%20email%20to%20subscribe)) / [unsubscribe](mailto:dev-unsubscribe@opendal.apache.org?subject=(send%20this%20email%20to%20unsubscribe)) / [archives](https://lists.apache.org/list.html?dev@opendal.apache.org))
- Asking questions in the [Discussions](https://github.com/apache/incubator-opendal/discussions/new?category=q-a).
- Talk to community directly at [Discord](https://discord.gg/XQy8yGR2dg).

## Who is using OpenDAL?

### Rust Core

- [Databend](https://github.com/datafuselabs/databend/): A modern Elasticity and Performance cloud data warehouse.
- [GreptimeDB](https://github.com/GreptimeTeam/greptimedb): An open-source, cloud-native, distributed time-series database.
- [deepeth/mars](https://github.com/deepeth/mars): The powerful analysis platform to explore and visualize data from blockchain.
- [mozilla/sccache](https://github.com/mozilla/sccache/): `sccache` is [`ccache`](https://github.com/ccache/ccache) with cloud storage
- [RisingWave](https://github.com/risingwavelabs/risingwave): A Distributed SQL Database for Stream Processing
- [Vector](https://github.com/vectordotdev/vector): A high-performance observability data pipeline.
- [OctoBase](https://github.com/toeverything/OctoBase): the open-source database behind [AFFiNE](https://github.com/toeverything/affine), local-first, yet collaborative.
- [Pants](https://github.com/pantsbuild/pants): A fast, scalable, user-friendly build system for codebases of all sizes.
- [QuestDB](https://github.com/questdb/questdb): An open-source time-series database for high throughput ingestion and fast SQL queries with operational simplicity.

### C Binding

- [Milvus](https://github.com/milvus-io/milvus): A cloud-native vector database, storage for next generation AI applications

### Java Binding

- [QuestDB](https://github.com/questdb/questdb): An open-source time-series database for high throughput ingestion and fast SQL queries with operational simplicity.

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL®** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
