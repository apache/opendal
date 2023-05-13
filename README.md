<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="website/static/img/logo_dark.svg">
  <img alt="Apache OpenDAL(incubating)" src="website/static/img/logo.svg" width="300px">
</picture>
</p>

---

OpenDAL is a data access layer that allows users to easily and efficiently retrieve data from various storage services in a unified way.

![](https://user-images.githubusercontent.com/5351546/222356748-14276998-501b-4d2a-9b09-b8cff3018204.png)


Major components of the project include:

**Libraries**

- [Rust Core](core/README.md)
- [Node.js Binding](bindings/nodejs/README.md)
- [Python Binding](bindings/python/README.md)
- [C Binding](bindings/c) *working on*
- [Java Binding](bindings/java) *working on*
- [Ruby Binding](bindings/ruby) *working on*

**Applications**

- [oli](bin/oli): OpenDAL Command Line Interface
- [oay](bin/oay): OpenDAL Gateway

**Services**

<details>
<summary>Standard Storage Protocols (like fs, ftp)</summary>

- fs: POSIX alike file system
- ftp: FTP and FTPS
- http: HTTP read-only services
- sftp: [SFTP](https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02) services *working on*
- webdav: [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) Service

</details>

<details>
<summary>Object Storage Services (like s3, gcs, azblob)</summary>

- azblob: [Azure Storage Blob](https://azure.microsoft.com/en-us/services/storage/blobs/) services
- gcs: [Google Cloud Storage](https://cloud.google.com/storage) Service
- obs: [Huawei Cloud Object Storage](https://www.huaweicloud.com/intl/en-us/product/obs.html) Service (OBS)
- oss: [Aliyun Object Storage Service](https://www.aliyun.com/product/oss) (OSS)
- s3: [AWS S3](https://aws.amazon.com/s3/) alike services
- supabase: [Supabase Storage](https://supabase.com/docs/guides/storage) Service *working on*
- wasabi: [Wasabi](https://wasabi.com/) Cloud Storage

</details>

<details>
<summary>File Storage Services (like azdfs, hdfs)</summary>

- azdfs: [Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/) services (As known as [abfs](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver))
- hdfs: [Hadoop Distributed File System](https://hadoop.apache.org/docs/r3.3.4/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)(HDFS)
- ipfs: [InterPlanetary File System](https://ipfs.tech/) HTTP Gateway
- ipmfs: [InterPlanetary File System](https://ipfs.tech/) MFS API *working on*
- webhdfs: [WebHDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) Service

</details>

<details>
<summary>Consumer Cloud Storage Service (like gdrive, onedrive)</summary>

- gdrive: [Google Drive](https://www.google.com/drive/) *working on*
- onedrive: [OneDrive](https://www.microsoft.com/en-us/microsoft-365/onedrive/online-cloud-storage) *working on*

</details>

<details>
<summary>Key-Value Storage Service (like rocksdb, sled)</summary>

- dashmap: [dashmap](https://github.com/xacrimon/dashmap) backend
- memory: In memory backend
- redis: [Redis](https://redis.io/) services
- rocksdb: [RocksDB](http://rocksdb.org/) services
- sled: [sled](https://crates.io/crates/sled) backend

</details>

<details>
<summary>Cache Storage Service (like memcached, moka)</summary>

- ghac: [Github Action Cache](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows) Service
- memcached: [Memcached](https://memcached.org/) service
- moka: [moka](https://github.com/moka-rs/moka) backend
- vercel_artifacts: [Vercel Remote Caching](https://vercel.com/docs/concepts/monorepos/remote-caching) Service *working on*

</details>

> Welcome to add any services that are not currently supported [here](https://github.com/apache/incubator-opendal/issues/5).

## Documentation

The documentation is available at <https://opendal.apache.org>.

We are engaged in a practice we call "documentation as code". You can also view the documentation directly in project's source code. And we welcome you to contribute to the documentation.

## Contribute

OpenDAL is an active open-source project. We are always open to people who want to use it or contribute to it. Here are some ways to go.

- Start with [Contributing Guide](CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/incubator-opendal/issues/new) for bug report or feature requests.
- Asking questions in the [Discussions](https://github.com/apache/incubator-opendal/discussions/new?category=q-a).
- Talk to community directly at [Discord](https://discord.gg/XQy8yGR2dg).
- [Subscribe our dev mailing list](mailto:dev-subscribe@opendal.apache.org), then you can use it to ask questions, discuss design and implementation, etc. View the archive at <https://lists.apache.org/list.html?dev@opendal.apache.org>.

## Who is using OpenDAL?

- [Databend](https://github.com/datafuselabs/databend/): A modern Elasticity and Performance cloud data warehouse.
- [GreptimeDB](https://github.com/GreptimeTeam/greptimedb): An open-source, cloud-native, distributed time-series database.
- [deepeth/mars](https://github.com/deepeth/mars): The powerful analysis platform to explore and visualize data from blockchain.
- [mozilla/sccache](https://github.com/mozilla/sccache/): `sccache` is [`ccache`](https://github.com/ccache/ccache) with cloud storage
- [RisingWave](https://github.com/risingwavelabs/risingwave): A Distributed SQL Database for Stream Processing
- [Vector](https://github.com/vectordotdev/vector): A high-performance observability data pipeline.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
