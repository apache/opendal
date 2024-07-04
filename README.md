# Apache OpenDAL™: *Access Data Freely*

[![](https://img.shields.io/badge/maillist-dev%40opendal.apache.org-blue)](mailto:dev@opendal.apache.org)
[![](https://img.shields.io/discord/1081052318650339399?logo=discord&label=discord)](https://opendal.apache.org/discord)

OpenDAL offers a unified data access layer, empowering users to seamlessly and efficiently retrieve data from diverse storage services. Our goal is to deliver a comprehensive solution for any languages, methods, integrations, and services.

<img src="https://opendal.apache.org/img/architectural.png" alt="OpenDAL Architectural" width="61.8%" />

## For *ANY* languages

| Name              | Release                                          | Docs                                                                              |
|-------------------|--------------------------------------------------|-----------------------------------------------------------------------------------|
| [Rust Core]       | [![Rust Core Image]][Rust Core Link]             | [![Docs Release]][Rust Core Release Docs] [![Docs Dev]][Rust Core Dev Docs]       |
| [C Binding]       | -                                                | [![Docs Dev]][C Binding Dev Docs]                                                 |
| [Cpp Binding]     | -                                                | [![Docs Dev]][Cpp Binding Dev Docs]                                               |
| [Dotnet Binding]  | -                                                | -                                                                                 |
| [Go Binding]      | -                                                | -                                                                                 |
| [Haskell Binding] | -                                                | -                                                                                 |
| [Java Binding]    | [![Java Binding Image]][Java Binding Link]       | [![Docs Release]][Java Binding Release Docs] [![Docs Dev]][Java Binding Dev Docs] |
| [Lua Binding]     | -                                                | -                                                                                 |
| [Node.js Binding] | [![Node.js Binding Image]][Node.js Binding Link] | [![Docs Dev]][Node.js Binding Dev Docs]                                           |
| [Ocaml Binding]   | -                                                | -                                                                                 |
| [PHP Binding]     | -                                                | -                                                                                 |
| [Python Binding]  | [![Python Binding Image]][Python Binding Link]   | [![Docs Dev]][Python Binding Dev Docs]                                            |
| [Ruby Binding]    | -                                                | -                                                                                 |
| [Swift Binding]   | -                                                | -                                                                                 |
| [Zig Binding]     | -                                                | -                                                                                 |

[Docs Release]: https://img.shields.io/badge/docs-release-blue
[Docs Dev]: https://img.shields.io/badge/docs-dev-blue
[Rust Core]: core/README.md
[Rust Core Image]: https://img.shields.io/crates/v/opendal.svg
[Rust Core Link]: https://crates.io/crates/opendal
[Rust Core Release Docs]: https://docs.rs/opendal
[Rust Core Dev Docs]: https://opendal.apache.org/docs/rust/opendal/
[C Binding]: bindings/c/README.md
[C Binding Dev Docs]: https://opendal.apache.org/docs/c/
[Cpp Binding]: bindings/cpp/README.md
[Cpp Binding Dev Docs]: https://opendal.apache.org/docs/cpp/
[Dotnet Binding]: bindings/dotnet/README.md
[Go Binding]: bindings/go/README.md
[Haskell Binding]: bindings/haskell/README.md
[Java Binding]: bindings/java/README.md
[Java Binding Image]: https://img.shields.io/maven-central/v/org.apache.opendal/opendal-java
[Java Binding Link]: https://central.sonatype.com/artifact/org.apache.opendal/opendal-java
[Java Binding Release Docs]: https://javadoc.io/doc/org.apache.opendal/opendal-java
[Java Binding Dev Docs]: https://opendal.apache.org/docs/java/
[Lua Binding]: bindings/lua/README.md
[Node.js Binding]: bindings/nodejs/README.md
[Node.js Binding Image]: https://img.shields.io/npm/v/opendal
[Node.js Binding Link]: https://www.npmjs.com/package/opendal
[Node.js Binding Dev Docs]: https://opendal.apache.org/docs/nodejs/
[Ocaml Binding]: bindings/ocaml/README.md
[PHP Binding]: bindings/php/README.md
[Python Binding]: bindings/python/README.md
[Python Binding Image]: https://img.shields.io/pypi/v/opendal
[Python Binding Link]: https://pypi.org/project/opendal/
[Python Binding Dev Docs]: https://opendal.apache.org/docs/python/
[Ruby Binding]: bindings/ruby/README.md
[Swift Binding]: bindings/swift/README.md
[Zig Binding]: bindings/zig/README.md


## For *ANY* methods

| Name  | Description                                                        | Release                   |
|-------|--------------------------------------------------------------------|---------------------------|
| [oay] | Access data via API Gateway                                        | [![oay image]][oay crate] |
| [oli] | Access data via Command Line (alternative to s3cmd, s3cli, azcopy) | [![oli image]][oli crate] |
| [ofs] | Access data via POSIX file system API (alternative to s3fs)        | [![ofs image]][ofs crate] |

[oay]: bin/oay/README.md
[oay image]: https://img.shields.io/crates/v/oay.svg
[oay crate]: https://crates.io/crates/oay
[oli]: bin/oli/README.md
[oli image]: https://img.shields.io/crates/v/oli.svg
[oli crate]: https://crates.io/crates/oli
[ofs]: bin/ofs/README.md
[ofs image]: https://img.shields.io/crates/v/ofs.svg
[ofs crate]: https://crates.io/crates/ofs

## For *ANY* integrations

| Name                   | Description                                              | Release                                     | Docs                                                                              |
|------------------------|----------------------------------------------------------|---------------------------------------------|-----------------------------------------------------------------------------------|
| [dav-server-opendalfs] | a [dav-server-rs] implementation using opendal.          | [![dav-server image]][dav-server crate]     | [![Docs Release]][dav-server release docs] [![Docs Dev]][dav-server dev docs]     |
| [object_store_opendal] | an [object_store] implementation using opendal.          | [![object_store image]][object_store crate] | [![Docs Release]][object_store release docs] [![Docs Dev]][object_store dev docs] |
| [fuse3_opendal]        | Access data via integrations to [fuse3]                  | [![fuse3 image]][fuse3 crate]               | [![Docs Release]][fuse3 release docs] [![Docs Dev]][fuse3 dev docs]               |
| [virtiofs_opendal]     | Access data via integrations to [vhost-user-backend]     | [![virtiofs image]][virtiofs crate]         | [![Docs Release]][virtiofs release docs] [![Docs Dev]][virtiofs dev docs]         |
| [unftp-sbe-opendal]    | an [unftp] storage backend implementation using opendal. | [![unftp-sbe image]][unftp-sbe crate]       | [![Docs Release]][unftp-sbe release docs] [![Docs Dev]][unftp-sbe dev docs]       |

[dav-server-opendalfs]: integrations/dav-server/README.md
[dav-server-rs]: https://github.com/messense/dav-server-rs
[dav-server image]: https://img.shields.io/crates/v/dav-server-opendalfs.svg
[dav-server crate]: https://crates.io/crates/dav-server-opendalfs
[dav-server release docs]: https://docs.rs/dav-server-opendalfs/
[dav-server dev docs]: https://opendal.apache.org/docs/dav-server-opendalfs/dav_server_opendalfs/

[object_store_opendal]: integrations/object_store/README.md
[object_store]: https://docs.rs/object_store
[object_store image]: https://img.shields.io/crates/v/object_store_opendal.svg
[object_store crate]: https://crates.io/crates/object_store_opendal
[object_store release docs]: https://docs.rs/object_store_opendal/
[object_store dev docs]: https://opendal.apache.org/docs/object-store-opendal/object_store_opendal/

[fuse3_opendal]: integrations/fuse3/README.md
[fuse3]: https://docs.rs/fuse3
[fuse3 image]: https://img.shields.io/crates/v/fuse3_opendal.svg
[fuse3 crate]: https://crates.io/crates/fuse3_opendal
[fuse3 release docs]: https://docs.rs/fuse3_opendal/
[fuse3 dev docs]: https://opendal.apache.org/docs/fuse3-opendal/fuse3_opendal/

[virtiofs_opendal]: integrations/virtiofs/README.md
[vhost-user-backend]: https://docs.rs/vhost-user-backend
[virtiofs image]: https://img.shields.io/crates/v/virtiofs_opendal.svg
[virtiofs crate]: https://crates.io/crates/virtiofs_opendal
[virtiofs release docs]: https://docs.rs/virtiofs_opendal/
[virtiofs dev docs]: https://opendal.apache.org/docs/virtiofs-opendal/virtiofs_opendal/

[unftp-sbe-opendal]: integrations/unftp-sbe/README.md
[unftp]: https://crates.io/crates/unftp
[unftp-sbe image]: https://img.shields.io/crates/v/unftp-sbe-opendal.svg
[unftp-sbe crate]: https://crates.io/crates/unftp-sbe-opendal
[unftp-sbe release docs]: https://docs.rs/unftp-sbe-opendal/
[unftp-sbe dev docs]: https://opendal.apache.org/docs/unftp-sbe-opendal/unftp_sbe_opendal/

## For *ANY* services

| Type                           | Services                                                                                                                                 |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| Standard Storage Protocols     | ftp http [sftp] [webdav]                                                                                                                 |
| Object Storage Services        | [azblob] [cos] [gcs] [obs] [oss] [s3] <br> [b2] [openstack_swift] [upyun] [vercel_blob]                                                  |
| File Storage Services          | fs [alluxio] [azdls] [azfile] [chainsafe] [compfs] <br> [dbfs] [gridfs] [hdfs] [hdfs_native] [ipfs] [webhdfs]                            |
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
[chainsafe]: https://storage.chainsafe.io/
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

## Examples

The examples are available at [here](./examples/).

## Documentation

The documentation is available at <https://opendal.apache.org>.

## Contribute

OpenDAL is an active open-source project. We are always open to people who want to use it or contribute to it. Here are some ways to go.

- Start with [Contributing Guide](CONTRIBUTING.md).
- Submit [Issues](https://github.com/apache/opendal/issues/new) for bug report or feature requests.
- Discuss at [dev mailing list](mailto:dev@opendal.apache.org) ([subscribe](mailto:dev-subscribe@opendal.apache.org?subject=(send%20this%20email%20to%20subscribe)) / [unsubscribe](mailto:dev-unsubscribe@opendal.apache.org?subject=(send%20this%20email%20to%20unsubscribe)) / [archives](https://lists.apache.org/list.html?dev@opendal.apache.org))
- Asking questions in the [Discussions](https://github.com/apache/opendal/discussions/new?category=q-a).
- Talk to community directly at [Discord](https://opendal.apache.org/discord).

## Who is using OpenDAL?

### Rust Core

- [apache/iceberg-rust](https://github.com/apache/iceberg-rust/): Native Rust implementation of [Apache Iceberg](https://iceberg.apache.org/), the open table format for analytic datasets.
- [Databend](https://github.com/datafuselabs/databend/): A modern Elasticity and Performance cloud data warehouse.
- [deepeth/mars](https://github.com/deepeth/mars): The powerful analysis platform to explore and visualize data from blockchain.
- [GreptimeDB](https://github.com/GreptimeTeam/greptimedb): An open-source, cloud-native, distributed time-series database.
- [mozilla/sccache](https://github.com/mozilla/sccache/): `sccache` is [`ccache`](https://github.com/ccache/ccache) with cloud storage
- [OctoBase](https://github.com/toeverything/OctoBase): the open-source database behind [AFFiNE](https://github.com/toeverything/affine), local-first, yet collaborative.
- [ParadeDB](https://github.com/paradedb/paradedb): Postgres for Search and Analytics - fast full-text search and analytics in Postgres and over cloud storage. Built as an extension.
- [Pants](https://github.com/pantsbuild/pants): A fast, scalable, user-friendly build system for codebases of all sizes.
- [QuestDB](https://github.com/questdb/questdb): An open-source time-series database for high throughput ingestion and fast SQL queries with operational simplicity.
- [RisingWave](https://github.com/risingwavelabs/risingwave): A Distributed SQL Database for Stream Processing
- [Vector](https://github.com/vectordotdev/vector): A high-performance observability data pipeline.

### C Binding

- [Milvus](https://github.com/milvus-io/milvus): A cloud-native vector database, storage for next generation AI applications

### Java Binding

- [QuestDB](https://github.com/questdb/questdb): An open-source time-series database for high throughput ingestion and fast SQL queries with operational simplicity.

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
