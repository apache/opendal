# Apache OpenDAL™

**OpenDAL**: Access Data Freely.

<p>
<img src="https://opendal.apache.org/img/architectural.png" alt="OpenDAL Architectural" height="320px" align="right" />

OpenDAL offers a unified data access layer, empowering users to seamlessly and efficiently retrieve data from diverse storage services. Our goal is to deliver a comprehensive solution for any languages, methods, integrations, and services.

[![](https://img.shields.io/badge/maillist-dev%40opendal.apache.org-blue)](mailto:dev@opendal.apache.org)
[![](https://img.shields.io/discord/1081052318650339399?logo=discord&label=discord)](https://discord.gg/XQy8yGR2dg)
</p>

## For *ANY* languages

| Name                                          | Release                                                                                                                                              | 
|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------| 
| [Rust Core](core/README.md)                   | [![](https://img.shields.io/crates/v/opendal.svg)](https://crates.io/crates/opendal)                                                                 |
| [C Binding](bindings/c/README.md)             | -                                                                                                                                                    |
| [Cpp Binding](bindings/cpp/README.md)         | -                                                                                                                                                    |
| [Dotnet Binding](bindings/dotnet/README.md)   | -                                                                                                                                                    |
| [Go Binding](bindings/go/README.md)           | -                                                                                                                                                    |
| [Haskell Binding](bindings/haskell/README.md) | -                                                                                                                                                    |
| [Java Binding](bindings/java/README.md)       | [![](https://img.shields.io/maven-central/v/org.apache.opendal/opendal-java)](https://central.sonatype.com/artifact/org.apache.opendal/opendal-java) |
| [Lua Binding](bindings/lua/README.md)         | -                                                                                                                                                    |
| [Node.js Binding](bindings/nodejs/README.md)  | [![](https://img.shields.io/npm/v/opendal)](https://www.npmjs.com/package/opendal)                                                                   |
| [Ocaml Binding](bindings/ocaml/README.md)     | -                                                                                                                                                    |
| [PHP Binding](bindings/php/README.md)         | -                                                                                                                                                    |
| [Python Binding](bindings/python/README.md)   | [![](https://img.shields.io/pypi/v/opendal)](https://pypi.org/project/opendal/)                                                                      |
| [Ruby Binding](bindings/ruby/README.md)       | -                                                                                                                                                    |
| [Swift Binding](bindings/swift/README.md)     | -                                                                                                                                                    |
| [Zig Binding](bindings/zig/README.md)         | -                                                                                                                                                    |

## For *ANY* methods

| Name                     | Description                                                        | Release                                                                      | 
|--------------------------|--------------------------------------------------------------------|------------------------------------------------------------------------------| 
| [oay](bin/oay/README.md) | Access data via API Gateway                                        | [![](https://img.shields.io/crates/v/oay.svg)](https://crates.io/crates/oay) |
| [oli](bin/oli/README.md) | Access data via Command Line (alternative to s3cmd, s3cli, azcopy) | [![](https://img.shields.io/crates/v/oli.svg)](https://crates.io/crates/oli) |
| [ofs](bin/ofs/README.md) | Access data via POSIX file system API (alternative to s3fs)        | [![](https://img.shields.io/crates/v/ofs.svg)](https://crates.io/crates/ofs) |

## For *ANY* integrations

| Name                                                                | Description                                                                                | Release                                                                                                        | 
|---------------------------------------------------------------------|--------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------| 
| [dav-server-opendalfs](integrations/dav-server/README.md)           | Access data via integrations to [dav-server-rs](https://github.com/messense/dav-server-rs) | [![](https://img.shields.io/crates/v/dav-server-opendalfs.svg)](https://crates.io/crates/dav-server-opendalfs) |
| [object_store_opendal](integrations/object_store_opendal/README.md) | Access data via integrations to [object_store](https://docs.rs/object_store)               | [![](https://img.shields.io/crates/v/object_store_opendal.svg)](https://crates.io/crates/object_store_opendal) |

## For *ANY* services

| Type                           | Services                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | 
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| Standard Storage Protocols     | ftp, http, [sftp](https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02), [webdav](https://datatracker.ietf.org/doc/html/rfc4918), ..                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Object Storage Services        | [azblob](https://azure.microsoft.com/en-us/services/storage/blobs/), [cos](https://www.tencentcloud.com/products/cos), [gcs](https://cloud.google.com/storage), [obs](https://www.huaweicloud.com/intl/en-us/product/obs.html), [oss](https://www.aliyun.com/product/oss), [s3](https://aws.amazon.com/s3/), [supabase](https://supabase.com/docs/guides/storage), [b2](https://www.backblaze.com/), [openstack swift](https://docs.openstack.org/swift/latest/), [upyun](https://www.upyun.com/), [vercel_blob](https://vercel.com/docs/storage/vercel-blob), ..                                                                                    |
| File Storage Services          | fs, [alluxio](https://docs.alluxio.io/os/user/stable/en/api/REST-API.html), [azdls](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/), [azfile](https://learn.microsoft.com/en-us/rest/api/storageservices/file-service-rest-api), [chainsafe](https://storage.chainsafe.io/), [dbfs](https://docs.databricks.com/en/dbfs/index.html), [gridfs](https://www.mongodb.com/docs/manual/core/gridfs/), [hdfs](https://hadoop.apache.org/docs/r3.3.4/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html), [ipfs](https://ipfs.tech/), [webhdfs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html), .. |
| Consumer Cloud Storage Service | [gdrive](https://www.google.com/drive/), [onedrive](https://www.microsoft.com/en-us/microsoft-365/onedrive/online-cloud-storage), [dropbox](https://www.dropbox.com/), [icloud](https://www.icloud.com/iclouddrive), [koofr](https://koofr.eu/), [pcloud](https://www.pcloud.com/), [seafile](https://www.seafile.com/), [yandex_disk](https://360.yandex.com/disk/), ..                                                                                                                                                                                                                                                                             |
| Key-Value Storage Services     | [cacache](https://crates.io/crates/cacache), [cloudflare_kv](https://developers.cloudflare.com/kv/), [dashmap](https://github.com/xacrimon/dashmap), memory, [etcd](https://etcd.io/), [foundationdb](https://www.foundationdb.org/), [persy](https://crates.io/crates/persy), [redis](https://redis.io/), [rocksdb](http://rocksdb.org/), [sled](https://crates.io/crates/sled), [redb](https://crates.io/crates/redb), [tikv](https://tikv.org/), [atomicserver](https://github.com/atomicdata-dev/atomic-server), ..                                                                                                                              |
| Database Storage Services      | [d1](https://developers.cloudflare.com/d1/), [libsql](https://github.com/tursodatabase/libsql), [mongodb](https://www.mongodb.com/), [mysql](https://www.mysql.com/), [postgresql](https://www.postgresql.org/), [sqlite](https://www.sqlite.org/), ..                                                                                                                                                                                                                                                                                                                                                                                               |
| Cache Storage Services         | [ghac](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows), [memcached](https://memcached.org/), [mini_moka](https://github.com/moka-rs/mini-moka), [moka](https://github.com/moka-rs/moka), [vercel_artifacts](https://vercel.com/docs/concepts/monorepos/remote-caching), ..                                                                                                                                                                                                                                                                                                                            |
| Git Based Storage Services     | [huggingface](https://huggingface.co/), ..                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | 

> Welcome to add any services that are not currently supported [here](https://github.com/apache/opendal/issues/5).

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
- [apache/iceberg-rust](https://github.com/apache/iceberg-rust/): Native Rust implementation of [Apache Iceberg](https://iceberg.apache.org/), the open table format for analytic datasets.

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
