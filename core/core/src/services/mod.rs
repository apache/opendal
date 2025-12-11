// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Services will provide builders to build underlying backends.
//!
//! More ongoing services support is tracked at [opendal#5](https://github.com/apache/opendal/issues/5). Please feel free to submit issues if there are services not covered.

#[cfg(feature = "services-alluxio")]
mod alluxio;
#[cfg(feature = "services-alluxio")]
pub use alluxio::*;

#[cfg(feature = "services-b2")]
mod b2;
#[cfg(feature = "services-b2")]
pub use b2::*;

#[cfg(feature = "services-cacache")]
mod cacache;
#[cfg(feature = "services-cacache")]
pub use self::cacache::*;

#[cfg(feature = "services-compfs")]
mod compfs;
#[cfg(feature = "services-compfs")]
pub use compfs::*;

#[cfg(feature = "services-cos")]
mod cos;
#[cfg(feature = "services-cos")]
pub use cos::*;

#[cfg(feature = "services-d1")]
mod d1;
#[cfg(feature = "services-d1")]
pub use self::d1::*;

#[cfg(feature = "services-dashmap")]
mod dashmap;
#[cfg(feature = "services-dashmap")]
pub use self::dashmap::*;

#[cfg(feature = "services-dbfs")]
mod dbfs;
#[cfg(feature = "services-dbfs")]
pub use self::dbfs::*;

#[cfg(feature = "services-dropbox")]
mod dropbox;
#[cfg(feature = "services-dropbox")]
pub use dropbox::*;

#[cfg(feature = "services-etcd")]
mod etcd;
#[cfg(feature = "services-etcd")]
pub use self::etcd::*;

#[cfg(feature = "services-foundationdb")]
mod foundationdb;
#[cfg(feature = "services-foundationdb")]
pub use self::foundationdb::*;

#[cfg(feature = "services-fs")]
mod fs;
#[cfg(feature = "services-fs")]
pub use fs::*;

#[cfg(feature = "services-ftp")]
mod ftp;
#[cfg(feature = "services-ftp")]
pub use ftp::*;

#[cfg(feature = "services-gcs")]
mod gcs;
#[cfg(feature = "services-gcs")]
pub use gcs::*;

#[cfg(feature = "services-gdrive")]
mod gdrive;
#[cfg(feature = "services-gdrive")]
pub use gdrive::*;

#[cfg(feature = "services-github")]
mod github;
#[cfg(feature = "services-github")]
pub use github::*;

#[cfg(feature = "services-gridfs")]
mod gridfs;
#[cfg(feature = "services-gridfs")]
pub use gridfs::*;

#[cfg(feature = "services-hdfs")]
mod hdfs;
#[cfg(feature = "services-hdfs")]
pub use self::hdfs::*;

#[cfg(feature = "services-http")]
mod http;
#[cfg(feature = "services-http")]
pub use self::http::*;

#[cfg(feature = "services-huggingface")]
mod huggingface;
#[cfg(feature = "services-huggingface")]
pub use huggingface::*;

#[cfg(feature = "services-ipfs")]
mod ipfs;
#[cfg(feature = "services-ipfs")]
pub use self::ipfs::*;

#[cfg(feature = "services-ipmfs")]
mod ipmfs;
#[cfg(feature = "services-ipmfs")]
pub use ipmfs::*;

#[cfg(feature = "services-koofr")]
mod koofr;
#[cfg(feature = "services-koofr")]
pub use koofr::*;

#[cfg(feature = "services-lakefs")]
mod lakefs;
#[cfg(feature = "services-lakefs")]
pub use lakefs::*;

#[cfg(feature = "services-memcached")]
mod memcached;
#[cfg(feature = "services-memcached")]
pub use memcached::*;

#[cfg(feature = "services-memory")]
mod memory;
#[cfg(feature = "services-memory")]
pub use self::memory::*;

#[cfg(feature = "services-mini-moka")]
mod mini_moka;
#[cfg(feature = "services-mini-moka")]
pub use self::mini_moka::*;

#[cfg(feature = "services-mongodb")]
mod mongodb;
#[cfg(feature = "services-mongodb")]
pub use self::mongodb::*;

#[cfg(feature = "services-monoiofs")]
mod monoiofs;
#[cfg(feature = "services-monoiofs")]
pub use monoiofs::*;

#[cfg(feature = "services-obs")]
mod obs;
#[cfg(feature = "services-obs")]
pub use obs::*;

#[cfg(feature = "services-onedrive")]
mod onedrive;
#[cfg(feature = "services-onedrive")]
pub use onedrive::*;

#[cfg(feature = "services-oss")]
mod oss;
#[cfg(feature = "services-oss")]
pub use oss::*;

#[cfg(feature = "services-pcloud")]
mod pcloud;
#[cfg(feature = "services-pcloud")]
pub use pcloud::*;

#[cfg(feature = "services-persy")]
mod persy;
#[cfg(feature = "services-persy")]
pub use self::persy::*;

#[cfg(feature = "services-redb")]
mod redb;
#[cfg(feature = "services-redb")]
pub use self::redb::*;

#[cfg(feature = "services-redis")]
mod redis;
#[cfg(feature = "services-redis")]
pub use self::redis::*;

#[cfg(feature = "services-rocksdb")]
mod rocksdb;
#[cfg(feature = "services-rocksdb")]
pub use self::rocksdb::*;

#[cfg(feature = "services-seafile")]
mod seafile;
#[cfg(feature = "services-seafile")]
pub use seafile::*;

#[cfg(feature = "services-sftp")]
mod sftp;
#[cfg(feature = "services-sftp")]
pub use sftp::*;

#[cfg(feature = "services-sqlite")]
mod sqlite;
#[cfg(feature = "services-sqlite")]
pub use self::sqlite::*;

#[cfg(feature = "services-surrealdb")]
mod surrealdb;
#[cfg(feature = "services-surrealdb")]
pub use surrealdb::*;

#[cfg(feature = "services-swift")]
mod swift;
#[cfg(feature = "services-swift")]
pub use self::swift::*;

#[cfg(feature = "services-tikv")]
mod tikv;
#[cfg(feature = "services-tikv")]
pub use self::tikv::*;

#[cfg(feature = "services-upyun")]
mod upyun;
#[cfg(feature = "services-upyun")]
pub use upyun::*;

#[cfg(feature = "services-vercel-artifacts")]
mod vercel_artifacts;
#[cfg(feature = "services-vercel-artifacts")]
pub use vercel_artifacts::*;

#[cfg(feature = "services-webdav")]
mod webdav;
#[cfg(feature = "services-webdav")]
pub use webdav::*;

#[cfg(feature = "services-webhdfs")]
mod webhdfs;
#[cfg(feature = "services-webhdfs")]
pub use webhdfs::*;

#[cfg(feature = "services-yandex-disk")]
mod yandex_disk;
#[cfg(feature = "services-yandex-disk")]
pub use yandex_disk::*;

#[cfg(all(target_arch = "wasm32", feature = "services-opfs"))]
mod opfs;
