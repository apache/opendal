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
//! More ongoing services support is tracked at [opendal#5](https://github.com/apache/incubator-opendal/issues/5). Please feel free to submit issues if there are services not covered.

#[cfg(feature = "services-azblob")]
mod azblob;
#[cfg(feature = "services-azblob")]
pub use azblob::Azblob;

#[cfg(feature = "services-azdls")]
mod azdls;
#[cfg(feature = "services-azdls")]
pub use azdls::Azdls;

#[cfg(feature = "services-cos")]
mod cos;
#[cfg(feature = "services-cos")]
pub use cos::Cos;

#[cfg(feature = "services-dashmap")]
mod dashmap;
#[cfg(feature = "services-dashmap")]
pub use self::dashmap::Dashmap;

#[cfg(feature = "services-etcd")]
mod etcd;
#[cfg(feature = "services-etcd")]
pub use self::etcd::Etcd;

#[cfg(feature = "services-fs")]
mod fs;
#[cfg(feature = "services-fs")]
pub use fs::Fs;

#[cfg(feature = "services-ftp")]
mod ftp;
#[cfg(feature = "services-ftp")]
pub use ftp::Ftp;

#[cfg(feature = "services-gcs")]
mod gcs;
#[cfg(feature = "services-gcs")]
pub use gcs::Gcs;

#[cfg(feature = "services-ghac")]
mod ghac;
#[cfg(feature = "services-ghac")]
pub use ghac::Ghac;

#[cfg(feature = "services-hdfs")]
mod hdfs;
#[cfg(feature = "services-hdfs")]
pub use hdfs::Hdfs;

#[cfg(feature = "services-http")]
mod http;
#[cfg(feature = "services-http")]
pub use self::http::Http;

#[cfg(feature = "services-ipfs")]
mod ipfs;
#[cfg(feature = "services-ipfs")]
pub use self::ipfs::Ipfs;

#[cfg(feature = "services-ipmfs")]
mod ipmfs;
#[cfg(feature = "services-ipmfs")]
pub use ipmfs::Ipmfs;

#[cfg(feature = "services-memcached")]
mod memcached;
#[cfg(feature = "services-memcached")]
pub use memcached::Memcached;

#[cfg(feature = "services-memory")]
mod memory;
#[cfg(feature = "services-memory")]
pub use memory::Memory;

#[cfg(feature = "services-mini-moka")]
mod mini_moka;
#[cfg(feature = "services-mini-moka")]
pub use self::mini_moka::MiniMoka;

#[cfg(feature = "services-moka")]
mod moka;
#[cfg(feature = "services-moka")]
pub use self::moka::Moka;

#[cfg(feature = "services-obs")]
mod obs;
#[cfg(feature = "services-obs")]
pub use obs::Obs;

#[cfg(feature = "services-oss")]
mod oss;
#[cfg(feature = "services-oss")]
pub use oss::Oss;

#[cfg(feature = "services-cacache")]
mod cacache;
#[cfg(feature = "services-cacache")]
pub use self::cacache::Cacache;

#[cfg(feature = "services-persy")]
mod persy;
#[cfg(feature = "services-persy")]
pub use self::persy::Persy;

#[cfg(feature = "services-redis")]
mod redis;
#[cfg(feature = "services-redis")]
pub use self::redis::Redis;

#[cfg(feature = "services-rocksdb")]
mod rocksdb;
#[cfg(feature = "services-rocksdb")]
pub use self::rocksdb::Rocksdb;

#[cfg(feature = "services-s3")]
mod s3;
#[cfg(feature = "services-s3")]
pub use s3::S3;

#[cfg(feature = "services-sftp")]
mod sftp;
#[cfg(feature = "services-sftp")]
pub use sftp::Sftp;

#[cfg(feature = "services-sled")]
mod sled;
#[cfg(feature = "services-sled")]
pub use self::sled::Sled;

#[cfg(feature = "services-supabase")]
mod supabase;
#[cfg(feature = "services-supabase")]
pub use supabase::Supabase;

#[cfg(feature = "services-wasabi")]
mod wasabi;
#[cfg(feature = "services-wasabi")]
pub use wasabi::Wasabi;

#[cfg(feature = "services-webdav")]
mod webdav;
#[cfg(feature = "services-webdav")]
pub use webdav::Webdav;

#[cfg(feature = "services-webhdfs")]
mod webhdfs;

#[cfg(feature = "services-onedrive")]
mod onedrive;
#[cfg(feature = "services-onedrive")]
pub use onedrive::Onedrive;

#[cfg(feature = "services-gdrive")]
mod gdrive;
#[cfg(feature = "services-gdrive")]
pub use gdrive::Gdrive;

#[cfg(feature = "services-dropbox")]
mod dropbox;
#[cfg(feature = "services-dropbox")]
pub use dropbox::Dropbox;
#[cfg(feature = "services-webhdfs")]
pub use webhdfs::Webhdfs;

#[cfg(feature = "services-vercel-artifacts")]
mod vercel_artifacts;
#[cfg(feature = "services-vercel-artifacts")]
pub use vercel_artifacts::VercelArtifacts;

#[cfg(feature = "services-redb")]
mod redb;
#[cfg(feature = "services-redb")]
pub use self::redb::Redb;

#[cfg(feature = "services-tikv")]
mod tikv;
#[cfg(feature = "services-tikv")]
pub use self::tikv::Tikv;

#[cfg(feature = "services-foundationdb")]
mod foundationdb;
#[cfg(feature = "services-foundationdb")]
pub use self::foundationdb::Foundationdb;

#[cfg(feature = "services-postgresql")]
mod postgresql;
#[cfg(feature = "services-postgresql")]
pub use self::postgresql::Postgresql;

#[cfg(feature = "services-atomicserver")]
mod atomicserver;
#[cfg(feature = "services-atomicserver")]
pub use self::atomicserver::Atomicserver;

#[cfg(feature = "services-mysql")]
mod mysql;
#[cfg(feature = "services-mysql")]
pub use self::mysql::Mysql;

#[cfg(feature = "services-sqlite")]
mod sqlite;
#[cfg(feature = "services-sqlite")]
pub use self::sqlite::Sqlite;
