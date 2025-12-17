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

#[cfg(feature = "services-gdrive")]
mod gdrive;
#[cfg(feature = "services-gdrive")]
pub use gdrive::*;

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

#[cfg(feature = "services-ipmfs")]
mod ipmfs;
#[cfg(feature = "services-ipmfs")]
pub use ipmfs::*;

#[cfg(feature = "services-lakefs")]
mod lakefs;
#[cfg(feature = "services-lakefs")]
pub use lakefs::*;

#[cfg(feature = "services-memory")]
mod memory;
#[cfg(feature = "services-memory")]
pub use self::memory::*;

#[cfg(feature = "services-mongodb")]
mod mongodb;
#[cfg(feature = "services-mongodb")]
pub use self::mongodb::*;

#[cfg(feature = "services-onedrive")]
mod onedrive;
#[cfg(feature = "services-onedrive")]
pub use onedrive::*;

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

#[cfg(feature = "services-swift")]
mod swift;
#[cfg(feature = "services-swift")]
pub use self::swift::*;

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
