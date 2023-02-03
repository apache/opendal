// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Providing specific services support.
//!
//! In order to implement a service, we need the following things:
//!
//! - Builder: responsible for building the service backend.
//! - Backend: the service backend which implements the [`Accessor`][crate::raw::Accessor] trait.

pub mod azblob;
pub use azblob::Builder as Azblob;

pub mod azdfs;
pub use azdfs::Builder as Azdfs;

pub mod fs;
pub use fs::Builder as Fs;

#[cfg(feature = "services-ftp")]
pub mod ftp;
#[cfg(feature = "services-ftp")]
pub use ftp::Builder as Ftp;

pub mod gcs;
pub use gcs::Builder as Gcs;

pub mod ghac;
pub use ghac::Builder as Ghac;

#[cfg(feature = "services-hdfs")]
pub mod hdfs;
#[cfg(feature = "services-hdfs")]
pub use hdfs::Builder as Hdfs;

pub mod http;
pub use self::http::Builder as Http;

#[cfg(feature = "services-ipfs")]
pub mod ipfs;
#[cfg(feature = "services-ipfs")]
pub use self::ipfs::Builder as Ipfs;

pub mod ipmfs;
pub use ipmfs::Builder as Ipmfs;

#[cfg(feature = "services-memcached")]
pub mod memcached;
#[cfg(feature = "services-memcached")]
pub use memcached::Builder as Memcached;

pub mod memory;
pub use memory::Builder as Memory;

#[cfg(feature = "services-moka")]
pub mod moka;
#[cfg(feature = "services-moka")]
pub use self::moka::Builder as Moka;

pub mod obs;
pub use obs::Builder as Obs;

pub mod oss;
pub use oss::Builder as Oss;

#[cfg(feature = "services-redis")]
pub mod redis;
#[cfg(feature = "services-redis")]
pub use self::redis::Builder as Redis;

#[cfg(feature = "services-rocksdb")]
pub mod rocksdb;
#[cfg(feature = "services-rocksdb")]
pub use self::rocksdb::Builder as Rocksdb;

pub mod s3;
pub use s3::Builder as S3;

pub mod webdav;
pub use webdav::Builder as Webdav;
