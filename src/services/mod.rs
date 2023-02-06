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

//! Services will provide builders to build underlying backends.
//!
//! # Capabilities
//!
//! | services      | read | write | list | presign | multipart | blocking |
//! |---------------|------|-------|------|---------|-----------|----------|
//! | [`Azblob`]    | Y    | Y     | Y    | N       | N         | N        |
//! | [`Azdfs`]     | Y    | Y     | Y    | N       | N         | N        |
//! | [`Fs`]        | Y    | Y     | Y    | X       | X         | Y        |
//! | [`Ftp`]       | Y    | Y     | Y    | X       | X         | N        |
//! | [`Gcs`]       | Y    | Y     | Y    | N       | N         | N        |
//! | [`Ghac`]      | Y    | Y     | N    | X       | X         | N        |
//! | [`Hdfs`]      | Y    | Y     | Y    | X       | X         | Y        |
//! | [`Http`]      | Y    | Y     | N    | N       | X         | N        |
//! | [`Ipfs`]      | Y    | Y     | Y    | Y       | X         | N        |
//! | [`Ipmfs`]     | Y    | Y     | Y    | Y       | X         | N        |
//! | [`Memcached`] | Y    | Y     | X    | X       | X         | N        |
//! | [`Memory`]    | Y    | Y     | X    | X       | X         | N        |
//! | [`Moka`]      | Y    | Y     | X    | X       | X         | N        |
//! | [`Obs`]       | Y    | Y     | Y    | N       | N         | N        |
//! | [`Oss`]       | Y    | Y     | Y    | N       | N         | N        |
//! | [`Redis`]     | Y    | Y     | X    | X       | X         | N        |
//! | [`Rocksdb`]   | Y    | Y     | X    | X       | X         | N        |
//! | [`S3`]        | Y    | Y     | Y    | Y       | Y         | N        |
//! | [`Webdav`]    | Y    | Y     | Y    | X       | X         | N        |
//!
//! - `Y` means the feature has been implemented.
//! - `N` means the feature is not implemented for now. Please feel free to open an issue to request it.
//! - `X` means the feature can't be implemented. Please report an issue if you think it's wrong.

mod azblob;
pub use azblob::Azblob;

mod azdfs;
pub use azdfs::Azdfs;

mod fs;
pub use fs::Fs;

#[cfg(feature = "services-ftp")]
mod ftp;
#[cfg(feature = "services-ftp")]
pub use ftp::Ftp;

mod gcs;
pub use gcs::Gcs;

mod ghac;
pub use ghac::Ghac;

#[cfg(feature = "services-hdfs")]
mod hdfs;
#[cfg(feature = "services-hdfs")]
pub use hdfs::Hdfs;

mod http;
pub use self::http::Http;

#[cfg(feature = "services-ipfs")]
mod ipfs;
#[cfg(feature = "services-ipfs")]
pub use self::ipfs::Ipfs;

mod ipmfs;
pub use ipmfs::Ipmfs;

#[cfg(feature = "services-memcached")]
mod memcached;
#[cfg(feature = "services-memcached")]
pub use memcached::Memcached;

mod memory;
pub use memory::Memory;

#[cfg(feature = "services-moka")]
mod moka;
#[cfg(feature = "services-moka")]
pub use self::moka::Moka;

mod obs;
pub use obs::Obs;

mod oss;
pub use oss::Oss;

#[cfg(feature = "services-redis")]
mod redis;
#[cfg(feature = "services-redis")]
pub use self::redis::Redis;

mod rocksdb;
#[cfg(feature = "services-rocksdb")]
pub mod webhdfs;
#[cfg(feature = "services-rocksdb")]
pub use self::rocksdb::Rocksdb;

mod s3;
pub use s3::S3;

mod webdav;
pub use webdav::Webdav;
