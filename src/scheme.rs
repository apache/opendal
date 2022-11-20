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

use crate::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

/// Services that OpenDAL supports
///
/// # Notes
///
/// - Scheme is `non_exhaustive`, new variant COULD be added at any time.
/// - New variant SHOULD be added in alphabet orders,
/// - Users MUST NOT relay on its order.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Scheme {
    /// [azblob][crate::services::azblob]: Azure Storage Blob services.
    Azblob,
    /// [fs][crate::services::fs]: POSIX alike file system.
    Fs,
    /// [gcs][crate::services::gcs]: Google Cloud Storage backend.
    Gcs,
    /// [hdfs][crate::services::hdfs]: Hadoop Distributed File System.
    #[cfg(feature = "services-hdfs")]
    Hdfs,
    /// [http][crate::services::http]: HTTP backend.
    Http,
    /// [ftp][crate::services::ftp]: FTP backend.
    #[cfg(feature = "services-ftp")]
    Ftp,
    /// [ipmfs][crate::services::ipfs]: IPFS HTTP Gateway
    #[cfg(feature = "services-ipfs")]
    Ipfs,
    /// [ipmfs][crate::services::ipmfs]: IPFS mutable file system
    Ipmfs,
    /// [memory][crate::services::memory]: In memory backend support.
    Memory,
    /// [moka][crate::services::moka]: moka backend support.
    #[cfg(feature = "services-moka")]
    Moka,
    /// [obs][crate::services::obs]: Huawei Cloud OBS services.
    Obs,
    /// [redis][crate::services::redis]: Redis services
    #[cfg(feature = "services-redis")]
    Redis,
    /// [rocksdb][crate::services::rocksdb]: RocksDB services
    #[cfg(feature = "services-rocksdb")]
    Rocksdb,
    /// [s3][crate::services::s3]: AWS S3 alike services.
    S3,
    /// [oss][crate::services::oss]: Aliyun Object Storage Services
    Oss,
    /// Custom that allow users to implement services outside of OpenDAL.
    ///
    /// # NOTE
    ///
    /// - Custom must not overwrite any existing services name.
    /// - Custom must be lowed cases.
    Custom(&'static str),
}

impl Scheme {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Default for Scheme {
    fn default() -> Self {
        Self::Memory
    }
}

impl Display for Scheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scheme::Azblob => write!(f, "azblob"),
            Scheme::Fs => write!(f, "fs"),
            #[cfg(feature = "services-hdfs")]
            Scheme::Hdfs => write!(f, "hdfs"),
            Scheme::Gcs => write!(f, "gcs"),
            Scheme::Http => write!(f, "http"),
            #[cfg(feature = "services-ftp")]
            Scheme::Ftp => write!(f, "ftp"),
            #[cfg(feature = "services-ipfs")]
            Scheme::Ipfs => write!(f, "ipfs"),
            Scheme::Ipmfs => write!(f, "ipmfs"),
            Scheme::Memory => write!(f, "memory"),
            #[cfg(feature = "services-moka")]
            Scheme::Moka => write!(f, "moka"),
            Scheme::Obs => write!(f, "obs"),
            #[cfg(feature = "services-redis")]
            Scheme::Redis => write!(f, "redis"),
            #[cfg(feature = "services-rocksdb")]
            Scheme::Rocksdb => write!(f, "rocksdb"),
            Scheme::S3 => write!(f, "s3"),
            Scheme::Oss => write!(f, "oss"),
            Scheme::Custom(v) => write!(f, "{v}"),
        }
    }
}

impl FromStr for Scheme {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "azblob" => Ok(Scheme::Azblob),
            "fs" => Ok(Scheme::Fs),
            "gcs" => Ok(Scheme::Gcs),
            #[cfg(feature = "services-hdfs")]
            "hdfs" => Ok(Scheme::Hdfs),
            "http" | "https" => Ok(Scheme::Http),
            #[cfg(feature = "services-ftp")]
            "ftp" | "ftps" => Ok(Scheme::Ftp),
            #[cfg(feature = "services-ipfs")]
            "ipfs" | "ipns" => Ok(Scheme::Ipfs),
            "ipmfs" => Ok(Scheme::Ipmfs),
            "memory" => Ok(Scheme::Memory),
            #[cfg(feature = "services-moka")]
            "moka" => Ok(Scheme::Moka),
            "obs" => Ok(Scheme::Obs),
            #[cfg(feature = "services-redis")]
            "redis" => Ok(Scheme::Redis),
            #[cfg(feature = "services-rocksdb")]
            "rocksdb" => Ok(Scheme::Rocksdb),
            "s3" => Ok(Scheme::S3),
            "oss" => Ok(Scheme::Oss),
            _ => Ok(Scheme::Custom(Box::leak(s.into_boxed_str()))),
        }
    }
}

impl From<Scheme> for &'static str {
    fn from(v: Scheme) -> Self {
        match v {
            Scheme::Azblob => "azblob",
            Scheme::Fs => "fs",
            Scheme::Gcs => "gcs",
            #[cfg(feature = "services-hdfs")]
            Scheme::Hdfs => "hdfs",
            Scheme::Http => "http",
            #[cfg(feature = "services-ftp")]
            Scheme::Ftp => "ftp",
            #[cfg(feature = "services-ipfs")]
            Scheme::Ipfs => "ipfs",
            Scheme::Ipmfs => "ipmfs",
            Scheme::Memory => "memory",
            #[cfg(feature = "services-moka")]
            Scheme::Moka => "moka",
            Scheme::Obs => "obs",
            #[cfg(feature = "services-redis")]
            Scheme::Redis => "redis",
            #[cfg(feature = "services-rocksdb")]
            Scheme::Rocksdb => "service-rocksdb",
            Scheme::S3 => "s3",
            Scheme::Oss => "oss",
            Scheme::Custom(v) => v,
        }
    }
}

impl From<Scheme> for String {
    fn from(v: Scheme) -> Self {
        v.into_static().to_string()
    }
}
