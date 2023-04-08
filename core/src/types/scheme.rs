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

use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use crate::Error;

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
    /// [azblob][crate::services::Azblob]: Azure Storage Blob services.
    Azblob,
    /// [azdfs][crate::services::Azdfs]: Azure Data Lake Storage Gen2.
    Azdfs,
    /// [dashmap][crate::services::Dashmap]: dashmap backend support.
    #[cfg(feature = "services-dashmap")]
    Dashmap,
    /// [fs][crate::services::Fs]: POSIX alike file system.
    Fs,
    /// [gcs][crate::services::Gcs]: Google Cloud Storage backend.
    Gcs,
    /// [ghac][crate::services::Ghac]: GitHub Action Cache services.
    Ghac,
    /// [hdfs][crate::services::Hdfs]: Hadoop Distributed File System.
    #[cfg(feature = "services-hdfs")]
    Hdfs,
    /// [http][crate::services::Http]: HTTP backend.
    Http,
    /// [ftp][crate::services::Ftp]: FTP backend.
    #[cfg(feature = "services-ftp")]
    Ftp,
    /// [ipmfs][crate::services::Ipfs]: IPFS HTTP Gateway
    #[cfg(feature = "services-ipfs")]
    Ipfs,
    /// [ipmfs][crate::services::Ipmfs]: IPFS mutable file system
    Ipmfs,
    /// [memcached][crate::services::Memcached]: Memcached service support.
    #[cfg(feature = "services-memcached")]
    Memcached,
    /// [memory][crate::services::Memory]: In memory backend support.
    Memory,
    /// [moka][crate::services::Moka]: moka backend support.
    #[cfg(feature = "services-moka")]
    Moka,
    /// [obs][crate::services::Obs]: Huawei Cloud OBS services.
    Obs,
    /// [onedrive][crate::services::Onedrive]: Microsoft OneDrive services.
    Onedrive,
    /// [oss][crate::services::Oss]: Aliyun Object Storage Services
    Oss,
    /// [redis][crate::services::Redis]: Redis services
    #[cfg(feature = "services-redis")]
    Redis,
    /// [rocksdb][crate::services::Rocksdb]: RocksDB services
    #[cfg(feature = "services-rocksdb")]
    Rocksdb,
    /// [s3][crate::services::S3]: AWS S3 alike services.
    S3,
    /// [sled][crate::services::Sled]: Sled services
    #[cfg(feature = "services-sled")]
    Sled,
    /// [webdav][crate::services::Webdav]: WebDAV support.
    Webdav,
    /// [webhdfs][crate::services::Webhdfs]: WebHDFS RESTful API Services
    Webhdfs,
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
        write!(f, "{}", self.into_static())
    }
}

impl FromStr for Scheme {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "azblob" => Ok(Scheme::Azblob),
            "azdfs" => Ok(Scheme::Azdfs),
            #[cfg(feature = "services-dashmap")]
            "dashmap" => Ok(Scheme::Dashmap),
            "fs" => Ok(Scheme::Fs),
            "gcs" => Ok(Scheme::Gcs),
            "ghac" => Ok(Scheme::Ghac),
            #[cfg(feature = "services-hdfs")]
            "hdfs" => Ok(Scheme::Hdfs),
            "http" | "https" => Ok(Scheme::Http),
            #[cfg(feature = "services-ftp")]
            "ftp" | "ftps" => Ok(Scheme::Ftp),
            #[cfg(feature = "services-ipfs")]
            "ipfs" | "ipns" => Ok(Scheme::Ipfs),
            "ipmfs" => Ok(Scheme::Ipmfs),
            #[cfg(feature = "services-memcached")]
            "memcached" => Ok(Scheme::Memcached),
            "memory" => Ok(Scheme::Memory),
            #[cfg(feature = "services-moka")]
            "moka" => Ok(Scheme::Moka),
            "obs" => Ok(Scheme::Obs),
            #[cfg(feature = "services-redis")]
            "redis" => Ok(Scheme::Redis),
            #[cfg(feature = "services-rocksdb")]
            "rocksdb" => Ok(Scheme::Rocksdb),
            "s3" => Ok(Scheme::S3),
            #[cfg(feature = "services-sled")]
            "sled" => Ok(Scheme::Sled),
            "oss" => Ok(Scheme::Oss),
            "webdav" => Ok(Scheme::Webdav),
            "webhdfs" => Ok(Scheme::Webhdfs),
            _ => Ok(Scheme::Custom(Box::leak(s.into_boxed_str()))),
        }
    }
}

impl From<Scheme> for &'static str {
    fn from(v: Scheme) -> Self {
        match v {
            Scheme::Azblob => "azblob",
            Scheme::Azdfs => "azdfs",
            #[cfg(feature = "services-dashmap")]
            Scheme::Dashmap => "dashmap",
            Scheme::Fs => "fs",
            Scheme::Gcs => "gcs",
            Scheme::Ghac => "ghac",
            #[cfg(feature = "services-hdfs")]
            Scheme::Hdfs => "hdfs",
            Scheme::Http => "http",
            #[cfg(feature = "services-ftp")]
            Scheme::Ftp => "ftp",
            #[cfg(feature = "services-ipfs")]
            Scheme::Ipfs => "ipfs",
            Scheme::Ipmfs => "ipmfs",
            #[cfg(feature = "services-memcached")]
            Scheme::Memcached => "memcached",
            Scheme::Memory => "memory",
            #[cfg(feature = "services-moka")]
            Scheme::Moka => "moka",
            Scheme::Obs => "obs",
            Scheme::Onedrive => "onedrive",
            #[cfg(feature = "services-redis")]
            Scheme::Redis => "redis",
            #[cfg(feature = "services-rocksdb")]
            Scheme::Rocksdb => "rocksdb",
            Scheme::S3 => "s3",
            #[cfg(feature = "services-sled")]
            Scheme::Sled => "sled",
            Scheme::Oss => "oss",
            Scheme::Webdav => "webdav",
            Scheme::Webhdfs => "webhdfs",
            Scheme::Custom(v) => v,
        }
    }
}

impl From<Scheme> for String {
    fn from(v: Scheme) -> Self {
        v.into_static().to_string()
    }
}
