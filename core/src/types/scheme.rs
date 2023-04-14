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
    Dashmap,
    /// [fs][crate::services::Fs]: POSIX alike file system.
    Fs,
    /// [gcs][crate::services::Gcs]: Google Cloud Storage backend.
    Gcs,
    /// [ghac][crate::services::Ghac]: GitHub Action Cache services.
    Ghac,
    /// [hdfs][crate::services::Hdfs]: Hadoop Distributed File System.
    Hdfs,
    /// [http][crate::services::Http]: HTTP backend.
    Http,
    /// [ftp][crate::services::Ftp]: FTP backend.
    Ftp,
    /// [ipmfs][crate::services::Ipfs]: IPFS HTTP Gateway
    Ipfs,
    /// [ipmfs][crate::services::Ipmfs]: IPFS mutable file system
    Ipmfs,
    /// [memcached][crate::services::Memcached]: Memcached service support.
    Memcached,
    /// [memory][crate::services::Memory]: In memory backend support.
    Memory,
    /// [moka][crate::services::Moka]: moka backend support.
    Moka,
    /// [obs][crate::services::Obs]: Huawei Cloud OBS services.
    Obs,
    /// [oss][crate::services::Oss]: Aliyun Object Storage Services
    Oss,
    /// [redis][crate::services::Redis]: Redis services
    Redis,
    /// [rocksdb][crate::services::Rocksdb]: RocksDB services
    Rocksdb,
    /// [s3][crate::services::S3]: AWS S3 alike services.
    S3,
    /// [sled][crate::services::Sled]: Sled services
    Sled,
    /// [wasabi][crate::services::Wasabi]: Wasabi service
    Wasabi,
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
            "dashmap" => Ok(Scheme::Dashmap),
            "fs" => Ok(Scheme::Fs),
            "gcs" => Ok(Scheme::Gcs),
            "ghac" => Ok(Scheme::Ghac),
            "hdfs" => Ok(Scheme::Hdfs),
            "http" | "https" => Ok(Scheme::Http),
            "ftp" | "ftps" => Ok(Scheme::Ftp),
            "ipfs" | "ipns" => Ok(Scheme::Ipfs),
            "ipmfs" => Ok(Scheme::Ipmfs),
            "memcached" => Ok(Scheme::Memcached),
            "memory" => Ok(Scheme::Memory),
            "moka" => Ok(Scheme::Moka),
            "obs" => Ok(Scheme::Obs),
            "redis" => Ok(Scheme::Redis),
            "rocksdb" => Ok(Scheme::Rocksdb),
            "s3" => Ok(Scheme::S3),
            "sled" => Ok(Scheme::Sled),
            "oss" => Ok(Scheme::Oss),
            "wasabi" => Ok(Scheme::Wasabi),
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
            Scheme::Dashmap => "dashmap",
            Scheme::Fs => "fs",
            Scheme::Gcs => "gcs",
            Scheme::Ghac => "ghac",
            Scheme::Hdfs => "hdfs",
            Scheme::Http => "http",
            Scheme::Ftp => "ftp",
            Scheme::Ipfs => "ipfs",
            Scheme::Ipmfs => "ipmfs",
            Scheme::Memcached => "memcached",
            Scheme::Memory => "memory",
            Scheme::Moka => "moka",
            Scheme::Obs => "obs",
            Scheme::Redis => "redis",
            Scheme::Rocksdb => "rocksdb",
            Scheme::S3 => "s3",
            Scheme::Sled => "sled",
            Scheme::Oss => "oss",
            Scheme::Wasabi => "wasabi",
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
