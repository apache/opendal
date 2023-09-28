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
    /// [atomicserver][crate::services::Atomicserver]: Atomicserver services.
    Atomicserver,
    /// [azblob][crate::services::Azblob]: Azure Storage Blob services.
    Azblob,
    /// [Azdls][crate::services::Azdls]: Azure Data Lake Storage Gen2.
    Azdls,
    /// [cacache][crate::services::Cacache]: cacache backend support.
    Cacache,
    /// [cos][crate::services::Cos]: Tencent Cloud Object Storage services.
    Cos,
    /// [dashmap][crate::services::Dashmap]: dashmap backend support.
    Dashmap,
    /// [etcd][crate::services::Etcd]: Etcd Services
    Etcd,
    /// [foundationdb][crate::services::Foundationdb]: Foundationdb services.
    Foundationdb,
    /// [fs][crate::services::Fs]: POSIX alike file system.
    Fs,
    /// [ftp][crate::services::Ftp]: FTP backend.
    Ftp,
    /// [gcs][crate::services::Gcs]: Google Cloud Storage backend.
    Gcs,
    /// [ghac][crate::services::Ghac]: GitHub Action Cache services.
    Ghac,
    /// [hdfs][crate::services::Hdfs]: Hadoop Distributed File System.
    Hdfs,
    /// [http][crate::services::Http]: HTTP backend.
    Http,

    /// [ipmfs][crate::services::Ipfs]: IPFS HTTP Gateway
    Ipfs,
    /// [ipmfs][crate::services::Ipmfs]: IPFS mutable file system
    Ipmfs,
    /// [memcached][crate::services::Memcached]: Memcached service support.
    Memcached,
    /// [memory][crate::services::Memory]: In memory backend support.
    Memory,
    /// [mini-moka][crate::services::MiniMoka]: Mini Moka backend support.
    MiniMoka,
    /// [moka][crate::services::Moka]: moka backend support.
    Moka,
    /// [obs][crate::services::Obs]: Huawei Cloud OBS services.
    Obs,
    /// [onedrive][crate::services::Onedrive]: Microsoft OneDrive services.
    Onedrive,
    /// [gdrive][crate::services::Gdrive]: GoogleDrive services.
    Gdrive,
    /// [dropbox][crate::services::Dropbox]: Dropbox services.
    Dropbox,
    /// [oss][crate::services::Oss]: Aliyun Object Storage Services
    Oss,
    /// [persy][crate::services::Persy]: persy backend support.
    Persy,
    /// [redis][crate::services::Redis]: Redis services
    Redis,
    /// [postgresql][crate::services::Postgresql]: Postgresql services
    Postgresql,
    /// [mysql][crate::services::Mysql]: Mysql services
    Mysql,
    /// [sqlite][crate::services::Sqlite]: Sqlite services
    Sqlite,
    /// [rocksdb][crate::services::Rocksdb]: RocksDB services
    Rocksdb,
    /// [s3][crate::services::S3]: AWS S3 alike services.
    S3,
    /// [sftp][crate::services::Sftp]: SFTP services
    Sftp,
    /// [sled][crate::services::Sled]: Sled services
    Sled,
    /// [Supabase][crate::services::Supabase]: Supabase storage service
    Supabase,
    /// [Vercel Artifacts][crate::services::VercelArtifacts]: Vercel Artifacts service, as known as Vercel Remote Caching.
    VercelArtifacts,
    /// [wasabi][crate::services::Wasabi]: Wasabi service
    Wasabi,
    /// [webdav][crate::services::Webdav]: WebDAV support.
    Webdav,
    /// [webhdfs][crate::services::Webhdfs]: WebHDFS RESTful API Services
    Webhdfs,
    /// [redb][crate::services::Redb]: Redb Services
    Redb,
    /// [tikv][crate::services::tikv]: Tikv Services
    Tikv,
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
            "atomicserver" => Ok(Scheme::Atomicserver),
            "azblob" => Ok(Scheme::Azblob),
            // Notes:
            //
            // OpenDAL used to call `azdls` as `azdfs`, we keep it for backward compatibility.
            // And abfs is widely used in hadoop ecosystem, keep it for easy to use.
            "azdls" | "azdfs" | "abfs" => Ok(Scheme::Azdls),
            "cacache" => Ok(Scheme::Cacache),
            "cos" => Ok(Scheme::Cos),
            "dashmap" => Ok(Scheme::Dashmap),
            "dropbox" => Ok(Scheme::Dropbox),
            "etcd" => Ok(Scheme::Etcd),
            "fs" => Ok(Scheme::Fs),
            "gcs" => Ok(Scheme::Gcs),
            "gdrive" => Ok(Scheme::Gdrive),
            "ghac" => Ok(Scheme::Ghac),
            "hdfs" => Ok(Scheme::Hdfs),
            "http" | "https" => Ok(Scheme::Http),
            "ftp" | "ftps" => Ok(Scheme::Ftp),
            "ipfs" | "ipns" => Ok(Scheme::Ipfs),
            "ipmfs" => Ok(Scheme::Ipmfs),
            "memcached" => Ok(Scheme::Memcached),
            "memory" => Ok(Scheme::Memory),
            "mysql" => Ok(Scheme::Mysql),
            "sqlite" => Ok(Scheme::Sqlite),
            "mini_moka" => Ok(Scheme::MiniMoka),
            "moka" => Ok(Scheme::Moka),
            "obs" => Ok(Scheme::Obs),
            "onedrive" => Ok(Scheme::Onedrive),
            "persy" => Ok(Scheme::Persy),
            "postgresql" => Ok(Scheme::Postgresql),
            "redb" => Ok(Scheme::Redb),
            "redis" => Ok(Scheme::Redis),
            "rocksdb" => Ok(Scheme::Rocksdb),
            "s3" => Ok(Scheme::S3),
            "sftp" => Ok(Scheme::Sftp),
            "sled" => Ok(Scheme::Sled),
            "supabase" => Ok(Scheme::Supabase),
            "oss" => Ok(Scheme::Oss),
            "vercel_artifacts" => Ok(Scheme::VercelArtifacts),
            "wasabi" => Ok(Scheme::Wasabi),
            "webdav" => Ok(Scheme::Webdav),
            "webhdfs" => Ok(Scheme::Webhdfs),
            "tikv" => Ok(Scheme::Tikv),
            _ => Ok(Scheme::Custom(Box::leak(s.into_boxed_str()))),
        }
    }
}

impl From<Scheme> for &'static str {
    fn from(v: Scheme) -> Self {
        match v {
            Scheme::Atomicserver => "atomicserver",
            Scheme::Azblob => "azblob",
            Scheme::Azdls => "Azdls",
            Scheme::Cacache => "cacache",
            Scheme::Cos => "cos",
            Scheme::Dashmap => "dashmap",
            Scheme::Etcd => "etcd",
            Scheme::Fs => "fs",
            Scheme::Gcs => "gcs",
            Scheme::Ghac => "ghac",
            Scheme::Hdfs => "hdfs",
            Scheme::Http => "http",
            Scheme::Foundationdb => "foundationdb",
            Scheme::Ftp => "ftp",
            Scheme::Ipfs => "ipfs",
            Scheme::Ipmfs => "ipmfs",
            Scheme::Memcached => "memcached",
            Scheme::Memory => "memory",
            Scheme::MiniMoka => "mini_moka",
            Scheme::Moka => "moka",
            Scheme::Obs => "obs",
            Scheme::Onedrive => "onedrive",
            Scheme::Persy => "persy",
            Scheme::Postgresql => "postgresql",
            Scheme::Mysql => "mysql",
            Scheme::Gdrive => "gdrive",
            Scheme::Dropbox => "dropbox",
            Scheme::Redis => "redis",
            Scheme::Rocksdb => "rocksdb",
            Scheme::S3 => "s3",
            Scheme::Sftp => "sftp",
            Scheme::Sled => "sled",
            Scheme::Supabase => "supabase",
            Scheme::VercelArtifacts => "vercel_artifacts",
            Scheme::Oss => "oss",
            Scheme::Wasabi => "wasabi",
            Scheme::Webdav => "webdav",
            Scheme::Webhdfs => "webhdfs",
            Scheme::Redb => "redb",
            Scheme::Tikv => "tikv",
            Scheme::Sqlite => "sqlite",
            Scheme::Custom(v) => v,
        }
    }
}

impl From<Scheme> for String {
    fn from(v: Scheme) -> Self {
        v.into_static().to_string()
    }
}
