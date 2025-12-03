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
use std::str::FromStr;

use crate::Error;

/// Services that OpenDAL supports.
///
/// # Notes
/// - `Scheme` is `non_exhaustive`; new variants may be added.
/// - Ordering has no semantic meaning.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Scheme {
    #[default]
    Memory,
    AliyunDrive,
    Alluxio,
    Azblob,
    Azdls,
    Azfile,
    B2,
    Cacache,
    CloudflareKv,
    Compfs,
    Cos,
    D1,
    Dashmap,
    Dbfs,
    Dropbox,
    Etcd,
    Foundationdb,
    Fs,
    Ftp,
    Gcs,
    Gdrive,
    Ghac,
    Github,
    Gridfs,
    Hdfs,
    HdfsNative,
    Http,
    Huggingface,
    Ipfs,
    Ipmfs,
    Koofr,
    Lakefs,
    Memcached,
    MiniMoka,
    Moka,
    Mongodb,
    Monoiofs,
    Mysql,
    Obs,
    Onedrive,
    Oss,
    Pcloud,
    Persy,
    Postgresql,
    Redb,
    Redis,
    Rocksdb,
    S3,
    Seafile,
    Sftp,
    Sled,
    Sqlite,
    Surrealdb,
    Swift,
    Tikv,
    Upyun,
    VercelArtifacts,
    VercelBlob,
    Webdav,
    Webhdfs,
    YandexDisk,
    Custom(&'static str),
}

impl Scheme {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.into_static())
    }
}

impl AsRef<str> for Scheme {
    fn as_ref(&self) -> &str {
        self.into_static()
    }
}

impl FromStr for Scheme {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "aliyun-drive" | "aliyun_drive" => Ok(Scheme::AliyunDrive),
            "alluxio" => Ok(Scheme::Alluxio),
            "azblob" => Ok(Scheme::Azblob),
            // Notes:
            //
            // OpenDAL used to call `azdls` as `azdfs`, we keep it for backward compatibility.
            // And abfs is widely used in hadoop ecosystem, keep it for easy to use.
            "azdls" | "azdfs" | "abfs" => Ok(Scheme::Azdls),
            "azfile" => Ok(Scheme::Azfile),
            "b2" => Ok(Scheme::B2),
            "cacache" => Ok(Scheme::Cacache),
            "cloudflare-kv" | "cloudflare_kv" => Ok(Scheme::CloudflareKv),
            "compfs" => Ok(Scheme::Compfs),
            "cos" => Ok(Scheme::Cos),
            "d1" => Ok(Scheme::D1),
            "dashmap" => Ok(Scheme::Dashmap),
            "dbfs" => Ok(Scheme::Dbfs),
            "dropbox" => Ok(Scheme::Dropbox),
            "etcd" => Ok(Scheme::Etcd),
            "foundationdb" => Ok(Scheme::Foundationdb),
            "fs" => Ok(Scheme::Fs),
            "ftp" | "ftps" => Ok(Scheme::Ftp),
            "gcs" => Ok(Scheme::Gcs),
            "gdrive" => Ok(Scheme::Gdrive),
            "ghac" => Ok(Scheme::Ghac),
            "github" => Ok(Scheme::Github),
            "gridfs" => Ok(Scheme::Gridfs),
            "hdfs" => Ok(Scheme::Hdfs),
            "hdfs-native" | "hdfs_native" => Ok(Scheme::HdfsNative),
            "http" | "https" => Ok(Scheme::Http),
            "huggingface" | "hf" => Ok(Scheme::Huggingface),
            "ipfs" | "ipns" => Ok(Scheme::Ipfs),
            "ipmfs" => Ok(Scheme::Ipmfs),
            "koofr" => Ok(Scheme::Koofr),
            "lakefs" => Ok(Scheme::Lakefs),
            "memcached" => Ok(Scheme::Memcached),
            "memory" => Ok(Scheme::Memory),
            "mini-moka" | "mini_moka" => Ok(Scheme::MiniMoka),
            "moka" => Ok(Scheme::Moka),
            "mongodb" => Ok(Scheme::Mongodb),
            "monoiofs" => Ok(Scheme::Monoiofs),
            "mysql" => Ok(Scheme::Mysql),
            "obs" => Ok(Scheme::Obs),
            "onedrive" => Ok(Scheme::Onedrive),
            "oss" => Ok(Scheme::Oss),
            "pcloud" => Ok(Scheme::Pcloud),
            "persy" => Ok(Scheme::Persy),
            "postgresql" => Ok(Scheme::Postgresql),
            "redb" => Ok(Scheme::Redb),
            "redis" => Ok(Scheme::Redis),
            "rocksdb" => Ok(Scheme::Rocksdb),
            "s3" => Ok(Scheme::S3),
            "seafile" => Ok(Scheme::Seafile),
            "sftp" => Ok(Scheme::Sftp),
            "sled" => Ok(Scheme::Sled),
            "sqlite" => Ok(Scheme::Sqlite),
            "surrealdb" => Ok(Scheme::Surrealdb),
            "swift" => Ok(Scheme::Swift),
            "tikv" => Ok(Scheme::Tikv),
            "upyun" => Ok(Scheme::Upyun),
            "vercel-artifacts" | "vercel_artifacts" => Ok(Scheme::VercelArtifacts),
            "vercel-blob" | "vercel_blob" => Ok(Scheme::VercelBlob),
            "webdav" => Ok(Scheme::Webdav),
            "webhdfs" => Ok(Scheme::Webhdfs),
            "yandex-disk" | "yandex_disk" => Ok(Scheme::YandexDisk),
            _ => Ok(Scheme::Custom(Box::leak(s.into_boxed_str()))),
        }
    }
}

impl From<Scheme> for &'static str {
    fn from(v: Scheme) -> Self {
        match v {
            Scheme::AliyunDrive => "aliyun-drive",
            Scheme::Alluxio => "alluxio",
            Scheme::Azblob => "azblob",
            Scheme::Azdls => "azdls",
            Scheme::Azfile => "azfile",
            Scheme::B2 => "b2",
            Scheme::Cacache => "cacache",
            Scheme::CloudflareKv => "cloudflare-kv",
            Scheme::Compfs => "compfs",
            Scheme::Cos => "cos",
            Scheme::D1 => "d1",
            Scheme::Dashmap => "dashmap",
            Scheme::Dbfs => "dbfs",
            Scheme::Dropbox => "dropbox",
            Scheme::Etcd => "etcd",
            Scheme::Foundationdb => "foundationdb",
            Scheme::Fs => "fs",
            Scheme::Ftp => "ftp",
            Scheme::Gcs => "gcs",
            Scheme::Gdrive => "gdrive",
            Scheme::Ghac => "ghac",
            Scheme::Github => "github",
            Scheme::Gridfs => "gridfs",
            Scheme::Hdfs => "hdfs",
            Scheme::HdfsNative => "hdfs-native",
            Scheme::Http => "http",
            Scheme::Huggingface => "huggingface",
            Scheme::Ipfs => "ipfs",
            Scheme::Ipmfs => "ipmfs",
            Scheme::Koofr => "koofr",
            Scheme::Lakefs => "lakefs",
            Scheme::Memcached => "memcached",
            Scheme::Memory => "memory",
            Scheme::MiniMoka => "mini-moka",
            Scheme::Moka => "moka",
            Scheme::Mongodb => "mongodb",
            Scheme::Monoiofs => "monoiofs",
            Scheme::Mysql => "mysql",
            Scheme::Obs => "obs",
            Scheme::Onedrive => "onedrive",
            Scheme::Oss => "oss",
            Scheme::Pcloud => "pcloud",
            Scheme::Persy => "persy",
            Scheme::Postgresql => "postgresql",
            Scheme::Redb => "redb",
            Scheme::Redis => "redis",
            Scheme::Rocksdb => "rocksdb",
            Scheme::S3 => "s3",
            Scheme::Seafile => "seafile",
            Scheme::Sftp => "sftp",
            Scheme::Sled => "sled",
            Scheme::Sqlite => "sqlite",
            Scheme::Surrealdb => "surrealdb",
            Scheme::Swift => "swift",
            Scheme::Tikv => "tikv",
            Scheme::Upyun => "upyun",
            Scheme::VercelArtifacts => "vercel-artifacts",
            Scheme::VercelBlob => "vercel-blob",
            Scheme::Webdav => "webdav",
            Scheme::Webhdfs => "webhdfs",
            Scheme::YandexDisk => "yandex-disk",
            Scheme::Custom(v) => v,
        }
    }
}

impl From<Scheme> for String {
    fn from(v: Scheme) -> Self {
        v.into_static().to_string()
    }
}
