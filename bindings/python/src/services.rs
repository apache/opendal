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

// > DO NOT EDIT IT MANUALLY <

use std::str::FromStr;

use crate::*;
use pyo3_stub_gen::derive::*;

#[gen_stub_pyclass_enum]
#[pyclass(
    eq,
    eq_int,
    dict,
    hash,
    frozen,
    name = "Scheme",
    module = "opendal.services"
)]
#[pyo3(rename_all = "PascalCase")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PyScheme {
    #[cfg(feature = "services-aliyun-drive")]
    AliyunDrive,
    #[cfg(feature = "services-alluxio")]
    Alluxio,
    #[cfg(feature = "services-azblob")]
    Azblob,
    #[cfg(feature = "services-azdls")]
    Azdls,
    #[cfg(feature = "services-azfile")]
    Azfile,
    #[cfg(feature = "services-b2")]
    B2,
    #[cfg(feature = "services-cacache")]
    Cacache,
    #[cfg(feature = "services-cos")]
    Cos,
    #[cfg(feature = "services-dashmap")]
    Dashmap,
    #[cfg(feature = "services-dropbox")]
    Dropbox,
    #[cfg(feature = "services-fs")]
    Fs,
    #[cfg(feature = "services-gcs")]
    Gcs,
    #[cfg(feature = "services-gdrive")]
    Gdrive,
    #[cfg(feature = "services-ghac")]
    Ghac,
    #[cfg(feature = "services-gridfs")]
    Gridfs,
    #[cfg(feature = "services-hdfs-native")]
    HdfsNative,
    #[cfg(feature = "services-http")]
    Http,
    #[cfg(feature = "services-huggingface")]
    Huggingface,
    #[cfg(feature = "services-ipfs")]
    Ipfs,
    #[cfg(feature = "services-ipmfs")]
    Ipmfs,
    #[cfg(feature = "services-koofr")]
    Koofr,
    #[cfg(feature = "services-memcached")]
    Memcached,
    #[cfg(feature = "services-memory")]
    Memory,
    #[cfg(feature = "services-mini-moka")]
    MiniMoka,
    #[cfg(feature = "services-moka")]
    Moka,
    #[cfg(feature = "services-mongodb")]
    Mongodb,
    #[cfg(feature = "services-mysql")]
    Mysql,
    #[cfg(feature = "services-obs")]
    Obs,
    #[cfg(feature = "services-onedrive")]
    Onedrive,
    #[cfg(feature = "services-oss")]
    Oss,
    #[cfg(feature = "services-persy")]
    Persy,
    #[cfg(feature = "services-postgresql")]
    Postgresql,
    #[cfg(feature = "services-redb")]
    Redb,
    #[cfg(feature = "services-redis")]
    Redis,
    #[cfg(feature = "services-s3")]
    S3,
    #[cfg(feature = "services-seafile")]
    Seafile,
    #[cfg(feature = "services-sled")]
    Sled,
    #[cfg(feature = "services-sqlite")]
    Sqlite,
    #[cfg(feature = "services-swift")]
    Swift,
    #[cfg(feature = "services-upyun")]
    Upyun,
    #[cfg(feature = "services-vercel-artifacts")]
    VercelArtifacts,
    #[cfg(feature = "services-webdav")]
    Webdav,
    #[cfg(feature = "services-webhdfs")]
    Webhdfs,
    #[cfg(feature = "services-yandex-disk")]
    YandexDisk,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyScheme {
    #[getter]
    pub fn name(&self) -> &'static str {
        match self {
            #[cfg(feature = "services-aliyun-drive")]
            PyScheme::AliyunDrive => "AliyunDrive",
            #[cfg(feature = "services-alluxio")]
            PyScheme::Alluxio => "Alluxio",
            #[cfg(feature = "services-azblob")]
            PyScheme::Azblob => "Azblob",
            #[cfg(feature = "services-azdls")]
            PyScheme::Azdls => "Azdls",
            #[cfg(feature = "services-azfile")]
            PyScheme::Azfile => "Azfile",
            #[cfg(feature = "services-b2")]
            PyScheme::B2 => "B2",
            #[cfg(feature = "services-cacache")]
            PyScheme::Cacache => "Cacache",
            #[cfg(feature = "services-cos")]
            PyScheme::Cos => "Cos",
            #[cfg(feature = "services-dashmap")]
            PyScheme::Dashmap => "Dashmap",
            #[cfg(feature = "services-dropbox")]
            PyScheme::Dropbox => "Dropbox",
            #[cfg(feature = "services-fs")]
            PyScheme::Fs => "Fs",
            #[cfg(feature = "services-gcs")]
            PyScheme::Gcs => "Gcs",
            #[cfg(feature = "services-gdrive")]
            PyScheme::Gdrive => "Gdrive",
            #[cfg(feature = "services-ghac")]
            PyScheme::Ghac => "Ghac",
            #[cfg(feature = "services-gridfs")]
            PyScheme::Gridfs => "Gridfs",
            #[cfg(feature = "services-hdfs-native")]
            PyScheme::HdfsNative => "HdfsNative",
            #[cfg(feature = "services-http")]
            PyScheme::Http => "Http",
            #[cfg(feature = "services-huggingface")]
            PyScheme::Huggingface => "Huggingface",
            #[cfg(feature = "services-ipfs")]
            PyScheme::Ipfs => "Ipfs",
            #[cfg(feature = "services-ipmfs")]
            PyScheme::Ipmfs => "Ipmfs",
            #[cfg(feature = "services-koofr")]
            PyScheme::Koofr => "Koofr",
            #[cfg(feature = "services-memcached")]
            PyScheme::Memcached => "Memcached",
            #[cfg(feature = "services-memory")]
            PyScheme::Memory => "Memory",
            #[cfg(feature = "services-mini-moka")]
            PyScheme::MiniMoka => "MiniMoka",
            #[cfg(feature = "services-moka")]
            PyScheme::Moka => "Moka",
            #[cfg(feature = "services-mongodb")]
            PyScheme::Mongodb => "Mongodb",
            #[cfg(feature = "services-mysql")]
            PyScheme::Mysql => "Mysql",
            #[cfg(feature = "services-obs")]
            PyScheme::Obs => "Obs",
            #[cfg(feature = "services-onedrive")]
            PyScheme::Onedrive => "Onedrive",
            #[cfg(feature = "services-oss")]
            PyScheme::Oss => "Oss",
            #[cfg(feature = "services-persy")]
            PyScheme::Persy => "Persy",
            #[cfg(feature = "services-postgresql")]
            PyScheme::Postgresql => "Postgresql",
            #[cfg(feature = "services-redb")]
            PyScheme::Redb => "Redb",
            #[cfg(feature = "services-redis")]
            PyScheme::Redis => "Redis",
            #[cfg(feature = "services-s3")]
            PyScheme::S3 => "S3",
            #[cfg(feature = "services-seafile")]
            PyScheme::Seafile => "Seafile",
            #[cfg(feature = "services-sled")]
            PyScheme::Sled => "Sled",
            #[cfg(feature = "services-sqlite")]
            PyScheme::Sqlite => "Sqlite",
            #[cfg(feature = "services-swift")]
            PyScheme::Swift => "Swift",
            #[cfg(feature = "services-upyun")]
            PyScheme::Upyun => "Upyun",
            #[cfg(feature = "services-vercel-artifacts")]
            PyScheme::VercelArtifacts => "VercelArtifacts",
            #[cfg(feature = "services-webdav")]
            PyScheme::Webdav => "Webdav",
            #[cfg(feature = "services-webhdfs")]
            PyScheme::Webhdfs => "Webhdfs",
            #[cfg(feature = "services-yandex-disk")]
            PyScheme::YandexDisk => "YandexDisk",
        }
    }

    #[getter]
    pub fn value(&self) -> &'static str {
        match self {
            #[cfg(feature = "services-aliyun-drive")]
            PyScheme::AliyunDrive => "aliyun_drive",
            #[cfg(feature = "services-alluxio")]
            PyScheme::Alluxio => "alluxio",
            #[cfg(feature = "services-azblob")]
            PyScheme::Azblob => "azblob",
            #[cfg(feature = "services-azdls")]
            PyScheme::Azdls => "azdls",
            #[cfg(feature = "services-azfile")]
            PyScheme::Azfile => "azfile",
            #[cfg(feature = "services-b2")]
            PyScheme::B2 => "b2",
            #[cfg(feature = "services-cacache")]
            PyScheme::Cacache => "cacache",
            #[cfg(feature = "services-cos")]
            PyScheme::Cos => "cos",
            #[cfg(feature = "services-dashmap")]
            PyScheme::Dashmap => "dashmap",
            #[cfg(feature = "services-dropbox")]
            PyScheme::Dropbox => "dropbox",
            #[cfg(feature = "services-fs")]
            PyScheme::Fs => "fs",
            #[cfg(feature = "services-gcs")]
            PyScheme::Gcs => "gcs",
            #[cfg(feature = "services-gdrive")]
            PyScheme::Gdrive => "gdrive",
            #[cfg(feature = "services-ghac")]
            PyScheme::Ghac => "ghac",
            #[cfg(feature = "services-gridfs")]
            PyScheme::Gridfs => "gridfs",
            #[cfg(feature = "services-hdfs-native")]
            PyScheme::HdfsNative => "hdfs_native",
            #[cfg(feature = "services-http")]
            PyScheme::Http => "http",
            #[cfg(feature = "services-huggingface")]
            PyScheme::Huggingface => "huggingface",
            #[cfg(feature = "services-ipfs")]
            PyScheme::Ipfs => "ipfs",
            #[cfg(feature = "services-ipmfs")]
            PyScheme::Ipmfs => "ipmfs",
            #[cfg(feature = "services-koofr")]
            PyScheme::Koofr => "koofr",
            #[cfg(feature = "services-memcached")]
            PyScheme::Memcached => "memcached",
            #[cfg(feature = "services-memory")]
            PyScheme::Memory => "memory",
            #[cfg(feature = "services-mini-moka")]
            PyScheme::MiniMoka => "mini_moka",
            #[cfg(feature = "services-moka")]
            PyScheme::Moka => "moka",
            #[cfg(feature = "services-mongodb")]
            PyScheme::Mongodb => "mongodb",
            #[cfg(feature = "services-mysql")]
            PyScheme::Mysql => "mysql",
            #[cfg(feature = "services-obs")]
            PyScheme::Obs => "obs",
            #[cfg(feature = "services-onedrive")]
            PyScheme::Onedrive => "onedrive",
            #[cfg(feature = "services-oss")]
            PyScheme::Oss => "oss",
            #[cfg(feature = "services-persy")]
            PyScheme::Persy => "persy",
            #[cfg(feature = "services-postgresql")]
            PyScheme::Postgresql => "postgresql",
            #[cfg(feature = "services-redb")]
            PyScheme::Redb => "redb",
            #[cfg(feature = "services-redis")]
            PyScheme::Redis => "redis",
            #[cfg(feature = "services-s3")]
            PyScheme::S3 => "s3",
            #[cfg(feature = "services-seafile")]
            PyScheme::Seafile => "seafile",
            #[cfg(feature = "services-sled")]
            PyScheme::Sled => "sled",
            #[cfg(feature = "services-sqlite")]
            PyScheme::Sqlite => "sqlite",
            #[cfg(feature = "services-swift")]
            PyScheme::Swift => "swift",
            #[cfg(feature = "services-upyun")]
            PyScheme::Upyun => "upyun",
            #[cfg(feature = "services-vercel-artifacts")]
            PyScheme::VercelArtifacts => "vercel_artifacts",
            #[cfg(feature = "services-webdav")]
            PyScheme::Webdav => "webdav",
            #[cfg(feature = "services-webhdfs")]
            PyScheme::Webhdfs => "webhdfs",
            #[cfg(feature = "services-yandex-disk")]
            PyScheme::YandexDisk => "yandex_disk",
        }
    }
}

impl From<PyScheme> for ocore::Scheme {
    fn from(p: PyScheme) -> Self {
        ocore::Scheme::from_str(p.value())
            .unwrap_or_else(|_| panic!("No Scheme found for '{}'", p.value()))
    }
}
