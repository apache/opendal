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

use crate::*;

#[pyclass(
    eq,
    eq_int,
    dict,
    hash,
    frozen,
    module = "opendal.services",
    from_py_object
)]
#[pyo3(rename_all = "PascalCase")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scheme {
    #[cfg(feature = "services-akamai")]
    Akamai,
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
    #[cfg(feature = "services-ceph-rgw")]
    CephRgw,
    #[cfg(feature = "services-cos")]
    Cos,
    #[cfg(feature = "services-dashmap")]
    Dashmap,
    #[cfg(feature = "services-dropbox")]
    Dropbox,
    #[cfg(feature = "services-exoscale")]
    Exoscale,
    #[cfg(feature = "services-foyer")]
    Foyer,
    #[cfg(feature = "services-fs")]
    Fs,
    #[cfg(feature = "services-ftp")]
    Ftp,
    #[cfg(feature = "services-garage")]
    Garage,
    #[cfg(feature = "services-gcs")]
    Gcs,
    #[cfg(feature = "services-gcs-grpc")]
    GcsGrpc,
    #[cfg(feature = "services-gdrive")]
    Gdrive,
    #[cfg(feature = "services-ghac")]
    Ghac,
    #[cfg(feature = "services-github")]
    Github,
    #[cfg(feature = "services-gridfs")]
    Gridfs,
    #[cfg(feature = "services-hdfs-native")]
    HdfsNative,
    #[cfg(feature = "services-hetzner")]
    Hetzner,
    #[cfg(feature = "services-hf")]
    Hf,
    #[cfg(feature = "services-http")]
    Http,
    #[cfg(feature = "services-ibm-cos")]
    IbmCos,
    #[cfg(feature = "services-idrive-e2")]
    IdriveE2,
    #[cfg(feature = "services-ionos")]
    Ionos,
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
    #[cfg(feature = "services-minio")]
    Minio,
    #[cfg(feature = "services-moka")]
    Moka,
    #[cfg(feature = "services-mongodb")]
    Mongodb,
    #[cfg(feature = "services-mysql")]
    Mysql,
    #[cfg(feature = "services-neon")]
    Neon,
    #[cfg(feature = "services-obs")]
    Obs,
    #[cfg(feature = "services-oci")]
    Oci,
    #[cfg(feature = "services-onedrive")]
    Onedrive,
    #[cfg(feature = "services-oss")]
    Oss,
    #[cfg(feature = "services-ovhcloud")]
    Ovhcloud,
    #[cfg(feature = "services-persy")]
    Persy,
    #[cfg(feature = "services-postgresql")]
    Postgresql,
    #[cfg(feature = "services-qingstor")]
    Qingstor,
    #[cfg(feature = "services-r2")]
    R2,
    #[cfg(feature = "services-redb")]
    Redb,
    #[cfg(feature = "services-redis")]
    Redis,
    #[cfg(feature = "services-rustfs")]
    Rustfs,
    #[cfg(feature = "services-s3")]
    S3,
    #[cfg(feature = "services-scaleway")]
    Scaleway,
    #[cfg(feature = "services-seafile")]
    Seafile,
    #[cfg(feature = "services-seaweedfs")]
    Seaweedfs,
    #[cfg(feature = "services-sftp")]
    Sftp,
    #[cfg(feature = "services-sled")]
    Sled,
    #[cfg(feature = "services-spaces")]
    Spaces,
    #[cfg(feature = "services-sqlite")]
    Sqlite,
    #[cfg(feature = "services-storj")]
    Storj,
    #[cfg(feature = "services-supabase")]
    Supabase,
    #[cfg(feature = "services-swift")]
    Swift,
    #[cfg(feature = "services-tigris")]
    Tigris,
    #[cfg(feature = "services-tos")]
    Tos,
    #[cfg(feature = "services-upyun")]
    Upyun,
    #[cfg(feature = "services-vercel-artifacts")]
    VercelArtifacts,
    #[cfg(feature = "services-wasabi")]
    Wasabi,
    #[cfg(feature = "services-webdav")]
    Webdav,
    #[cfg(feature = "services-webhdfs")]
    Webhdfs,
    #[cfg(feature = "services-yandex-disk")]
    YandexDisk,
}

#[pymethods]
impl Scheme {
    #[getter]
    pub fn name(&self) -> String {
        format!("{:?}", self)
    }

    #[getter]
    pub fn value(&self) -> &'static str {
        (*self).into()
    }
}

macro_rules! impl_enum_to_str {
    ($src:ty { $(
        $(#[$cfg:meta])?
        $variant:ident => $value:literal
    ),* $(,)? }) => {
        impl From<$src> for &'static str {
            fn from(value: $src) -> Self {
                match value {
                    $(
                        $(#[$cfg])?
                        <$src>::$variant => $value,
                    )*
                }
            }
        }

        impl From<$src> for String {
            fn from(value: $src) -> Self {
                let v: &'static str = value.into();
                v.to_string()
            }
        }
    };
}

impl_enum_to_str!(
    Scheme {
        #[cfg(feature = "services-akamai")]
        Akamai => "akamai",
        #[cfg(feature = "services-aliyun-drive")]
        AliyunDrive => "aliyun-drive",
        #[cfg(feature = "services-alluxio")]
        Alluxio => "alluxio",
        #[cfg(feature = "services-azblob")]
        Azblob => "azblob",
        #[cfg(feature = "services-azdls")]
        Azdls => "azdls",
        #[cfg(feature = "services-azfile")]
        Azfile => "azfile",
        #[cfg(feature = "services-b2")]
        B2 => "b2",
        #[cfg(feature = "services-cacache")]
        Cacache => "cacache",
        #[cfg(feature = "services-ceph-rgw")]
        CephRgw => "ceph-rgw",
        #[cfg(feature = "services-cos")]
        Cos => "cos",
        #[cfg(feature = "services-dashmap")]
        Dashmap => "dashmap",
        #[cfg(feature = "services-dropbox")]
        Dropbox => "dropbox",
        #[cfg(feature = "services-exoscale")]
        Exoscale => "exoscale",
        #[cfg(feature = "services-foyer")]
        Foyer => "foyer",
        #[cfg(feature = "services-fs")]
        Fs => "fs",
        #[cfg(feature = "services-ftp")]
        Ftp => "ftp",
        #[cfg(feature = "services-garage")]
        Garage => "garage",
        #[cfg(feature = "services-gcs")]
        Gcs => "gcs",
        #[cfg(feature = "services-gcs-grpc")]
        GcsGrpc => "gcs-grpc",
        #[cfg(feature = "services-gdrive")]
        Gdrive => "gdrive",
        #[cfg(feature = "services-ghac")]
        Ghac => "ghac",
        #[cfg(feature = "services-github")]
        Github => "github",
        #[cfg(feature = "services-gridfs")]
        Gridfs => "gridfs",
        #[cfg(feature = "services-hdfs-native")]
        HdfsNative => "hdfs-native",
        #[cfg(feature = "services-hetzner")]
        Hetzner => "hetzner",
        #[cfg(feature = "services-hf")]
        Hf => "hf",
        #[cfg(feature = "services-http")]
        Http => "http",
        #[cfg(feature = "services-ibm-cos")]
        IbmCos => "ibm-cos",
        #[cfg(feature = "services-idrive-e2")]
        IdriveE2 => "idrive-e2",
        #[cfg(feature = "services-ionos")]
        Ionos => "ionos",
        #[cfg(feature = "services-ipfs")]
        Ipfs => "ipfs",
        #[cfg(feature = "services-ipmfs")]
        Ipmfs => "ipmfs",
        #[cfg(feature = "services-koofr")]
        Koofr => "koofr",
        #[cfg(feature = "services-memcached")]
        Memcached => "memcached",
        #[cfg(feature = "services-memory")]
        Memory => "memory",
        #[cfg(feature = "services-mini-moka")]
        MiniMoka => "mini-moka",
        #[cfg(feature = "services-minio")]
        Minio => "minio",
        #[cfg(feature = "services-moka")]
        Moka => "moka",
        #[cfg(feature = "services-mongodb")]
        Mongodb => "mongodb",
        #[cfg(feature = "services-mysql")]
        Mysql => "mysql",
        #[cfg(feature = "services-neon")]
        Neon => "neon",
        #[cfg(feature = "services-obs")]
        Obs => "obs",
        #[cfg(feature = "services-oci")]
        Oci => "oci",
        #[cfg(feature = "services-onedrive")]
        Onedrive => "onedrive",
        #[cfg(feature = "services-oss")]
        Oss => "oss",
        #[cfg(feature = "services-ovhcloud")]
        Ovhcloud => "ovhcloud",
        #[cfg(feature = "services-persy")]
        Persy => "persy",
        #[cfg(feature = "services-postgresql")]
        Postgresql => "postgresql",
        #[cfg(feature = "services-qingstor")]
        Qingstor => "qingstor",
        #[cfg(feature = "services-r2")]
        R2 => "r2",
        #[cfg(feature = "services-redb")]
        Redb => "redb",
        #[cfg(feature = "services-redis")]
        Redis => "redis",
        #[cfg(feature = "services-rustfs")]
        Rustfs => "rustfs",
        #[cfg(feature = "services-s3")]
        S3 => "s3",
        #[cfg(feature = "services-scaleway")]
        Scaleway => "scaleway",
        #[cfg(feature = "services-seafile")]
        Seafile => "seafile",
        #[cfg(feature = "services-seaweedfs")]
        Seaweedfs => "seaweedfs",
        #[cfg(feature = "services-sftp")]
        Sftp => "sftp",
        #[cfg(feature = "services-sled")]
        Sled => "sled",
        #[cfg(feature = "services-spaces")]
        Spaces => "spaces",
        #[cfg(feature = "services-sqlite")]
        Sqlite => "sqlite",
        #[cfg(feature = "services-storj")]
        Storj => "storj",
        #[cfg(feature = "services-supabase")]
        Supabase => "supabase",
        #[cfg(feature = "services-swift")]
        Swift => "swift",
        #[cfg(feature = "services-tigris")]
        Tigris => "tigris",
        #[cfg(feature = "services-tos")]
        Tos => "tos",
        #[cfg(feature = "services-upyun")]
        Upyun => "upyun",
        #[cfg(feature = "services-vercel-artifacts")]
        VercelArtifacts => "vercel-artifacts",
        #[cfg(feature = "services-wasabi")]
        Wasabi => "wasabi",
        #[cfg(feature = "services-webdav")]
        Webdav => "webdav",
        #[cfg(feature = "services-webhdfs")]
        Webhdfs => "webhdfs",
        #[cfg(feature = "services-yandex-disk")]
        YandexDisk => "yandex-disk",
    }
);
