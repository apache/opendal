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
use std::collections::HashMap;

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
    #[cfg(feature = "services-ftp")]
    Ftp,
    #[cfg(feature = "services-gcs")]
    Gcs,
    #[cfg(feature = "services-gdrive")]
    Gdrive,
    #[cfg(feature = "services-ghac")]
    Ghac,
    #[cfg(feature = "services-goosefs")]
    Goosefs,
    #[cfg(feature = "services-gridfs")]
    Gridfs,
    #[cfg(feature = "services-hdfs-native")]
    HdfsNative,
    #[cfg(feature = "services-hf")]
    Hf,
    #[cfg(feature = "services-http")]
    Http,
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
    #[cfg(feature = "services-sftp")]
    Sftp,
    #[cfg(feature = "services-sled")]
    Sled,
    #[cfg(feature = "services-sqlite")]
    Sqlite,
    #[cfg(feature = "services-swift")]
    Swift,
    #[cfg(feature = "services-tos")]
    Tos,
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

#[pymethods]
impl Scheme {
    #[getter]
    pub fn name(&self) -> String {
        format!("{:?}", &self)
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
        #[cfg(feature = "services-cos")]
        Cos => "cos",
        #[cfg(feature = "services-dashmap")]
        Dashmap => "dashmap",
        #[cfg(feature = "services-dropbox")]
        Dropbox => "dropbox",
        #[cfg(feature = "services-fs")]
        Fs => "fs",
        #[cfg(feature = "services-ftp")]
        Ftp => "ftp",
        #[cfg(feature = "services-gcs")]
        Gcs => "gcs",
        #[cfg(feature = "services-gdrive")]
        Gdrive => "gdrive",
        #[cfg(feature = "services-ghac")]
        Ghac => "ghac",
        #[cfg(feature = "services-goosefs")]
        Goosefs => "goosefs",
        #[cfg(feature = "services-gridfs")]
        Gridfs => "gridfs",
        #[cfg(feature = "services-hdfs-native")]
        HdfsNative => "hdfs-native",
        #[cfg(feature = "services-hf")]
        Hf => "hf",
        #[cfg(feature = "services-http")]
        Http => "http",
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
        #[cfg(feature = "services-moka")]
        Moka => "moka",
        #[cfg(feature = "services-mongodb")]
        Mongodb => "mongodb",
        #[cfg(feature = "services-mysql")]
        Mysql => "mysql",
        #[cfg(feature = "services-obs")]
        Obs => "obs",
        #[cfg(feature = "services-onedrive")]
        Onedrive => "onedrive",
        #[cfg(feature = "services-oss")]
        Oss => "oss",
        #[cfg(feature = "services-persy")]
        Persy => "persy",
        #[cfg(feature = "services-postgresql")]
        Postgresql => "postgresql",
        #[cfg(feature = "services-redb")]
        Redb => "redb",
        #[cfg(feature = "services-redis")]
        Redis => "redis",
        #[cfg(feature = "services-s3")]
        S3 => "s3",
        #[cfg(feature = "services-seafile")]
        Seafile => "seafile",
        #[cfg(feature = "services-sftp")]
        Sftp => "sftp",
        #[cfg(feature = "services-sled")]
        Sled => "sled",
        #[cfg(feature = "services-sqlite")]
        Sqlite => "sqlite",
        #[cfg(feature = "services-swift")]
        Swift => "swift",
        #[cfg(feature = "services-tos")]
        Tos => "tos",
        #[cfg(feature = "services-upyun")]
        Upyun => "upyun",
        #[cfg(feature = "services-vercel-artifacts")]
        VercelArtifacts => "vercel-artifacts",
        #[cfg(feature = "services-webdav")]
        Webdav => "webdav",
        #[cfg(feature = "services-webhdfs")]
        Webhdfs => "webhdfs",
        #[cfg(feature = "services-yandex-disk")]
        YandexDisk => "yandex-disk",
    }
);

pub trait ConfigBuilder: Send + Sync {
    fn build(&self) -> PyResult<ocore::Operator>;
    fn scheme(&self) -> &'static str;
    fn to_map(&self) -> HashMap<String, String>;
    fn is_picklable(&self) -> bool;
}

/// Base class for all service configs.
#[pyclass(module = "opendal.services", subclass)]
pub struct ServiceConfig(pub Box<dyn ConfigBuilder>);

#[pymethods]
impl ServiceConfig {
    /// The service scheme this config targets, e.g. ``"s3"``.
    #[getter]
    pub fn scheme(&self) -> &'static str {
        self.0.scheme()
    }
}

#[cfg(feature = "services-aliyun-drive")]
/// Configuration for the `aliyun-drive` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct AliyunDriveConfig(ocore::services::AliyunDriveConfig);

#[cfg(feature = "services-aliyun-drive")]
#[allow(deprecated)]
impl ConfigBuilder for AliyunDriveConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "aliyun-drive"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("drive_type".to_string(), self.0.drive_type.clone());
        if let Some(v) = &self.0.access_token {
            map.insert("access_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_id {
            map.insert("client_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_secret {
            map.insert("client_secret".to_string(), v.clone());
        }
        if let Some(v) = &self.0.refresh_token {
            map.insert("refresh_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-aliyun-drive")]
#[allow(deprecated)]
#[pymethods]
impl AliyunDriveConfig {
    #[new]
    #[pyo3(signature = (
        drive_type,
        access_token = None,
        client_id = None,
        client_secret = None,
        refresh_token = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        drive_type: String,
        access_token: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        refresh_token: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("drive_type".to_string(), drive_type);
        if let Some(v) = access_token {
            opts.insert("access_token".to_string(), v);
        }
        if let Some(v) = client_id {
            opts.insert("client_id".to_string(), v);
        }
        if let Some(v) = client_secret {
            opts.insert("client_secret".to_string(), v);
        }
        if let Some(v) = refresh_token {
            opts.insert("refresh_token".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::AliyunDriveConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The drive_type of this backend.
    /// All operations will happen under this type of drive.
    /// Available values are `default`, `backup` and `resource`.
    /// Fallback to default if not set or no other drives can be found.
    #[getter]
    fn drive_type(&self) -> String {
        self.0.drive_type.clone()
    }
    /// The access_token of this backend.
    /// Solution for client-only purpose.
    /// #4733 Required if no client_id, client_secret and refresh_token are
    /// provided.
    #[getter]
    fn access_token(&self) -> Option<String> {
        self.0.access_token.clone()
    }
    /// The client_id of this backend.
    /// Required if no access_token is provided.
    #[getter]
    fn client_id(&self) -> Option<String> {
        self.0.client_id.clone()
    }
    /// The client_secret of this backend.
    /// Required if no access_token is provided.
    #[getter]
    fn client_secret(&self) -> Option<String> {
        self.0.client_secret.clone()
    }
    /// The refresh_token of this backend.
    /// Required if no access_token is provided.
    #[getter]
    fn refresh_token(&self) -> Option<String> {
        self.0.refresh_token.clone()
    }
    /// The Root of this backend.
    /// All operations will happen under this root.
    /// Default to `/` if not set.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-alluxio")]
/// Configuration for the `alluxio` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct AlluxioConfig(ocore::services::AlluxioConfig);

#[cfg(feature = "services-alluxio")]
#[allow(deprecated)]
impl ConfigBuilder for AlluxioConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "alluxio"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-alluxio")]
#[allow(deprecated)]
#[pymethods]
impl AlluxioConfig {
    #[new]
    #[pyo3(signature = (
        endpoint = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::AlluxioConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// endpoint of this backend.
    /// Endpoint must be full uri, mostly like `http://127.0.0.1:39999`.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// root of this backend.
    /// All operations will happen under this root.
    /// default to `/` if not set.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-azblob")]
/// Configuration for the `azblob` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct AzblobConfig(ocore::services::AzblobConfig);

#[cfg(feature = "services-azblob")]
#[allow(deprecated)]
impl ConfigBuilder for AzblobConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "azblob"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("container".to_string(), self.0.container.clone());
        if let Some(v) = &self.0.account_key {
            map.insert("account_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.account_name {
            map.insert("account_name".to_string(), v.clone());
        }
        if let Some(v) = &self.0.batch_max_operations {
            map.insert("batch_max_operations".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.encryption_algorithm {
            map.insert("encryption_algorithm".to_string(), v.clone());
        }
        if let Some(v) = &self.0.encryption_key {
            map.insert("encryption_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.encryption_key_sha256 {
            map.insert("encryption_key_sha256".to_string(), v.clone());
        }
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.sas_token {
            map.insert("sas_token".to_string(), v.clone());
        }
        map.insert(
            "skip_signature".to_string(),
            if self.0.skip_signature {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-azblob")]
#[allow(deprecated)]
#[pymethods]
impl AzblobConfig {
    #[new]
    #[pyo3(signature = (
        container,
        account_key = None,
        account_name = None,
        batch_max_operations = None,
        encryption_algorithm = None,
        encryption_key = None,
        encryption_key_sha256 = None,
        endpoint = None,
        root = None,
        sas_token = None,
        skip_signature = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        container: String,
        account_key: Option<String>,
        account_name: Option<String>,
        batch_max_operations: Option<usize>,
        encryption_algorithm: Option<String>,
        encryption_key: Option<String>,
        encryption_key_sha256: Option<String>,
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
        sas_token: Option<String>,
        skip_signature: Option<bool>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("container".to_string(), container);
        if let Some(v) = account_key {
            opts.insert("account_key".to_string(), v);
        }
        if let Some(v) = account_name {
            opts.insert("account_name".to_string(), v);
        }
        if let Some(v) = batch_max_operations {
            opts.insert("batch_max_operations".to_string(), v.to_string());
        }
        if let Some(v) = encryption_algorithm {
            opts.insert("encryption_algorithm".to_string(), v);
        }
        if let Some(v) = encryption_key {
            opts.insert("encryption_key".to_string(), v);
        }
        if let Some(v) = encryption_key_sha256 {
            opts.insert("encryption_key_sha256".to_string(), v);
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = sas_token {
            opts.insert("sas_token".to_string(), v);
        }
        if let Some(v) = skip_signature {
            opts.insert(
                "skip_signature".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::AzblobConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The container name of Azblob service backend.
    #[getter]
    fn container(&self) -> String {
        self.0.container.clone()
    }
    /// The account key of Azblob service backend.
    #[getter]
    fn account_key(&self) -> Option<String> {
        self.0.account_key.clone()
    }
    /// The account name of Azblob service backend.
    #[getter]
    fn account_name(&self) -> Option<String> {
        self.0.account_name.clone()
    }
    /// Deprecated: Azblob delete batch capability is enabled by default with Azure
    /// Blob's 256-operation batch limit.
    /// [Deprecated since 0.57.0] Azblob delete batch capability is enabled by
    /// default with Azure Blob's 256-operation batch limit.
    /// Use CapabilityOverrideLayer to override delete_max_size for specific
    /// endpoints.
    #[getter]
    fn batch_max_operations(&self) -> Option<usize> {
        self.0.batch_max_operations
    }
    /// The encryption algorithm of Azblob service backend.
    #[getter]
    fn encryption_algorithm(&self) -> Option<String> {
        self.0.encryption_algorithm.clone()
    }
    /// The encryption key of Azblob service backend.
    #[getter]
    fn encryption_key(&self) -> Option<String> {
        self.0.encryption_key.clone()
    }
    /// The encryption key sha256 of Azblob service backend.
    #[getter]
    fn encryption_key_sha256(&self) -> Option<String> {
        self.0.encryption_key_sha256.clone()
    }
    /// The endpoint of Azblob service backend.
    /// Endpoint must be full uri, e.g.
    /// - Azblob: `https://accountname.blob.core.windows.net` - Azurite:
    /// `http://127.0.0.1:10000/devstoreaccount1`
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// The root of Azblob service backend.
    /// All operations will happen under this root.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// The sas token of Azblob service backend.
    #[getter]
    fn sas_token(&self) -> Option<String> {
        self.0.sas_token.clone()
    }
    /// Skip signature will skip loading credentials and signing requests.
    #[getter]
    fn skip_signature(&self) -> Option<bool> {
        Some(self.0.skip_signature)
    }
}

#[cfg(feature = "services-azdls")]
/// Configuration for the `azdls` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct AzdlsConfig(ocore::services::AzdlsConfig);

#[cfg(feature = "services-azdls")]
#[allow(deprecated)]
impl ConfigBuilder for AzdlsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "azdls"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("filesystem".to_string(), self.0.filesystem.clone());
        if let Some(v) = &self.0.account_key {
            map.insert("account_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.account_name {
            map.insert("account_name".to_string(), v.clone());
        }
        if let Some(v) = &self.0.authority_host {
            map.insert("authority_host".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_id {
            map.insert("client_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_secret {
            map.insert("client_secret".to_string(), v.clone());
        }
        map.insert(
            "enable_hns".to_string(),
            if self.0.enable_hns { "true" } else { "false" }.to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.sas_token {
            map.insert("sas_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.tenant_id {
            map.insert("tenant_id".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-azdls")]
#[allow(deprecated)]
#[pymethods]
impl AzdlsConfig {
    #[new]
    #[pyo3(signature = (
        filesystem,
        account_key = None,
        account_name = None,
        authority_host = None,
        client_id = None,
        client_secret = None,
        enable_hns = None,
        endpoint = None,
        root = None,
        sas_token = None,
        tenant_id = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        filesystem: String,
        account_key: Option<String>,
        account_name: Option<String>,
        authority_host: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        enable_hns: Option<bool>,
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
        sas_token: Option<String>,
        tenant_id: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("filesystem".to_string(), filesystem);
        if let Some(v) = account_key {
            opts.insert("account_key".to_string(), v);
        }
        if let Some(v) = account_name {
            opts.insert("account_name".to_string(), v);
        }
        if let Some(v) = authority_host {
            opts.insert("authority_host".to_string(), v);
        }
        if let Some(v) = client_id {
            opts.insert("client_id".to_string(), v);
        }
        if let Some(v) = client_secret {
            opts.insert("client_secret".to_string(), v);
        }
        if let Some(v) = enable_hns {
            opts.insert(
                "enable_hns".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = sas_token {
            opts.insert("sas_token".to_string(), v);
        }
        if let Some(v) = tenant_id {
            opts.insert("tenant_id".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::AzdlsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Filesystem name of this backend.
    #[getter]
    fn filesystem(&self) -> String {
        self.0.filesystem.clone()
    }
    /// Account key of this backend.
    /// - required for shared_key authentication
    #[getter]
    fn account_key(&self) -> Option<String> {
        self.0.account_key.clone()
    }
    /// Account name of this backend.
    #[getter]
    fn account_name(&self) -> Option<String> {
        self.0.account_name.clone()
    }
    /// authority_host The authority host of the service principal.
    /// - required for client_credentials authentication - default value:
    /// `https://login.microsoftonline.com`
    #[getter]
    fn authority_host(&self) -> Option<String> {
        self.0.authority_host.clone()
    }
    /// client_id The client id of the service principal.
    /// - required for client_credentials authentication
    #[getter]
    fn client_id(&self) -> Option<String> {
        self.0.client_id.clone()
    }
    /// client_secret The client secret of the service principal.
    /// - required for client_credentials authentication
    #[getter]
    fn client_secret(&self) -> Option<String> {
        self.0.client_secret.clone()
    }
    /// Whether hierarchical namespace (HNS) is enabled for the storage account.
    /// When enabled, recursive deletion can use pagination to avoid timeouts on
    /// large directories.
    /// - default value: `false`
    #[getter]
    fn enable_hns(&self) -> Option<bool> {
        Some(self.0.enable_hns)
    }
    /// Endpoint of this backend.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// Root of this backend.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// sas_token The shared access signature token.
    /// - required for sas authentication
    #[getter]
    fn sas_token(&self) -> Option<String> {
        self.0.sas_token.clone()
    }
    /// tenant_id The tenant id of the service principal.
    /// - required for client_credentials authentication
    #[getter]
    fn tenant_id(&self) -> Option<String> {
        self.0.tenant_id.clone()
    }
}

#[cfg(feature = "services-azfile")]
/// Configuration for the `azfile` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct AzfileConfig(ocore::services::AzfileConfig);

#[cfg(feature = "services-azfile")]
#[allow(deprecated)]
impl ConfigBuilder for AzfileConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "azfile"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("share_name".to_string(), self.0.share_name.clone());
        if let Some(v) = &self.0.account_key {
            map.insert("account_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.account_name {
            map.insert("account_name".to_string(), v.clone());
        }
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.sas_token {
            map.insert("sas_token".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-azfile")]
#[allow(deprecated)]
#[pymethods]
impl AzfileConfig {
    #[new]
    #[pyo3(signature = (
        share_name,
        account_key = None,
        account_name = None,
        endpoint = None,
        root = None,
        sas_token = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        share_name: String,
        account_key: Option<String>,
        account_name: Option<String>,
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
        sas_token: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("share_name".to_string(), share_name);
        if let Some(v) = account_key {
            opts.insert("account_key".to_string(), v);
        }
        if let Some(v) = account_name {
            opts.insert("account_name".to_string(), v);
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = sas_token {
            opts.insert("sas_token".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::AzfileConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The share name for azfile.
    #[getter]
    fn share_name(&self) -> String {
        self.0.share_name.clone()
    }
    /// The account key for azfile.
    #[getter]
    fn account_key(&self) -> Option<String> {
        self.0.account_key.clone()
    }
    /// The account name for azfile.
    #[getter]
    fn account_name(&self) -> Option<String> {
        self.0.account_name.clone()
    }
    /// The endpoint for azfile.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// The root path for azfile.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// The sas token for azfile.
    #[getter]
    fn sas_token(&self) -> Option<String> {
        self.0.sas_token.clone()
    }
}

#[cfg(feature = "services-b2")]
/// Configuration for the `b2` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct B2Config(ocore::services::B2Config);

#[cfg(feature = "services-b2")]
#[allow(deprecated)]
impl ConfigBuilder for B2Config {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "b2"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("bucket".to_string(), self.0.bucket.clone());
        map.insert("bucket_id".to_string(), self.0.bucket_id.clone());
        if let Some(v) = &self.0.application_key {
            map.insert("application_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.application_key_id {
            map.insert("application_key_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-b2")]
#[allow(deprecated)]
#[pymethods]
impl B2Config {
    #[new]
    #[pyo3(signature = (
        bucket,
        bucket_id,
        application_key = None,
        application_key_id = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        bucket_id: String,
        application_key: Option<String>,
        application_key_id: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("bucket".to_string(), bucket);
        opts.insert("bucket_id".to_string(), bucket_id);
        if let Some(v) = application_key {
            opts.insert("application_key".to_string(), v);
        }
        if let Some(v) = application_key_id {
            opts.insert("application_key_id".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::B2Config as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// bucket of this backend.
    /// required.
    #[getter]
    fn bucket(&self) -> String {
        self.0.bucket.clone()
    }
    /// bucket id of this backend.
    /// required.
    #[getter]
    fn bucket_id(&self) -> String {
        self.0.bucket_id.clone()
    }
    /// applicationKey of this backend.
    /// - If application_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[getter]
    fn application_key(&self) -> Option<String> {
        self.0.application_key.clone()
    }
    /// keyID of this backend.
    /// - If application_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[getter]
    fn application_key_id(&self) -> Option<String> {
        self.0.application_key_id.clone()
    }
    /// root of this backend.
    /// All operations will happen under this root.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-cacache")]
/// Configuration for the `cacache` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct CacacheConfig(ocore::services::CacacheConfig);

#[cfg(feature = "services-cacache")]
#[allow(deprecated)]
impl ConfigBuilder for CacacheConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "cacache"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.datadir {
            map.insert("datadir".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-cacache")]
#[allow(deprecated)]
#[pymethods]
impl CacacheConfig {
    #[new]
    #[pyo3(signature = (
        datadir = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(datadir: Option<String>) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = datadir {
            opts.insert("datadir".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::CacacheConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// That path to the cacache data directory.
    #[getter]
    fn datadir(&self) -> Option<String> {
        self.0.datadir.clone()
    }
}

#[cfg(feature = "services-cos")]
/// Configuration for the `cos` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct CosConfig(ocore::services::CosConfig);

#[cfg(feature = "services-cos")]
#[allow(deprecated)]
impl ConfigBuilder for CosConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "cos"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.bucket {
            map.insert("bucket".to_string(), v.clone());
        }
        map.insert(
            "disable_config_load".to_string(),
            if self.0.disable_config_load {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "enable_versioning".to_string(),
            if self.0.enable_versioning {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.secret_id {
            map.insert("secret_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.secret_key {
            map.insert("secret_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.security_token {
            map.insert("security_token".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-cos")]
#[allow(deprecated)]
#[pymethods]
impl CosConfig {
    #[new]
    #[pyo3(signature = (
        bucket = None,
        disable_config_load = None,
        enable_versioning = None,
        endpoint = None,
        root = None,
        secret_id = None,
        secret_key = None,
        security_token = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: Option<String>,
        disable_config_load: Option<bool>,
        enable_versioning: Option<bool>,
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
        secret_id: Option<String>,
        secret_key: Option<String>,
        security_token: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = bucket {
            opts.insert("bucket".to_string(), v);
        }
        if let Some(v) = disable_config_load {
            opts.insert(
                "disable_config_load".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = enable_versioning {
            opts.insert(
                "enable_versioning".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = secret_id {
            opts.insert("secret_id".to_string(), v);
        }
        if let Some(v) = secret_key {
            opts.insert("secret_key".to_string(), v);
        }
        if let Some(v) = security_token {
            opts.insert("security_token".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::CosConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Bucket of this backend.
    #[getter]
    fn bucket(&self) -> Option<String> {
        self.0.bucket.clone()
    }
    /// Disable config load so that opendal will not load config from
    #[getter]
    fn disable_config_load(&self) -> Option<bool> {
        Some(self.0.disable_config_load)
    }
    /// Deprecated: COS versioning capability is enabled by default.
    /// [Deprecated since 0.57.0] COS versioning capability is enabled by default
    /// and this option is no longer needed.
    #[getter]
    fn enable_versioning(&self) -> Option<bool> {
        Some(self.0.enable_versioning)
    }
    /// Endpoint of this backend.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// Root of this backend.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Secret ID of this backend.
    #[getter]
    fn secret_id(&self) -> Option<String> {
        self.0.secret_id.clone()
    }
    /// Secret key of this backend.
    #[getter]
    fn secret_key(&self) -> Option<String> {
        self.0.secret_key.clone()
    }
    /// Security token (a.k.a.
    /// session token) of this backend.
    /// This is used for temporary credentials issued by Tencent Cloud STS (e.g.
    /// `GetFederationToken` / `AssumeRole`).
    /// When `security_token` is provided, it will be used together with `secret_id`
    /// and `secret_key` to sign requests, and the `x-cos-security-token` header
    /// will be attached automatically by the signer.
    /// If this field is not set, OpenDAL will also fall back to reading the token
    /// from environment variables `TENCENTCLOUD_TOKEN`,
    /// `TENCENTCLOUD_SECURITY_TOKEN` or `QCLOUD_SECRET_TOKEN` (unless
    /// `disable_config_load` is enabled).
    #[getter]
    fn security_token(&self) -> Option<String> {
        self.0.security_token.clone()
    }
}

#[cfg(feature = "services-dashmap")]
/// Configuration for the `dashmap` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct DashmapConfig(ocore::services::DashmapConfig);

#[cfg(feature = "services-dashmap")]
#[allow(deprecated)]
impl ConfigBuilder for DashmapConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "dashmap"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-dashmap")]
#[allow(deprecated)]
#[pymethods]
impl DashmapConfig {
    #[new]
    #[pyo3(signature = (
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(root: Option<crate::PyPath>) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::DashmapConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// root path of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-dropbox")]
/// Configuration for the `dropbox` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct DropboxConfig(ocore::services::DropboxConfig);

#[cfg(feature = "services-dropbox")]
#[allow(deprecated)]
impl ConfigBuilder for DropboxConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "dropbox"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.access_token {
            map.insert("access_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_id {
            map.insert("client_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_secret {
            map.insert("client_secret".to_string(), v.clone());
        }
        if let Some(v) = &self.0.refresh_token {
            map.insert("refresh_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-dropbox")]
#[allow(deprecated)]
#[pymethods]
impl DropboxConfig {
    #[new]
    #[pyo3(signature = (
        access_token = None,
        client_id = None,
        client_secret = None,
        refresh_token = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        access_token: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        refresh_token: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = access_token {
            opts.insert("access_token".to_string(), v);
        }
        if let Some(v) = client_id {
            opts.insert("client_id".to_string(), v);
        }
        if let Some(v) = client_secret {
            opts.insert("client_secret".to_string(), v);
        }
        if let Some(v) = refresh_token {
            opts.insert("refresh_token".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::DropboxConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// access token for dropbox.
    #[getter]
    fn access_token(&self) -> Option<String> {
        self.0.access_token.clone()
    }
    /// client_id for dropbox.
    #[getter]
    fn client_id(&self) -> Option<String> {
        self.0.client_id.clone()
    }
    /// client_secret for dropbox.
    #[getter]
    fn client_secret(&self) -> Option<String> {
        self.0.client_secret.clone()
    }
    /// refresh_token for dropbox.
    #[getter]
    fn refresh_token(&self) -> Option<String> {
        self.0.refresh_token.clone()
    }
    /// root path for dropbox.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-fs")]
/// Configuration for the `fs` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct FsConfig(ocore::services::FsConfig);

#[cfg(feature = "services-fs")]
#[allow(deprecated)]
impl ConfigBuilder for FsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "fs"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.atomic_write_dir {
            map.insert("atomic_write_dir".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-fs")]
#[allow(deprecated)]
#[pymethods]
impl FsConfig {
    #[new]
    #[pyo3(signature = (
        atomic_write_dir = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        atomic_write_dir: Option<crate::PyPath>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = atomic_write_dir {
            opts.insert("atomic_write_dir".to_string(), v.into_string());
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::FsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// tmp dir for atomic write
    #[getter]
    fn atomic_write_dir(&self) -> Option<crate::PyPath> {
        self.0.atomic_write_dir.clone().map(crate::PyPath::from)
    }
    /// root dir for backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-ftp")]
/// Configuration for the `ftp` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct FtpConfig(ocore::services::FtpConfig);

#[cfg(feature = "services-ftp")]
#[allow(deprecated)]
impl ConfigBuilder for FtpConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "ftp"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.password {
            map.insert("password".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.user {
            map.insert("user".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-ftp")]
#[allow(deprecated)]
#[pymethods]
impl FtpConfig {
    #[new]
    #[pyo3(signature = (
        endpoint = None,
        password = None,
        root = None,
        user = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: Option<String>,
        password: Option<String>,
        root: Option<crate::PyPath>,
        user: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = password {
            opts.insert("password".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = user {
            opts.insert("user".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::FtpConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// endpoint of this backend
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// password of this backend
    #[getter]
    fn password(&self) -> Option<String> {
        self.0.password.clone()
    }
    /// root of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// user of this backend
    #[getter]
    fn user(&self) -> Option<String> {
        self.0.user.clone()
    }
}

#[cfg(feature = "services-gcs")]
/// Configuration for the `gcs` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct GcsConfig(ocore::services::GcsConfig);

#[cfg(feature = "services-gcs")]
#[allow(deprecated)]
impl ConfigBuilder for GcsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "gcs"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("bucket".to_string(), self.0.bucket.clone());
        map.insert(
            "allow_anonymous".to_string(),
            if self.0.allow_anonymous {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.credential {
            map.insert("credential".to_string(), v.clone());
        }
        if let Some(v) = &self.0.credential_path {
            map.insert("credential_path".to_string(), v.clone());
        }
        if let Some(v) = &self.0.default_storage_class {
            map.insert("default_storage_class".to_string(), v.clone());
        }
        map.insert(
            "disable_config_load".to_string(),
            if self.0.disable_config_load {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "disable_vm_metadata".to_string(),
            if self.0.disable_vm_metadata {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.predefined_acl {
            map.insert("predefined_acl".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.scope {
            map.insert("scope".to_string(), v.clone());
        }
        if let Some(v) = &self.0.service_account {
            map.insert("service_account".to_string(), v.clone());
        }
        map.insert(
            "skip_signature".to_string(),
            if self.0.skip_signature {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.token {
            map.insert("token".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-gcs")]
#[allow(deprecated)]
#[pymethods]
impl GcsConfig {
    #[new]
    #[pyo3(signature = (
        bucket,
        allow_anonymous = None,
        credential = None,
        credential_path = None,
        default_storage_class = None,
        disable_config_load = None,
        disable_vm_metadata = None,
        endpoint = None,
        predefined_acl = None,
        root = None,
        scope = None,
        service_account = None,
        skip_signature = None,
        token = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        allow_anonymous: Option<bool>,
        credential: Option<String>,
        credential_path: Option<String>,
        default_storage_class: Option<String>,
        disable_config_load: Option<bool>,
        disable_vm_metadata: Option<bool>,
        endpoint: Option<String>,
        predefined_acl: Option<String>,
        root: Option<crate::PyPath>,
        scope: Option<String>,
        service_account: Option<String>,
        skip_signature: Option<bool>,
        token: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("bucket".to_string(), bucket);
        if let Some(v) = allow_anonymous {
            opts.insert(
                "allow_anonymous".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = credential {
            opts.insert("credential".to_string(), v);
        }
        if let Some(v) = credential_path {
            opts.insert("credential_path".to_string(), v);
        }
        if let Some(v) = default_storage_class {
            opts.insert("default_storage_class".to_string(), v);
        }
        if let Some(v) = disable_config_load {
            opts.insert(
                "disable_config_load".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = disable_vm_metadata {
            opts.insert(
                "disable_vm_metadata".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = predefined_acl {
            opts.insert("predefined_acl".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = scope {
            opts.insert("scope".to_string(), v);
        }
        if let Some(v) = service_account {
            opts.insert("service_account".to_string(), v);
        }
        if let Some(v) = skip_signature {
            opts.insert(
                "skip_signature".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = token {
            opts.insert("token".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::GcsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// bucket name
    #[getter]
    fn bucket(&self) -> String {
        self.0.bucket.clone()
    }
    /// Allow opendal to send requests without signing when credentials are not
    /// loaded.
    /// [Deprecated since 0.57.0] Please use `skip_signature` instead of
    /// `allow_anonymous`
    #[getter]
    fn allow_anonymous(&self) -> Option<bool> {
        Some(self.0.allow_anonymous)
    }
    /// Credentials string for GCS service OAuth2 authentication.
    #[getter]
    fn credential(&self) -> Option<String> {
        self.0.credential.clone()
    }
    /// Local path to credentials file for GCS service OAuth2 authentication.
    #[getter]
    fn credential_path(&self) -> Option<String> {
        self.0.credential_path.clone()
    }
    /// The default storage class used by gcs.
    #[getter]
    fn default_storage_class(&self) -> Option<String> {
        self.0.default_storage_class.clone()
    }
    /// Disable loading configuration from the environment.
    #[getter]
    fn disable_config_load(&self) -> Option<bool> {
        Some(self.0.disable_config_load)
    }
    /// Disable attempting to load credentials from the GCE metadata server when
    /// running within Google Cloud.
    #[getter]
    fn disable_vm_metadata(&self) -> Option<bool> {
        Some(self.0.disable_vm_metadata)
    }
    /// endpoint URI of GCS service, default is `https://storage.googleapis.com`
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// The predefined acl for GCS.
    #[getter]
    fn predefined_acl(&self) -> Option<String> {
        self.0.predefined_acl.clone()
    }
    /// root URI, all operations happens under `root`
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Scope for gcs.
    #[getter]
    fn scope(&self) -> Option<String> {
        self.0.scope.clone()
    }
    /// Service Account for gcs.
    #[getter]
    fn service_account(&self) -> Option<String> {
        self.0.service_account.clone()
    }
    /// Skip signature will skip loading credentials and signing requests.
    #[getter]
    fn skip_signature(&self) -> Option<bool> {
        Some(self.0.skip_signature)
    }
    /// A Google Cloud OAuth2 token.
    /// Takes precedence over `credential` and `credential_path`.
    #[getter]
    fn token(&self) -> Option<String> {
        self.0.token.clone()
    }
}

#[cfg(feature = "services-gdrive")]
/// Configuration for the `gdrive` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct GdriveConfig(ocore::services::GdriveConfig);

#[cfg(feature = "services-gdrive")]
#[allow(deprecated)]
impl ConfigBuilder for GdriveConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "gdrive"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.access_token {
            map.insert("access_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_id {
            map.insert("client_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_secret {
            map.insert("client_secret".to_string(), v.clone());
        }
        if let Some(v) = &self.0.refresh_token {
            map.insert("refresh_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-gdrive")]
#[allow(deprecated)]
#[pymethods]
impl GdriveConfig {
    #[new]
    #[pyo3(signature = (
        access_token = None,
        client_id = None,
        client_secret = None,
        refresh_token = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        access_token: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        refresh_token: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = access_token {
            opts.insert("access_token".to_string(), v);
        }
        if let Some(v) = client_id {
            opts.insert("client_id".to_string(), v);
        }
        if let Some(v) = client_secret {
            opts.insert("client_secret".to_string(), v);
        }
        if let Some(v) = refresh_token {
            opts.insert("refresh_token".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::GdriveConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Access token for gdrive.
    #[getter]
    fn access_token(&self) -> Option<String> {
        self.0.access_token.clone()
    }
    /// Client id for gdrive.
    #[getter]
    fn client_id(&self) -> Option<String> {
        self.0.client_id.clone()
    }
    /// Client secret for gdrive.
    #[getter]
    fn client_secret(&self) -> Option<String> {
        self.0.client_secret.clone()
    }
    /// Refresh token for gdrive.
    #[getter]
    fn refresh_token(&self) -> Option<String> {
        self.0.refresh_token.clone()
    }
    /// The root for gdrive
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-ghac")]
/// Configuration for the `ghac` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct GhacConfig(ocore::services::GhacConfig);

#[cfg(feature = "services-ghac")]
#[allow(deprecated)]
impl ConfigBuilder for GhacConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "ghac"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.runtime_token {
            map.insert("runtime_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.version {
            map.insert("version".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-ghac")]
#[allow(deprecated)]
#[pymethods]
impl GhacConfig {
    #[new]
    #[pyo3(signature = (
        endpoint = None,
        root = None,
        runtime_token = None,
        version = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
        runtime_token: Option<String>,
        version: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = runtime_token {
            opts.insert("runtime_token".to_string(), v);
        }
        if let Some(v) = version {
            opts.insert("version".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::GhacConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The endpoint for ghac service.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// The root path for ghac.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// The runtime token for ghac service.
    #[getter]
    fn runtime_token(&self) -> Option<String> {
        self.0.runtime_token.clone()
    }
    /// The version that used by cache.
    #[getter]
    fn version(&self) -> Option<String> {
        self.0.version.clone()
    }
}

#[cfg(feature = "services-goosefs")]
/// Configuration for the `goosefs` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct GoosefsConfig(ocore::services::GoosefsConfig);

#[cfg(feature = "services-goosefs")]
#[allow(deprecated)]
impl ConfigBuilder for GoosefsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "goosefs"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.auth_type {
            map.insert("auth_type".to_string(), v.clone());
        }
        if let Some(v) = &self.0.auth_username {
            map.insert("auth_username".to_string(), v.clone());
        }
        if let Some(v) = &self.0.block_size {
            map.insert("block_size".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.chunk_size {
            map.insert("chunk_size".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.master_addr {
            map.insert("master_addr".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.write_type {
            map.insert("write_type".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-goosefs")]
#[allow(deprecated)]
#[pymethods]
impl GoosefsConfig {
    #[new]
    #[pyo3(signature = (
        auth_type = None,
        auth_username = None,
        block_size = None,
        chunk_size = None,
        master_addr = None,
        root = None,
        write_type = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        auth_type: Option<String>,
        auth_username: Option<String>,
        block_size: Option<u64>,
        chunk_size: Option<u64>,
        master_addr: Option<String>,
        root: Option<crate::PyPath>,
        write_type: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = auth_type {
            opts.insert("auth_type".to_string(), v);
        }
        if let Some(v) = auth_username {
            opts.insert("auth_username".to_string(), v);
        }
        if let Some(v) = block_size {
            opts.insert("block_size".to_string(), v.to_string());
        }
        if let Some(v) = chunk_size {
            opts.insert("chunk_size".to_string(), v.to_string());
        }
        if let Some(v) = master_addr {
            opts.insert("master_addr".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = write_type {
            opts.insert("write_type".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::GoosefsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Authentication type.
    /// Supported values: `"nosasl"`, `"simple"`.
    /// Default: `"simple"` — PLAIN SASL with usernam
    /// e. `"nosasl"` — skip authentication entirely.
    #[getter]
    fn auth_type(&self) -> Option<String> {
        self.0.auth_type.clone()
    }
    /// Authentication username.
    /// Used in SIMPLE mode as the login identity.
    /// Default: current OS user (`$USER` / `$USERNAME`).
    #[getter]
    fn auth_username(&self) -> Option<String> {
        self.0.auth_username.clone()
    }
    /// Block size in bytes for new files (default: 64 MiB).
    #[getter]
    fn block_size(&self) -> Option<u64> {
        self.0.block_size
    }
    /// Chunk size in bytes for streaming RPCs (default: 1 MiB).
    #[getter]
    fn chunk_size(&self) -> Option<u64> {
        self.0.chunk_size
    }
    /// Master address(es) in `host:port` format.
    /// For single master: `"10.0.0.1:9200"` For HA (comma-separated):
    /// `"10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200"` When multiple addresses are
    /// provided, the client uses `PollingMasterInquireClient` to discover the
    /// Primary Master automatically.
    /// Resolution precedence at `build()` time (highest → lowest), following
    /// `goosefs-sdk` `docs/CLIENT_CONFIGURATION.md` §1:
    /// 1. This field (when set on the builder / OpenDAL config map)
    /// 2. `GOOSEFS_MASTER_ADDR` environment variable
    /// 3. `goosefs.master.rpc.addresses` / `goosefs.master.hostname` in
    /// `goosefs-site.properties` `build()` fails with `ConfigInvalid` only when
    /// **none** of the above supplies a master address.
    #[getter]
    fn master_addr(&self) -> Option<String> {
        self.0.master_addr.clone()
    }
    /// Root path of this backend.
    /// All operations will happen under this root.
    /// Default to `/` if not set.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Default write type for new files.
    /// Supported values: `"must_cache"`, `"cache_through"`, `"through"`,
    /// `"async_through"`.
    /// Default: `"must_cache"`.
    #[getter]
    fn write_type(&self) -> Option<String> {
        self.0.write_type.clone()
    }
}

#[cfg(feature = "services-gridfs")]
/// Configuration for the `gridfs` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct GridfsConfig(ocore::services::GridfsConfig);

#[cfg(feature = "services-gridfs")]
#[allow(deprecated)]
impl ConfigBuilder for GridfsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "gridfs"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.bucket {
            map.insert("bucket".to_string(), v.clone());
        }
        if let Some(v) = &self.0.chunk_size {
            map.insert("chunk_size".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.connection_string {
            map.insert("connection_string".to_string(), v.clone());
        }
        if let Some(v) = &self.0.database {
            map.insert("database".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-gridfs")]
#[allow(deprecated)]
#[pymethods]
impl GridfsConfig {
    #[new]
    #[pyo3(signature = (
        bucket = None,
        chunk_size = None,
        connection_string = None,
        database = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: Option<String>,
        chunk_size: Option<u32>,
        connection_string: Option<String>,
        database: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = bucket {
            opts.insert("bucket".to_string(), v);
        }
        if let Some(v) = chunk_size {
            opts.insert("chunk_size".to_string(), v.to_string());
        }
        if let Some(v) = connection_string {
            opts.insert("connection_string".to_string(), v);
        }
        if let Some(v) = database {
            opts.insert("database".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::GridfsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The bucket name of the MongoDB GridFs service to read/write.
    #[getter]
    fn bucket(&self) -> Option<String> {
        self.0.bucket.clone()
    }
    /// The chunk size of the MongoDB GridFs service used to break the user file
    /// into chunks.
    #[getter]
    fn chunk_size(&self) -> Option<u32> {
        self.0.chunk_size
    }
    /// The connection string of the MongoDB service.
    #[getter]
    fn connection_string(&self) -> Option<String> {
        self.0.connection_string.clone()
    }
    /// The database name of the MongoDB GridFs service to read/write.
    #[getter]
    fn database(&self) -> Option<String> {
        self.0.database.clone()
    }
    /// The working directory, all operations will be performed under it.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-hdfs-native")]
/// Configuration for the `hdfs-native` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct HdfsNativeConfig(ocore::services::HdfsNativeConfig);

#[cfg(feature = "services-hdfs-native")]
#[allow(deprecated)]
impl ConfigBuilder for HdfsNativeConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "hdfs-native"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert(
            "enable_append".to_string(),
            if self.0.enable_append {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.name_node {
            map.insert("name_node".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        if self.0.options.is_some() {
            ok = false;
        }
        ok
    }
}

#[cfg(feature = "services-hdfs-native")]
#[allow(deprecated)]
#[pymethods]
impl HdfsNativeConfig {
    #[new]
    #[pyo3(signature = (
        enable_append = None,
        name_node = None,
        options = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        enable_append: Option<bool>,
        name_node: Option<String>,
        options: Option<std::collections::HashMap<String, String>>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = enable_append {
            opts.insert(
                "enable_append".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = name_node {
            opts.insert("name_node".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::HdfsNativeConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        if let Some(v) = options {
            cfg.options = Some(v);
        }
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Deprecated: HDFS Native append capability is enabled by default.
    /// [Deprecated since 0.57.0] HDFS Native append capability is enabled by
    /// default and this option is no longer needed.
    #[getter]
    fn enable_append(&self) -> Option<bool> {
        Some(self.0.enable_append)
    }
    /// name_node of this backend
    #[getter]
    fn name_node(&self) -> Option<String> {
        self.0.name_node.clone()
    }
    /// other options for hdfs client
    #[getter]
    fn options(&self) -> Option<std::collections::HashMap<String, String>> {
        self.0.options.clone()
    }
    /// work dir of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-hf")]
/// Configuration for the `hf` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct HfConfig(ocore::services::HfConfig);

#[cfg(feature = "services-hf")]
#[allow(deprecated)]
impl ConfigBuilder for HfConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "hf"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.repo_id {
            map.insert("repo_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.revision {
            map.insert("revision".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.token {
            map.insert("token".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-hf")]
#[allow(deprecated)]
#[pymethods]
impl HfConfig {
    #[new]
    #[pyo3(signature = (
        download_mode = None,
        endpoint = None,
        repo_id = None,
        repo_type = None,
        revision = None,
        root = None,
        token = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        download_mode: Option<String>,
        endpoint: Option<String>,
        repo_id: Option<String>,
        repo_type: Option<String>,
        revision: Option<String>,
        root: Option<crate::PyPath>,
        token: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = download_mode {
            opts.insert("download_mode".to_string(), v);
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = repo_id {
            opts.insert("repo_id".to_string(), v);
        }
        if let Some(v) = repo_type {
            opts.insert("repo_type".to_string(), v);
        }
        if let Some(v) = revision {
            opts.insert("revision".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = token {
            opts.insert("token".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::HfConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Endpoint of the Hugging Face Hub.
    /// Default is "https://huggingface.co".
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// Repo id of this backend.
    /// This is required.
    #[getter]
    fn repo_id(&self) -> Option<String> {
        self.0.repo_id.clone()
    }
    /// Revision of this backend.
    /// Default is main.
    #[getter]
    fn revision(&self) -> Option<String> {
        self.0.revision.clone()
    }
    /// Root of this backend.
    /// Can be "/path/to/dir".
    /// Default is "/".
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Token of this backend.
    /// This is optional.
    #[getter]
    fn token(&self) -> Option<String> {
        self.0.token.clone()
    }
}

#[cfg(feature = "services-http")]
/// Configuration for the `http` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct HttpConfig(ocore::services::HttpConfig);

#[cfg(feature = "services-http")]
#[allow(deprecated)]
impl ConfigBuilder for HttpConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "http"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.password {
            map.insert("password".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.token {
            map.insert("token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.username {
            map.insert("username".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-http")]
#[allow(deprecated)]
#[pymethods]
impl HttpConfig {
    #[new]
    #[pyo3(signature = (
        endpoint = None,
        password = None,
        root = None,
        token = None,
        username = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: Option<String>,
        password: Option<String>,
        root: Option<crate::PyPath>,
        token: Option<String>,
        username: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = password {
            opts.insert("password".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = token {
            opts.insert("token".to_string(), v);
        }
        if let Some(v) = username {
            opts.insert("username".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::HttpConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// endpoint of this backend
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// password of this backend
    #[getter]
    fn password(&self) -> Option<String> {
        self.0.password.clone()
    }
    /// root of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// token of this backend
    #[getter]
    fn token(&self) -> Option<String> {
        self.0.token.clone()
    }
    /// username of this backend
    #[getter]
    fn username(&self) -> Option<String> {
        self.0.username.clone()
    }
}

#[cfg(feature = "services-ipfs")]
/// Configuration for the `ipfs` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct IpfsConfig(ocore::services::IpfsConfig);

#[cfg(feature = "services-ipfs")]
#[allow(deprecated)]
impl ConfigBuilder for IpfsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "ipfs"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-ipfs")]
#[allow(deprecated)]
#[pymethods]
impl IpfsConfig {
    #[new]
    #[pyo3(signature = (
        endpoint = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::IpfsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// IPFS gateway endpoint.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// IPFS root.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-ipmfs")]
/// Configuration for the `ipmfs` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct IpmfsConfig(ocore::services::IpmfsConfig);

#[cfg(feature = "services-ipmfs")]
#[allow(deprecated)]
impl ConfigBuilder for IpmfsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "ipmfs"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-ipmfs")]
#[allow(deprecated)]
#[pymethods]
impl IpmfsConfig {
    #[new]
    #[pyo3(signature = (
        endpoint = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::IpmfsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Endpoint for ipfs.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// Root for ipfs.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-koofr")]
/// Configuration for the `koofr` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct KoofrConfig(ocore::services::KoofrConfig);

#[cfg(feature = "services-koofr")]
#[allow(deprecated)]
impl ConfigBuilder for KoofrConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "koofr"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("email".to_string(), self.0.email.clone());
        map.insert("endpoint".to_string(), self.0.endpoint.clone());
        if let Some(v) = &self.0.password {
            map.insert("password".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-koofr")]
#[allow(deprecated)]
#[pymethods]
impl KoofrConfig {
    #[new]
    #[pyo3(signature = (
        email,
        endpoint,
        password = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        email: String,
        endpoint: String,
        password: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("email".to_string(), email);
        opts.insert("endpoint".to_string(), endpoint);
        if let Some(v) = password {
            opts.insert("password".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::KoofrConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Koofr email.
    #[getter]
    fn email(&self) -> String {
        self.0.email.clone()
    }
    /// Koofr endpoint.
    #[getter]
    fn endpoint(&self) -> String {
        self.0.endpoint.clone()
    }
    /// password of this backend.
    /// (Must be the application password)
    #[getter]
    fn password(&self) -> Option<String> {
        self.0.password.clone()
    }
    /// root of this backend.
    /// All operations will happen under this root.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-memcached")]
/// Configuration for the `memcached` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct MemcachedConfig(ocore::services::MemcachedConfig);

#[cfg(feature = "services-memcached")]
#[allow(deprecated)]
impl ConfigBuilder for MemcachedConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "memcached"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.connection_pool_max_size {
            map.insert("connection_pool_max_size".to_string(), v.to_string());
        }
        if let Some(d) = &self.0.default_ttl {
            map.insert("default_ttl".to_string(), format!("{}s", d.as_secs()));
        }
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.password {
            map.insert("password".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.username {
            map.insert("username".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-memcached")]
#[allow(deprecated)]
#[pymethods]
impl MemcachedConfig {
    #[new]
    #[pyo3(signature = (
        connection_pool_max_size = None,
        default_ttl = None,
        endpoint = None,
        password = None,
        root = None,
        username = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        connection_pool_max_size: Option<usize>,
        default_ttl: Option<String>,
        endpoint: Option<String>,
        password: Option<String>,
        root: Option<crate::PyPath>,
        username: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = connection_pool_max_size {
            opts.insert("connection_pool_max_size".to_string(), v.to_string());
        }
        if let Some(v) = default_ttl {
            opts.insert("default_ttl".to_string(), v);
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = password {
            opts.insert("password".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = username {
            opts.insert("username".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::MemcachedConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The maximum number of connections allowed.
    /// default is 10
    #[getter]
    fn connection_pool_max_size(&self) -> Option<usize> {
        self.0.connection_pool_max_size
    }
    /// The default ttl for put operations.
    /// Accepts a humantime duration string (e.g.
    /// "5s").
    #[getter]
    fn default_ttl(&self) -> Option<String> {
        self.0.default_ttl.map(|d| format!("{}s", d.as_secs()))
    }
    /// network address of the memcached service.
    /// For example: "tcp://localhost:11211"
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// Memcached password, optional.
    #[getter]
    fn password(&self) -> Option<String> {
        self.0.password.clone()
    }
    /// the working directory of the service.
    /// Can be "/path/to/dir" default is "/"
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Memcached username, optional.
    #[getter]
    fn username(&self) -> Option<String> {
        self.0.username.clone()
    }
}

#[cfg(feature = "services-memory")]
/// Configuration for the `memory` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct MemoryConfig(ocore::services::MemoryConfig);

#[cfg(feature = "services-memory")]
#[allow(deprecated)]
impl ConfigBuilder for MemoryConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "memory"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-memory")]
#[allow(deprecated)]
#[pymethods]
impl MemoryConfig {
    #[new]
    #[pyo3(signature = (
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(root: Option<crate::PyPath>) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::MemoryConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// root of the backend.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-mini-moka")]
/// Configuration for the `mini-moka` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct MiniMokaConfig(ocore::services::MiniMokaConfig);

#[cfg(feature = "services-mini-moka")]
#[allow(deprecated)]
impl ConfigBuilder for MiniMokaConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "mini-moka"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.max_capacity {
            map.insert("max_capacity".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.time_to_idle {
            map.insert("time_to_idle".to_string(), v.clone());
        }
        if let Some(v) = &self.0.time_to_live {
            map.insert("time_to_live".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-mini-moka")]
#[allow(deprecated)]
#[pymethods]
impl MiniMokaConfig {
    #[new]
    #[pyo3(signature = (
        max_capacity = None,
        root = None,
        time_to_idle = None,
        time_to_live = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_capacity: Option<u64>,
        root: Option<crate::PyPath>,
        time_to_idle: Option<String>,
        time_to_live: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = max_capacity {
            opts.insert("max_capacity".to_string(), v.to_string());
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = time_to_idle {
            opts.insert("time_to_idle".to_string(), v);
        }
        if let Some(v) = time_to_live {
            opts.insert("time_to_live".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::MiniMokaConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Sets the max capacity of the cache.
    /// Refer to
    /// [`mini-moka::sync::CacheBuilder::max_capacity`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.max_capacity)
    #[getter]
    fn max_capacity(&self) -> Option<u64> {
        self.0.max_capacity
    }
    /// root path of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Sets the time to idle of the cache.
    /// Refer to
    /// [`mini-moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    #[getter]
    fn time_to_idle(&self) -> Option<String> {
        self.0.time_to_idle.clone()
    }
    /// Sets the time to live of the cache.
    /// Refer to
    /// [`mini-moka::sync::CacheBuilder::time_to_live`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_live)
    #[getter]
    fn time_to_live(&self) -> Option<String> {
        self.0.time_to_live.clone()
    }
}

#[cfg(feature = "services-moka")]
/// Configuration for the `moka` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct MokaConfig(ocore::services::MokaConfig);

#[cfg(feature = "services-moka")]
#[allow(deprecated)]
impl ConfigBuilder for MokaConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "moka"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.max_capacity {
            map.insert("max_capacity".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.name {
            map.insert("name".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.time_to_idle {
            map.insert("time_to_idle".to_string(), v.clone());
        }
        if let Some(v) = &self.0.time_to_live {
            map.insert("time_to_live".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-moka")]
#[allow(deprecated)]
#[pymethods]
impl MokaConfig {
    #[new]
    #[pyo3(signature = (
        max_capacity = None,
        name = None,
        root = None,
        time_to_idle = None,
        time_to_live = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_capacity: Option<u64>,
        name: Option<String>,
        root: Option<crate::PyPath>,
        time_to_idle: Option<String>,
        time_to_live: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = max_capacity {
            opts.insert("max_capacity".to_string(), v.to_string());
        }
        if let Some(v) = name {
            opts.insert("name".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = time_to_idle {
            opts.insert("time_to_idle".to_string(), v);
        }
        if let Some(v) = time_to_live {
            opts.insert("time_to_live".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::MokaConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Sets the max capacity of the cache.
    /// Refer to
    /// [`moka::future::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.max_capacity)
    #[getter]
    fn max_capacity(&self) -> Option<u64> {
        self.0.max_capacity
    }
    /// Name for this cache instance.
    #[getter]
    fn name(&self) -> Option<String> {
        self.0.name.clone()
    }
    /// root path of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Sets the time to idle of the cache.
    /// Refer to
    /// [`moka::future::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.time_to_idle)
    #[getter]
    fn time_to_idle(&self) -> Option<String> {
        self.0.time_to_idle.clone()
    }
    /// Sets the time to live of the cache.
    /// Refer to
    /// [`moka::future::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.time_to_live)
    #[getter]
    fn time_to_live(&self) -> Option<String> {
        self.0.time_to_live.clone()
    }
}

#[cfg(feature = "services-mongodb")]
/// Configuration for the `mongodb` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct MongodbConfig(ocore::services::MongodbConfig);

#[cfg(feature = "services-mongodb")]
#[allow(deprecated)]
impl ConfigBuilder for MongodbConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "mongodb"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.collection {
            map.insert("collection".to_string(), v.clone());
        }
        if let Some(v) = &self.0.connection_string {
            map.insert("connection_string".to_string(), v.clone());
        }
        if let Some(v) = &self.0.database {
            map.insert("database".to_string(), v.clone());
        }
        if let Some(v) = &self.0.key_field {
            map.insert("key_field".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.value_field {
            map.insert("value_field".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-mongodb")]
#[allow(deprecated)]
#[pymethods]
impl MongodbConfig {
    #[new]
    #[pyo3(signature = (
        collection = None,
        connection_string = None,
        database = None,
        key_field = None,
        root = None,
        value_field = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        collection: Option<String>,
        connection_string: Option<String>,
        database: Option<String>,
        key_field: Option<String>,
        root: Option<crate::PyPath>,
        value_field: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = collection {
            opts.insert("collection".to_string(), v);
        }
        if let Some(v) = connection_string {
            opts.insert("connection_string".to_string(), v);
        }
        if let Some(v) = database {
            opts.insert("database".to_string(), v);
        }
        if let Some(v) = key_field {
            opts.insert("key_field".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = value_field {
            opts.insert("value_field".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::MongodbConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// collection of this backend
    #[getter]
    fn collection(&self) -> Option<String> {
        self.0.collection.clone()
    }
    /// connection string of this backend
    #[getter]
    fn connection_string(&self) -> Option<String> {
        self.0.connection_string.clone()
    }
    /// database of this backend
    #[getter]
    fn database(&self) -> Option<String> {
        self.0.database.clone()
    }
    /// key field of this backend
    #[getter]
    fn key_field(&self) -> Option<String> {
        self.0.key_field.clone()
    }
    /// root of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// value field of this backend
    #[getter]
    fn value_field(&self) -> Option<String> {
        self.0.value_field.clone()
    }
}

#[cfg(feature = "services-mysql")]
/// Configuration for the `mysql` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct MysqlConfig(ocore::services::MysqlConfig);

#[cfg(feature = "services-mysql")]
#[allow(deprecated)]
impl ConfigBuilder for MysqlConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "mysql"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.connection_string {
            map.insert("connection_string".to_string(), v.clone());
        }
        if let Some(v) = &self.0.key_field {
            map.insert("key_field".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.table {
            map.insert("table".to_string(), v.clone());
        }
        if let Some(v) = &self.0.value_field {
            map.insert("value_field".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-mysql")]
#[allow(deprecated)]
#[pymethods]
impl MysqlConfig {
    #[new]
    #[pyo3(signature = (
        connection_string = None,
        key_field = None,
        root = None,
        table = None,
        value_field = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        connection_string: Option<String>,
        key_field: Option<String>,
        root: Option<crate::PyPath>,
        table: Option<String>,
        value_field: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = connection_string {
            opts.insert("connection_string".to_string(), v);
        }
        if let Some(v) = key_field {
            opts.insert("key_field".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = table {
            opts.insert("table".to_string(), v);
        }
        if let Some(v) = value_field {
            opts.insert("value_field".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::MysqlConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// This connection string is used to connect to the mysql service.
    /// There are url based formats.
    /// The format of connect string resembles the url format of the mysql client.
    /// The format is:
    /// `[scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...`
    /// - `mysql://user@localhost` - `mysql://user:password@localhost` -
    /// `mysql://user:password@localhost:3306` -
    /// `mysql://user:password@localhost:3306/db` For more information, please refer
    /// to <https://docs.rs/sqlx/latest/sqlx/mysql/struct.MySqlConnectOptions.html>.
    #[getter]
    fn connection_string(&self) -> Option<String> {
        self.0.connection_string.clone()
    }
    /// The key field name for mysql.
    #[getter]
    fn key_field(&self) -> Option<String> {
        self.0.key_field.clone()
    }
    /// The root for mysql.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// The table name for mysql.
    #[getter]
    fn table(&self) -> Option<String> {
        self.0.table.clone()
    }
    /// The value field name for mysql.
    #[getter]
    fn value_field(&self) -> Option<String> {
        self.0.value_field.clone()
    }
}

#[cfg(feature = "services-obs")]
/// Configuration for the `obs` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct ObsConfig(ocore::services::ObsConfig);

#[cfg(feature = "services-obs")]
#[allow(deprecated)]
impl ConfigBuilder for ObsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "obs"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.access_key_id {
            map.insert("access_key_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.bucket {
            map.insert("bucket".to_string(), v.clone());
        }
        map.insert(
            "enable_versioning".to_string(),
            if self.0.enable_versioning {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.secret_access_key {
            map.insert("secret_access_key".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-obs")]
#[allow(deprecated)]
#[pymethods]
impl ObsConfig {
    #[new]
    #[pyo3(signature = (
        access_key_id = None,
        bucket = None,
        enable_versioning = None,
        endpoint = None,
        root = None,
        secret_access_key = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        access_key_id: Option<String>,
        bucket: Option<String>,
        enable_versioning: Option<bool>,
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
        secret_access_key: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = access_key_id {
            opts.insert("access_key_id".to_string(), v);
        }
        if let Some(v) = bucket {
            opts.insert("bucket".to_string(), v);
        }
        if let Some(v) = enable_versioning {
            opts.insert(
                "enable_versioning".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = secret_access_key {
            opts.insert("secret_access_key".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::ObsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Access key id for obs.
    #[getter]
    fn access_key_id(&self) -> Option<String> {
        self.0.access_key_id.clone()
    }
    /// Bucket for obs.
    #[getter]
    fn bucket(&self) -> Option<String> {
        self.0.bucket.clone()
    }
    /// Deprecated: OBS versioning capability is not controlled by service config.
    /// [Deprecated since 0.57.0] OBS versioning capability is not controlled by
    /// this option and this option is no longer needed.
    #[getter]
    fn enable_versioning(&self) -> Option<bool> {
        Some(self.0.enable_versioning)
    }
    /// Endpoint for obs.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// Root for obs.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Secret access key for obs.
    #[getter]
    fn secret_access_key(&self) -> Option<String> {
        self.0.secret_access_key.clone()
    }
}

#[cfg(feature = "services-onedrive")]
/// Configuration for the `onedrive` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct OnedriveConfig(ocore::services::OnedriveConfig);

#[cfg(feature = "services-onedrive")]
#[allow(deprecated)]
impl ConfigBuilder for OnedriveConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "onedrive"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.access_token {
            map.insert("access_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_id {
            map.insert("client_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.client_secret {
            map.insert("client_secret".to_string(), v.clone());
        }
        map.insert(
            "enable_versioning".to_string(),
            if self.0.enable_versioning {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.refresh_token {
            map.insert("refresh_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-onedrive")]
#[allow(deprecated)]
#[pymethods]
impl OnedriveConfig {
    #[new]
    #[pyo3(signature = (
        access_token = None,
        client_id = None,
        client_secret = None,
        enable_versioning = None,
        refresh_token = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        access_token: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        enable_versioning: Option<bool>,
        refresh_token: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = access_token {
            opts.insert("access_token".to_string(), v);
        }
        if let Some(v) = client_id {
            opts.insert("client_id".to_string(), v);
        }
        if let Some(v) = client_secret {
            opts.insert("client_secret".to_string(), v);
        }
        if let Some(v) = enable_versioning {
            opts.insert(
                "enable_versioning".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = refresh_token {
            opts.insert("refresh_token".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::OnedriveConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Microsoft Graph API (also OneDrive API) access token
    #[getter]
    fn access_token(&self) -> Option<String> {
        self.0.access_token.clone()
    }
    /// Microsoft Graph API Application (client) ID that is in the Azure's app
    /// registration portal
    #[getter]
    fn client_id(&self) -> Option<String> {
        self.0.client_id.clone()
    }
    /// Microsoft Graph API Application client secret that is in the Azure's app
    /// registration portal
    #[getter]
    fn client_secret(&self) -> Option<String> {
        self.0.client_secret.clone()
    }
    /// Deprecated: OneDrive versioning capability is enabled by default.
    /// [Deprecated since 0.57.0] OneDrive versioning capability is enabled by
    /// default and this option is no longer needed.
    #[getter]
    fn enable_versioning(&self) -> Option<bool> {
        Some(self.0.enable_versioning)
    }
    /// Microsoft Graph API (also OneDrive API) refresh token
    #[getter]
    fn refresh_token(&self) -> Option<String> {
        self.0.refresh_token.clone()
    }
    /// The root path for the OneDrive service for the file access
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-oss")]
/// Configuration for the `oss` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct OssConfig(ocore::services::OssConfig);

#[cfg(feature = "services-oss")]
#[allow(deprecated)]
impl ConfigBuilder for OssConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "oss"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("bucket".to_string(), self.0.bucket.clone());
        if let Some(v) = &self.0.access_key_id {
            map.insert("access_key_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.access_key_secret {
            map.insert("access_key_secret".to_string(), v.clone());
        }
        if let Some(v) = &self.0.addressing_style {
            map.insert("addressing_style".to_string(), v.clone());
        }
        map.insert(
            "allow_anonymous".to_string(),
            if self.0.allow_anonymous {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.batch_max_operations {
            map.insert("batch_max_operations".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.delete_max_size {
            map.insert("delete_max_size".to_string(), v.to_string());
        }
        map.insert(
            "enable_versioning".to_string(),
            if self.0.enable_versioning {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.external_id {
            map.insert("external_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.oidc_provider_arn {
            map.insert("oidc_provider_arn".to_string(), v.clone());
        }
        if let Some(v) = &self.0.oidc_token_file {
            map.insert("oidc_token_file".to_string(), v.clone());
        }
        if let Some(v) = &self.0.presign_addressing_style {
            map.insert("presign_addressing_style".to_string(), v.clone());
        }
        if let Some(v) = &self.0.presign_endpoint {
            map.insert("presign_endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.role_arn {
            map.insert("role_arn".to_string(), v.clone());
        }
        if let Some(v) = &self.0.role_session_name {
            map.insert("role_session_name".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.security_token {
            map.insert("security_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.server_side_encryption {
            map.insert("server_side_encryption".to_string(), v.clone());
        }
        if let Some(v) = &self.0.server_side_encryption_key_id {
            map.insert("server_side_encryption_key_id".to_string(), v.clone());
        }
        map.insert(
            "skip_signature".to_string(),
            if self.0.skip_signature {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.sts_endpoint {
            map.insert("sts_endpoint".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-oss")]
#[allow(deprecated)]
#[pymethods]
impl OssConfig {
    #[new]
    #[pyo3(signature = (
        bucket,
        access_key_id = None,
        access_key_secret = None,
        addressing_style = None,
        allow_anonymous = None,
        batch_max_operations = None,
        delete_max_size = None,
        enable_versioning = None,
        endpoint = None,
        external_id = None,
        oidc_provider_arn = None,
        oidc_token_file = None,
        presign_addressing_style = None,
        presign_endpoint = None,
        role_arn = None,
        role_session_name = None,
        root = None,
        security_token = None,
        server_side_encryption = None,
        server_side_encryption_key_id = None,
        skip_signature = None,
        sts_endpoint = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        access_key_id: Option<String>,
        access_key_secret: Option<String>,
        addressing_style: Option<String>,
        allow_anonymous: Option<bool>,
        batch_max_operations: Option<usize>,
        delete_max_size: Option<usize>,
        enable_versioning: Option<bool>,
        endpoint: Option<String>,
        external_id: Option<String>,
        oidc_provider_arn: Option<String>,
        oidc_token_file: Option<String>,
        presign_addressing_style: Option<String>,
        presign_endpoint: Option<String>,
        role_arn: Option<String>,
        role_session_name: Option<String>,
        root: Option<crate::PyPath>,
        security_token: Option<String>,
        server_side_encryption: Option<String>,
        server_side_encryption_key_id: Option<String>,
        skip_signature: Option<bool>,
        sts_endpoint: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("bucket".to_string(), bucket);
        if let Some(v) = access_key_id {
            opts.insert("access_key_id".to_string(), v);
        }
        if let Some(v) = access_key_secret {
            opts.insert("access_key_secret".to_string(), v);
        }
        if let Some(v) = addressing_style {
            opts.insert("addressing_style".to_string(), v);
        }
        if let Some(v) = allow_anonymous {
            opts.insert(
                "allow_anonymous".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = batch_max_operations {
            opts.insert("batch_max_operations".to_string(), v.to_string());
        }
        if let Some(v) = delete_max_size {
            opts.insert("delete_max_size".to_string(), v.to_string());
        }
        if let Some(v) = enable_versioning {
            opts.insert(
                "enable_versioning".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = external_id {
            opts.insert("external_id".to_string(), v);
        }
        if let Some(v) = oidc_provider_arn {
            opts.insert("oidc_provider_arn".to_string(), v);
        }
        if let Some(v) = oidc_token_file {
            opts.insert("oidc_token_file".to_string(), v);
        }
        if let Some(v) = presign_addressing_style {
            opts.insert("presign_addressing_style".to_string(), v);
        }
        if let Some(v) = presign_endpoint {
            opts.insert("presign_endpoint".to_string(), v);
        }
        if let Some(v) = role_arn {
            opts.insert("role_arn".to_string(), v);
        }
        if let Some(v) = role_session_name {
            opts.insert("role_session_name".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = security_token {
            opts.insert("security_token".to_string(), v);
        }
        if let Some(v) = server_side_encryption {
            opts.insert("server_side_encryption".to_string(), v);
        }
        if let Some(v) = server_side_encryption_key_id {
            opts.insert("server_side_encryption_key_id".to_string(), v);
        }
        if let Some(v) = skip_signature {
            opts.insert(
                "skip_signature".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = sts_endpoint {
            opts.insert("sts_endpoint".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::OssConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Bucket for oss.
    #[getter]
    fn bucket(&self) -> String {
        self.0.bucket.clone()
    }
    /// Access key id for oss.
    /// - this field if it's `is_some` - env value: `ALIBABA_CLOUD_ACCESS_KEY_ID`
    #[getter]
    fn access_key_id(&self) -> Option<String> {
        self.0.access_key_id.clone()
    }
    /// Access key secret for oss.
    /// - this field if it's `is_some` - env value:
    /// `ALIBABA_CLOUD_ACCESS_KEY_SECRET`
    #[getter]
    fn access_key_secret(&self) -> Option<String> {
        self.0.access_key_secret.clone()
    }
    /// Addressing style for oss.
    #[getter]
    fn addressing_style(&self) -> Option<String> {
        self.0.addressing_style.clone()
    }
    /// Allow anonymous for oss.
    /// [Deprecated since 0.57.0] Please use `skip_signature` instead of
    /// `allow_anonymous`
    #[getter]
    fn allow_anonymous(&self) -> Option<bool> {
        Some(self.0.allow_anonymous)
    }
    /// Deprecated: OSS delete batch capability is enabled by default.
    /// [Deprecated since 0.57.0] OSS delete batch capability is enabled by default.
    /// Use CapabilityOverrideLayer to override delete_max_size for specific
    /// endpoints.
    #[getter]
    fn batch_max_operations(&self) -> Option<usize> {
        self.0.batch_max_operations
    }
    /// Deprecated: OSS delete batch capability is enabled by default.
    /// [Deprecated since 0.57.0] OSS delete batch capability is enabled by default.
    /// Use CapabilityOverrideLayer to override delete_max_size for specific
    /// endpoints.
    #[getter]
    fn delete_max_size(&self) -> Option<usize> {
        self.0.delete_max_size
    }
    /// Deprecated: OSS versioning capability is enabled by default.
    /// [Deprecated since 0.57.0] OSS versioning capability is enabled by default
    /// and this option is no longer needed.
    #[getter]
    fn enable_versioning(&self) -> Option<bool> {
        Some(self.0.enable_versioning)
    }
    /// Endpoint for oss.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// external_id for this backend.
    #[getter]
    fn external_id(&self) -> Option<String> {
        self.0.external_id.clone()
    }
    /// `oidc_provider_arn` will be loaded from - this field if it's `is_some` - env
    /// value: `ALIBABA_CLOUD_OIDC_PROVIDER_ARN`
    #[getter]
    fn oidc_provider_arn(&self) -> Option<String> {
        self.0.oidc_provider_arn.clone()
    }
    /// `oidc_token_file` will be loaded from - this field if it's `is_some` - env
    /// value: `ALIBABA_CLOUD_OIDC_TOKEN_FILE`
    #[getter]
    fn oidc_token_file(&self) -> Option<String> {
        self.0.oidc_token_file.clone()
    }
    /// Pre sign addressing style for oss.
    #[getter]
    fn presign_addressing_style(&self) -> Option<String> {
        self.0.presign_addressing_style.clone()
    }
    /// Presign endpoint for oss.
    #[getter]
    fn presign_endpoint(&self) -> Option<String> {
        self.0.presign_endpoint.clone()
    }
    /// If `role_arn` is set, we will use already known config as source credential
    /// to assume role with `role_arn`.
    /// - this field if it's `is_some` - env value: `ALIBABA_CLOUD_ROLE_ARN`
    #[getter]
    fn role_arn(&self) -> Option<String> {
        self.0.role_arn.clone()
    }
    /// role_session_name for this backend.
    #[getter]
    fn role_session_name(&self) -> Option<String> {
        self.0.role_session_name.clone()
    }
    /// Root for oss.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// `security_token` will be loaded from - this field if it's `is_some` - env
    /// value: `ALIBABA_CLOUD_SECURITY_TOKEN`
    #[getter]
    fn security_token(&self) -> Option<String> {
        self.0.security_token.clone()
    }
    /// Server side encryption for oss.
    #[getter]
    fn server_side_encryption(&self) -> Option<String> {
        self.0.server_side_encryption.clone()
    }
    /// Server side encryption key id for oss.
    #[getter]
    fn server_side_encryption_key_id(&self) -> Option<String> {
        self.0.server_side_encryption_key_id.clone()
    }
    /// Skip signature will skip loading credentials and signing requests.
    #[getter]
    fn skip_signature(&self) -> Option<bool> {
        Some(self.0.skip_signature)
    }
    /// `sts_endpoint` will be loaded from - this field if it's `is_some` - env
    /// value: `ALIBABA_CLOUD_STS_ENDPOINT`
    #[getter]
    fn sts_endpoint(&self) -> Option<String> {
        self.0.sts_endpoint.clone()
    }
}

#[cfg(feature = "services-persy")]
/// Configuration for the `persy` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PersyConfig(ocore::services::PersyConfig);

#[cfg(feature = "services-persy")]
#[allow(deprecated)]
impl ConfigBuilder for PersyConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "persy"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.datafile {
            map.insert("datafile".to_string(), v.clone());
        }
        if let Some(v) = &self.0.index {
            map.insert("index".to_string(), v.clone());
        }
        if let Some(v) = &self.0.segment {
            map.insert("segment".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-persy")]
#[allow(deprecated)]
#[pymethods]
impl PersyConfig {
    #[new]
    #[pyo3(signature = (
        datafile = None,
        index = None,
        segment = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        datafile: Option<String>,
        index: Option<String>,
        segment: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = datafile {
            opts.insert("datafile".to_string(), v);
        }
        if let Some(v) = index {
            opts.insert("index".to_string(), v);
        }
        if let Some(v) = segment {
            opts.insert("segment".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::PersyConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// That path to the persy data file.
    /// The directory in the path must already exist.
    #[getter]
    fn datafile(&self) -> Option<String> {
        self.0.datafile.clone()
    }
    /// That name of the persy index.
    #[getter]
    fn index(&self) -> Option<String> {
        self.0.index.clone()
    }
    /// That name of the persy segment.
    #[getter]
    fn segment(&self) -> Option<String> {
        self.0.segment.clone()
    }
}

#[cfg(feature = "services-postgresql")]
/// Configuration for the `postgresql` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PostgresqlConfig(ocore::services::PostgresqlConfig);

#[cfg(feature = "services-postgresql")]
#[allow(deprecated)]
impl ConfigBuilder for PostgresqlConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "postgresql"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.connection_string {
            map.insert("connection_string".to_string(), v.clone());
        }
        if let Some(v) = &self.0.key_field {
            map.insert("key_field".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.table {
            map.insert("table".to_string(), v.clone());
        }
        if let Some(v) = &self.0.value_field {
            map.insert("value_field".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-postgresql")]
#[allow(deprecated)]
#[pymethods]
impl PostgresqlConfig {
    #[new]
    #[pyo3(signature = (
        connection_string = None,
        key_field = None,
        root = None,
        table = None,
        value_field = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        connection_string: Option<String>,
        key_field: Option<String>,
        root: Option<crate::PyPath>,
        table: Option<String>,
        value_field: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = connection_string {
            opts.insert("connection_string".to_string(), v);
        }
        if let Some(v) = key_field {
            opts.insert("key_field".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = table {
            opts.insert("table".to_string(), v);
        }
        if let Some(v) = value_field {
            opts.insert("value_field".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::PostgresqlConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The URL should be with a scheme of either `postgres://` or `postgresql://`.
    /// - `postgresql://user@localhost` -
    /// `postgresql://user:password@%2Fvar%2Flib%2Fpostgresql/mydb?connect_timeout=10`
    /// -
    /// `postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write`
    /// - `postgresql:///mydb?user=user&host=/var/lib/postgresql` For more
    /// information, please visit
    /// <https://docs.rs/sqlx/latest/sqlx/postgres/struct.PgConnectOptions.html>.
    #[getter]
    fn connection_string(&self) -> Option<String> {
        self.0.connection_string.clone()
    }
    /// the key field of postgresql
    #[getter]
    fn key_field(&self) -> Option<String> {
        self.0.key_field.clone()
    }
    /// Root of this backend.
    /// All operations will happen under this root.
    /// Default to `/` if not set.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// the table of postgresql
    #[getter]
    fn table(&self) -> Option<String> {
        self.0.table.clone()
    }
    /// the value field of postgresql
    #[getter]
    fn value_field(&self) -> Option<String> {
        self.0.value_field.clone()
    }
}

#[cfg(feature = "services-redb")]
/// Configuration for the `redb` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct RedbConfig(ocore::services::RedbConfig);

#[cfg(feature = "services-redb")]
#[allow(deprecated)]
impl ConfigBuilder for RedbConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "redb"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.datadir {
            map.insert("datadir".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.table {
            map.insert("table".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-redb")]
#[allow(deprecated)]
#[pymethods]
impl RedbConfig {
    #[new]
    #[pyo3(signature = (
        datadir = None,
        root = None,
        table = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        datadir: Option<String>,
        root: Option<crate::PyPath>,
        table: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = datadir {
            opts.insert("datadir".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = table {
            opts.insert("table".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::RedbConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// path to the redb data directory.
    #[getter]
    fn datadir(&self) -> Option<String> {
        self.0.datadir.clone()
    }
    /// The root for redb.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// The table name for redb.
    #[getter]
    fn table(&self) -> Option<String> {
        self.0.table.clone()
    }
}

#[cfg(feature = "services-redis")]
/// Configuration for the `redis` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct RedisConfig(ocore::services::RedisConfig);

#[cfg(feature = "services-redis")]
#[allow(deprecated)]
impl ConfigBuilder for RedisConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "redis"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("db".to_string(), self.0.db.to_string());
        if let Some(v) = &self.0.cluster_endpoints {
            map.insert("cluster_endpoints".to_string(), v.clone());
        }
        if let Some(v) = &self.0.connection_pool_max_size {
            map.insert("connection_pool_max_size".to_string(), v.to_string());
        }
        if let Some(d) = &self.0.default_ttl {
            map.insert("default_ttl".to_string(), format!("{}s", d.as_secs()));
        }
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.password {
            map.insert("password".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.username {
            map.insert("username".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-redis")]
#[allow(deprecated)]
#[pymethods]
impl RedisConfig {
    #[new]
    #[pyo3(signature = (
        db,
        cluster_endpoints = None,
        connection_pool_max_size = None,
        default_ttl = None,
        endpoint = None,
        password = None,
        root = None,
        username = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        db: Option<i64>,
        cluster_endpoints: Option<String>,
        connection_pool_max_size: Option<usize>,
        default_ttl: Option<String>,
        endpoint: Option<String>,
        password: Option<String>,
        root: Option<crate::PyPath>,
        username: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = db {
            opts.insert("db".to_string(), v.to_string());
        }
        if let Some(v) = cluster_endpoints {
            opts.insert("cluster_endpoints".to_string(), v);
        }
        if let Some(v) = connection_pool_max_size {
            opts.insert("connection_pool_max_size".to_string(), v.to_string());
        }
        if let Some(v) = default_ttl {
            opts.insert("default_ttl".to_string(), v);
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = password {
            opts.insert("password".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = username {
            opts.insert("username".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::RedisConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// the number of DBs redis can take is unlimited default is db 0
    #[getter]
    fn db(&self) -> i64 {
        self.0.db
    }
    /// network address of the Redis cluster service.
    /// Can be "tcp://127.0.0.1:6379,tcp://127.0.0.1:6380,tcp://127.0.0.1:6381",
    /// e.g.
    /// default is None
    #[getter]
    fn cluster_endpoints(&self) -> Option<String> {
        self.0.cluster_endpoints.clone()
    }
    /// The maximum number of connections allowed.
    /// default is 10
    #[getter]
    fn connection_pool_max_size(&self) -> Option<usize> {
        self.0.connection_pool_max_size
    }
    /// The default ttl for put operations.
    /// Accepts a humantime duration string (e.g.
    /// "5s").
    #[getter]
    fn default_ttl(&self) -> Option<String> {
        self.0.default_ttl.map(|d| format!("{}s", d.as_secs()))
    }
    /// network address of the Redis service.
    /// Can be "tcp://127.0.0.1:6379", e.g.
    /// default is "tcp://127.0.0.1:6379"
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// the password for authentication default is None
    #[getter]
    fn password(&self) -> Option<String> {
        self.0.password.clone()
    }
    /// the working directory of the Redis service.
    /// Can be "/path/to/dir" default is "/"
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// the username to connect redis service.
    /// default is None
    #[getter]
    fn username(&self) -> Option<String> {
        self.0.username.clone()
    }
}

#[cfg(feature = "services-s3")]
/// Configuration for the `s3` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct S3Config(ocore::services::S3Config);

#[cfg(feature = "services-s3")]
#[allow(deprecated)]
impl ConfigBuilder for S3Config {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "s3"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("bucket".to_string(), self.0.bucket.clone());
        if let Some(v) = &self.0.access_key_id {
            map.insert("access_key_id".to_string(), v.clone());
        }
        map.insert(
            "allow_anonymous".to_string(),
            if self.0.allow_anonymous {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.assume_role_duration_seconds {
            map.insert("assume_role_duration_seconds".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.batch_max_operations {
            map.insert("batch_max_operations".to_string(), v.to_string());
        }
        if let Some(v) = &self.0.checksum_algorithm {
            map.insert("checksum_algorithm".to_string(), v.clone());
        }
        if let Some(v) = &self.0.default_acl {
            map.insert("default_acl".to_string(), v.clone());
        }
        if let Some(v) = &self.0.default_storage_class {
            map.insert("default_storage_class".to_string(), v.clone());
        }
        if let Some(v) = &self.0.delete_max_size {
            map.insert("delete_max_size".to_string(), v.to_string());
        }
        map.insert(
            "disable_config_load".to_string(),
            if self.0.disable_config_load {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "disable_ec2_metadata".to_string(),
            if self.0.disable_ec2_metadata {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "disable_list_objects_v2".to_string(),
            if self.0.disable_list_objects_v2 {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "disable_stat_with_override".to_string(),
            if self.0.disable_stat_with_override {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "disable_write_with_if_match".to_string(),
            if self.0.disable_write_with_if_match {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "enable_request_payer".to_string(),
            if self.0.enable_request_payer {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "enable_versioning".to_string(),
            if self.0.enable_versioning {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "enable_virtual_host_style".to_string(),
            if self.0.enable_virtual_host_style {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "enable_write_with_append".to_string(),
            if self.0.enable_write_with_append {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.external_id {
            map.insert("external_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.region {
            map.insert("region".to_string(), v.clone());
        }
        if let Some(v) = &self.0.role_arn {
            map.insert("role_arn".to_string(), v.clone());
        }
        if let Some(v) = &self.0.role_session_name {
            map.insert("role_session_name".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.secret_access_key {
            map.insert("secret_access_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.server_side_encryption {
            map.insert("server_side_encryption".to_string(), v.clone());
        }
        if let Some(v) = &self.0.server_side_encryption_aws_kms_key_id {
            map.insert(
                "server_side_encryption_aws_kms_key_id".to_string(),
                v.clone(),
            );
        }
        if let Some(v) = &self.0.server_side_encryption_customer_algorithm {
            map.insert(
                "server_side_encryption_customer_algorithm".to_string(),
                v.clone(),
            );
        }
        if let Some(v) = &self.0.server_side_encryption_customer_key {
            map.insert("server_side_encryption_customer_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.server_side_encryption_customer_key_md5 {
            map.insert(
                "server_side_encryption_customer_key_md5".to_string(),
                v.clone(),
            );
        }
        if let Some(v) = &self.0.session_token {
            map.insert("session_token".to_string(), v.clone());
        }
        map.insert(
            "skip_signature".to_string(),
            if self.0.skip_signature {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        if self.0.assume_role_session_tags.is_some() {
            ok = false;
        }
        ok
    }
}

#[cfg(feature = "services-s3")]
#[allow(deprecated)]
#[pymethods]
impl S3Config {
    #[new]
    #[pyo3(signature = (
        bucket,
        access_key_id = None,
        allow_anonymous = None,
        assume_role_duration_seconds = None,
        assume_role_session_tags = None,
        batch_max_operations = None,
        checksum_algorithm = None,
        default_acl = None,
        default_storage_class = None,
        delete_max_size = None,
        disable_config_load = None,
        disable_ec2_metadata = None,
        disable_list_objects_v2 = None,
        disable_stat_with_override = None,
        disable_write_with_if_match = None,
        enable_request_payer = None,
        enable_versioning = None,
        enable_virtual_host_style = None,
        enable_write_with_append = None,
        endpoint = None,
        external_id = None,
        region = None,
        role_arn = None,
        role_session_name = None,
        root = None,
        secret_access_key = None,
        server_side_encryption = None,
        server_side_encryption_aws_kms_key_id = None,
        server_side_encryption_customer_algorithm = None,
        server_side_encryption_customer_key = None,
        server_side_encryption_customer_key_md5 = None,
        session_token = None,
        skip_signature = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        access_key_id: Option<String>,
        allow_anonymous: Option<bool>,
        assume_role_duration_seconds: Option<u32>,
        assume_role_session_tags: Option<std::collections::HashMap<String, String>>,
        batch_max_operations: Option<usize>,
        checksum_algorithm: Option<String>,
        default_acl: Option<String>,
        default_storage_class: Option<String>,
        delete_max_size: Option<usize>,
        disable_config_load: Option<bool>,
        disable_ec2_metadata: Option<bool>,
        disable_list_objects_v2: Option<bool>,
        disable_stat_with_override: Option<bool>,
        disable_write_with_if_match: Option<bool>,
        enable_request_payer: Option<bool>,
        enable_versioning: Option<bool>,
        enable_virtual_host_style: Option<bool>,
        enable_write_with_append: Option<bool>,
        endpoint: Option<String>,
        external_id: Option<String>,
        region: Option<String>,
        role_arn: Option<String>,
        role_session_name: Option<String>,
        root: Option<crate::PyPath>,
        secret_access_key: Option<String>,
        server_side_encryption: Option<String>,
        server_side_encryption_aws_kms_key_id: Option<String>,
        server_side_encryption_customer_algorithm: Option<String>,
        server_side_encryption_customer_key: Option<String>,
        server_side_encryption_customer_key_md5: Option<String>,
        session_token: Option<String>,
        skip_signature: Option<bool>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("bucket".to_string(), bucket);
        if let Some(v) = access_key_id {
            opts.insert("access_key_id".to_string(), v);
        }
        if let Some(v) = allow_anonymous {
            opts.insert(
                "allow_anonymous".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = assume_role_duration_seconds {
            opts.insert("assume_role_duration_seconds".to_string(), v.to_string());
        }
        if let Some(v) = batch_max_operations {
            opts.insert("batch_max_operations".to_string(), v.to_string());
        }
        if let Some(v) = checksum_algorithm {
            opts.insert("checksum_algorithm".to_string(), v);
        }
        if let Some(v) = default_acl {
            opts.insert("default_acl".to_string(), v);
        }
        if let Some(v) = default_storage_class {
            opts.insert("default_storage_class".to_string(), v);
        }
        if let Some(v) = delete_max_size {
            opts.insert("delete_max_size".to_string(), v.to_string());
        }
        if let Some(v) = disable_config_load {
            opts.insert(
                "disable_config_load".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = disable_ec2_metadata {
            opts.insert(
                "disable_ec2_metadata".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = disable_list_objects_v2 {
            opts.insert(
                "disable_list_objects_v2".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = disable_stat_with_override {
            opts.insert(
                "disable_stat_with_override".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = disable_write_with_if_match {
            opts.insert(
                "disable_write_with_if_match".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = enable_request_payer {
            opts.insert(
                "enable_request_payer".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = enable_versioning {
            opts.insert(
                "enable_versioning".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = enable_virtual_host_style {
            opts.insert(
                "enable_virtual_host_style".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = enable_write_with_append {
            opts.insert(
                "enable_write_with_append".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = external_id {
            opts.insert("external_id".to_string(), v);
        }
        if let Some(v) = region {
            opts.insert("region".to_string(), v);
        }
        if let Some(v) = role_arn {
            opts.insert("role_arn".to_string(), v);
        }
        if let Some(v) = role_session_name {
            opts.insert("role_session_name".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = secret_access_key {
            opts.insert("secret_access_key".to_string(), v);
        }
        if let Some(v) = server_side_encryption {
            opts.insert("server_side_encryption".to_string(), v);
        }
        if let Some(v) = server_side_encryption_aws_kms_key_id {
            opts.insert("server_side_encryption_aws_kms_key_id".to_string(), v);
        }
        if let Some(v) = server_side_encryption_customer_algorithm {
            opts.insert("server_side_encryption_customer_algorithm".to_string(), v);
        }
        if let Some(v) = server_side_encryption_customer_key {
            opts.insert("server_side_encryption_customer_key".to_string(), v);
        }
        if let Some(v) = server_side_encryption_customer_key_md5 {
            opts.insert("server_side_encryption_customer_key_md5".to_string(), v);
        }
        if let Some(v) = session_token {
            opts.insert("session_token".to_string(), v);
        }
        if let Some(v) = skip_signature {
            opts.insert(
                "skip_signature".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::S3Config as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        if let Some(v) = assume_role_session_tags {
            cfg.assume_role_session_tags = Some(v);
        }
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// bucket name of this backend.
    /// required.
    #[getter]
    fn bucket(&self) -> String {
        self.0.bucket.clone()
    }
    /// access_key_id of this backend.
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[getter]
    fn access_key_id(&self) -> Option<String> {
        self.0.access_key_id.clone()
    }
    /// Allow anonymous will allow opendal to send request without signing when
    /// credential is not loaded.
    /// [Deprecated since 0.57.0] Please use `skip_signature` instead of
    /// `allow_anonymous`
    #[getter]
    fn allow_anonymous(&self) -> Option<bool> {
        Some(self.0.allow_anonymous)
    }
    /// assume_role_duration_seconds for this backend.
    #[getter]
    fn assume_role_duration_seconds(&self) -> Option<u32> {
        self.0.assume_role_duration_seconds
    }
    /// assume_role_session_tags for this backend.
    #[getter]
    fn assume_role_session_tags(&self) -> Option<std::collections::HashMap<String, String>> {
        self.0.assume_role_session_tags.clone()
    }
    /// Deprecated: S3 delete batch capability is enabled by default.
    /// [Deprecated since 0.57.0] S3 delete batch capability is enabled by default.
    /// Use CapabilityOverrideLayer to override delete_max_size for specific
    /// endpoints.
    #[getter]
    fn batch_max_operations(&self) -> Option<usize> {
        self.0.batch_max_operations
    }
    /// Checksum Algorithm to use when sending checksums in HTTP headers.
    /// This is necessary when writing to AWS S3 Buckets with Object Lock enabled
    /// for example.
    /// Available options: - "crc32c" - "md5"
    #[getter]
    fn checksum_algorithm(&self) -> Option<String> {
        self.0.checksum_algorithm.clone()
    }
    /// Default ACL for new objects.
    /// Note that some s3 services like minio do not support this option.
    #[getter]
    fn default_acl(&self) -> Option<String> {
        self.0.default_acl.clone()
    }
    /// default storage_class for this backend.
    /// Available values: - `DEEP_ARCHIVE` - `GLACIER` - `GLACIER_IR` -
    /// `INTELLIGENT_TIERING` - `ONEZONE_IA` - `EXPRESS_ONEZONE` - `OUTPOSTS` -
    /// `REDUCED_REDUNDANCY` - `STANDARD` - `STANDARD_IA` S3 compatible services
    /// don't support all of them
    #[getter]
    fn default_storage_class(&self) -> Option<String> {
        self.0.default_storage_class.clone()
    }
    /// Deprecated: S3 delete batch capability is enabled by default.
    /// [Deprecated since 0.57.0] S3 delete batch capability is enabled by default.
    /// Use CapabilityOverrideLayer to override delete_max_size for specific
    /// endpoints.
    #[getter]
    fn delete_max_size(&self) -> Option<usize> {
        self.0.delete_max_size
    }
    /// Disable config load so that opendal will not load config from environment.
    /// For examples: - envs like `AWS_ACCESS_KEY_ID` - files like `~/.aws/config`
    #[getter]
    fn disable_config_load(&self) -> Option<bool> {
        Some(self.0.disable_config_load)
    }
    /// Disable load credential from ec2 metadata.
    /// This option is used to disable the default behavior of opendal to load
    /// credential from ec2 metadata, a.k.a., IMDSv2
    #[getter]
    fn disable_ec2_metadata(&self) -> Option<bool> {
        Some(self.0.disable_ec2_metadata)
    }
    /// OpenDAL uses List Objects V2 by default to list objects.
    /// However, some legacy services do not yet support V2.
    /// This option allows users to switch back to the older List Objects V1.
    #[getter]
    fn disable_list_objects_v2(&self) -> Option<bool> {
        Some(self.0.disable_list_objects_v2)
    }
    /// Deprecated: S3 stat override capabilities are enabled by default.
    /// [Deprecated since 0.57.0] S3 stat override capabilities are enabled by
    /// default.
    /// Use CapabilityOverrideLayer to override them for specific endpoints.
    #[getter]
    fn disable_stat_with_override(&self) -> Option<bool> {
        Some(self.0.disable_stat_with_override)
    }
    /// Deprecated: S3 write with If-Match capability is enabled by default.
    /// [Deprecated since 0.57.0] S3 write with If-Match capability is enabled by
    /// default and this option is no longer needed.
    #[getter]
    fn disable_write_with_if_match(&self) -> Option<bool> {
        Some(self.0.disable_write_with_if_match)
    }
    /// Indicates whether the client agrees to pay for the requests made to the S3
    /// bucket.
    #[getter]
    fn enable_request_payer(&self) -> Option<bool> {
        Some(self.0.enable_request_payer)
    }
    /// Deprecated: S3 versioning capability is enabled by default.
    /// [Deprecated since 0.57.0] S3 versioning capability is enabled by default and
    /// this option is no longer needed.
    #[getter]
    fn enable_versioning(&self) -> Option<bool> {
        Some(self.0.enable_versioning)
    }
    /// Enable virtual host style so that opendal will send API requests in virtual
    /// host style instead of path style.
    /// - By default, opendal will send API to
    /// `https://s3.us-east-1.amazonaws.com/bucket_name` - Enabled, opendal will
    /// send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
    #[getter]
    fn enable_virtual_host_style(&self) -> Option<bool> {
        Some(self.0.enable_virtual_host_style)
    }
    /// Deprecated: S3 append capability is enabled by default.
    /// [Deprecated since 0.57.0] S3 append capability is enabled by default and
    /// this option is no longer needed.
    #[getter]
    fn enable_write_with_append(&self) -> Option<bool> {
        Some(self.0.enable_write_with_append)
    }
    /// endpoint of this backend.
    /// Endpoint must be full uri, e.g.
    /// - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
    /// - Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` - Aliyun
    /// OSS: `https://{region}.aliyuncs.com` - Tencent COS:
    /// `https://cos.{region}.myqcloud.com` - Minio: `http://127.0.0.1:9000` If user
    /// inputs endpoint without scheme like "s3.amazonaws.com", we will prepend
    /// "https://" before it.
    /// - If endpoint is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - If still not set, default to `https://s3.amazonaws.com`.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// external_id for this backend.
    #[getter]
    fn external_id(&self) -> Option<String> {
        self.0.external_id.clone()
    }
    /// Region represent the signing region of this endpoint.
    /// This is required if you are using the default AWS S3 endpoint.
    /// If using a custom endpoint, - If region is set, we will take user's input
    /// first.
    /// - If not, we will try to load it from environment.
    #[getter]
    fn region(&self) -> Option<String> {
        self.0.region.clone()
    }
    /// role_arn for this backend.
    /// If `role_arn` is set, we will use already known config as source credential
    /// to assume role with `role_arn`.
    #[getter]
    fn role_arn(&self) -> Option<String> {
        self.0.role_arn.clone()
    }
    /// role_session_name for this backend.
    #[getter]
    fn role_session_name(&self) -> Option<String> {
        self.0.role_session_name.clone()
    }
    /// root of this backend.
    /// All operations will happen under this root.
    /// default to `/` if not set.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// secret_access_key of this backend.
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[getter]
    fn secret_access_key(&self) -> Option<String> {
        self.0.secret_access_key.clone()
    }
    /// server_side_encryption for this backend.
    /// Available values: `AES256`, `aws:kms`.
    #[getter]
    fn server_side_encryption(&self) -> Option<String> {
        self.0.server_side_encryption.clone()
    }
    /// server_side_encryption_aws_kms_key_id for this backend - If
    /// `server_side_encryption` set to `aws:kms`, and
    /// `server_side_encryption_aws_kms_key_id` is not set, S3 will use aws managed
    /// kms key to encrypt data.
    /// - If `server_side_encryption` set to `aws:kms`, and
    /// `server_side_encryption_aws_kms_key_id` is a valid kms key id, S3 will use
    /// the provided kms key to encrypt data.
    /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an
    /// error will be returned.
    /// - If `server_side_encryption` is not `aws:kms`, setting
    /// `server_side_encryption_aws_kms_key_id` is a noop.
    #[getter]
    fn server_side_encryption_aws_kms_key_id(&self) -> Option<String> {
        self.0.server_side_encryption_aws_kms_key_id.clone()
    }
    /// server_side_encryption_customer_algorithm for this backend.
    /// Available values: `AES256`.
    #[getter]
    fn server_side_encryption_customer_algorithm(&self) -> Option<String> {
        self.0.server_side_encryption_customer_algorithm.clone()
    }
    /// server_side_encryption_customer_key for this backend.
    /// Value: BASE64-encoded key that matches algorithm specified in
    /// `server_side_encryption_customer_algorithm`.
    #[getter]
    fn server_side_encryption_customer_key(&self) -> Option<String> {
        self.0.server_side_encryption_customer_key.clone()
    }
    /// Set server_side_encryption_customer_key_md5 for this backend.
    /// Value: MD5 digest of key specified in `server_side_encryption_customer_key`.
    #[getter]
    fn server_side_encryption_customer_key_md5(&self) -> Option<String> {
        self.0.server_side_encryption_customer_key_md5.clone()
    }
    /// session_token (aka, security token) of this backend.
    /// This token will expire after sometime, it's recommended to set session_token
    /// by hand.
    #[getter]
    fn session_token(&self) -> Option<String> {
        self.0.session_token.clone()
    }
    /// Skip signature will skip loading credentials and signing requests.
    #[getter]
    fn skip_signature(&self) -> Option<bool> {
        Some(self.0.skip_signature)
    }
}

#[cfg(feature = "services-seafile")]
/// Configuration for the `seafile` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct SeafileConfig(ocore::services::SeafileConfig);

#[cfg(feature = "services-seafile")]
#[allow(deprecated)]
impl ConfigBuilder for SeafileConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "seafile"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("repo_name".to_string(), self.0.repo_name.clone());
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.password {
            map.insert("password".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.username {
            map.insert("username".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-seafile")]
#[allow(deprecated)]
#[pymethods]
impl SeafileConfig {
    #[new]
    #[pyo3(signature = (
        repo_name,
        endpoint = None,
        password = None,
        root = None,
        username = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        repo_name: String,
        endpoint: Option<String>,
        password: Option<String>,
        root: Option<crate::PyPath>,
        username: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("repo_name".to_string(), repo_name);
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = password {
            opts.insert("password".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = username {
            opts.insert("username".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::SeafileConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// repo_name of this backend.
    /// required.
    #[getter]
    fn repo_name(&self) -> String {
        self.0.repo_name.clone()
    }
    /// endpoint address of this backend.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// password of this backend.
    #[getter]
    fn password(&self) -> Option<String> {
        self.0.password.clone()
    }
    /// root of this backend.
    /// All operations will happen under this root.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// username of this backend.
    #[getter]
    fn username(&self) -> Option<String> {
        self.0.username.clone()
    }
}

#[cfg(feature = "services-sftp")]
/// Configuration for the `sftp` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct SftpConfig(ocore::services::SftpConfig);

#[cfg(feature = "services-sftp")]
#[allow(deprecated)]
impl ConfigBuilder for SftpConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "sftp"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert(
            "enable_copy".to_string(),
            if self.0.enable_copy { "true" } else { "false" }.to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.key {
            map.insert("key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.known_hosts_strategy {
            map.insert("known_hosts_strategy".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.user {
            map.insert("user".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-sftp")]
#[allow(deprecated)]
#[pymethods]
impl SftpConfig {
    #[new]
    #[pyo3(signature = (
        enable_copy = None,
        endpoint = None,
        key = None,
        known_hosts_strategy = None,
        root = None,
        user = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        enable_copy: Option<bool>,
        endpoint: Option<String>,
        key: Option<String>,
        known_hosts_strategy: Option<String>,
        root: Option<crate::PyPath>,
        user: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = enable_copy {
            opts.insert(
                "enable_copy".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = key {
            opts.insert("key".to_string(), v);
        }
        if let Some(v) = known_hosts_strategy {
            opts.insert("known_hosts_strategy".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = user {
            opts.insert("user".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::SftpConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Deprecated: SFTP copy capability is enabled by default.
    /// [Deprecated since 0.57.0] SFTP copy capability is enabled by default and
    /// this option is no longer needed.
    #[getter]
    fn enable_copy(&self) -> Option<bool> {
        Some(self.0.enable_copy)
    }
    /// endpoint of this backend
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// key of this backend
    #[getter]
    fn key(&self) -> Option<String> {
        self.0.key.clone()
    }
    /// known_hosts_strategy of this backend
    #[getter]
    fn known_hosts_strategy(&self) -> Option<String> {
        self.0.known_hosts_strategy.clone()
    }
    /// root of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// user of this backend
    #[getter]
    fn user(&self) -> Option<String> {
        self.0.user.clone()
    }
}

#[cfg(feature = "services-sled")]
/// Configuration for the `sled` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct SledConfig(ocore::services::SledConfig);

#[cfg(feature = "services-sled")]
#[allow(deprecated)]
impl ConfigBuilder for SledConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "sled"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.datadir {
            map.insert("datadir".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.tree {
            map.insert("tree".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-sled")]
#[allow(deprecated)]
#[pymethods]
impl SledConfig {
    #[new]
    #[pyo3(signature = (
        datadir = None,
        root = None,
        tree = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        datadir: Option<String>,
        root: Option<crate::PyPath>,
        tree: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = datadir {
            opts.insert("datadir".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = tree {
            opts.insert("tree".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::SledConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// That path to the sled data directory.
    #[getter]
    fn datadir(&self) -> Option<String> {
        self.0.datadir.clone()
    }
    /// The root for sled.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// The tree for sled.
    #[getter]
    fn tree(&self) -> Option<String> {
        self.0.tree.clone()
    }
}

#[cfg(feature = "services-sqlite")]
/// Configuration for the `sqlite` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct SqliteConfig(ocore::services::SqliteConfig);

#[cfg(feature = "services-sqlite")]
#[allow(deprecated)]
impl ConfigBuilder for SqliteConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "sqlite"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.connection_string {
            map.insert("connection_string".to_string(), v.clone());
        }
        if let Some(v) = &self.0.key_field {
            map.insert("key_field".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.table {
            map.insert("table".to_string(), v.clone());
        }
        if let Some(v) = &self.0.value_field {
            map.insert("value_field".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-sqlite")]
#[allow(deprecated)]
#[pymethods]
impl SqliteConfig {
    #[new]
    #[pyo3(signature = (
        connection_string = None,
        key_field = None,
        root = None,
        table = None,
        value_field = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        connection_string: Option<String>,
        key_field: Option<String>,
        root: Option<crate::PyPath>,
        table: Option<String>,
        value_field: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = connection_string {
            opts.insert("connection_string".to_string(), v);
        }
        if let Some(v) = key_field {
            opts.insert("key_field".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = table {
            opts.insert("table".to_string(), v);
        }
        if let Some(v) = value_field {
            opts.insert("value_field".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::SqliteConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Set the connection_string of the sqlite service.
    /// This connection string is used to connect to the sqlite service.
    /// The format of connect string resembles the url format of the sqlite client:
    /// - `sqlite::memory:` - `sqlite:data.db` - `sqlite://data.db` For more
    /// information, please visit
    /// <https://docs.rs/sqlx/latest/sqlx/sqlite/struct.SqliteConnectOptions.html>.
    #[getter]
    fn connection_string(&self) -> Option<String> {
        self.0.connection_string.clone()
    }
    /// Set the key field name of the sqlite service to read/write.
    /// Default to `key` if not specified.
    #[getter]
    fn key_field(&self) -> Option<String> {
        self.0.key_field.clone()
    }
    /// set the working directory, all operations will be performed under it.
    /// default: "/"
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Set the table name of the sqlite service to read/write.
    #[getter]
    fn table(&self) -> Option<String> {
        self.0.table.clone()
    }
    /// Set the value field name of the sqlite service to read/write.
    /// Default to `value` if not specified.
    #[getter]
    fn value_field(&self) -> Option<String> {
        self.0.value_field.clone()
    }
}

#[cfg(feature = "services-swift")]
/// Configuration for the `swift` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct SwiftConfig(ocore::services::SwiftConfig);

#[cfg(feature = "services-swift")]
#[allow(deprecated)]
impl ConfigBuilder for SwiftConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "swift"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.container {
            map.insert("container".to_string(), v.clone());
        }
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.temp_url_hash_algorithm {
            map.insert("temp_url_hash_algorithm".to_string(), v.clone());
        }
        if let Some(v) = &self.0.temp_url_key {
            map.insert("temp_url_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.token {
            map.insert("token".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-swift")]
#[allow(deprecated)]
#[pymethods]
impl SwiftConfig {
    #[new]
    #[pyo3(signature = (
        container = None,
        endpoint = None,
        root = None,
        temp_url_hash_algorithm = None,
        temp_url_key = None,
        token = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        container: Option<String>,
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
        temp_url_hash_algorithm: Option<String>,
        temp_url_key: Option<String>,
        token: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = container {
            opts.insert("container".to_string(), v);
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = temp_url_hash_algorithm {
            opts.insert("temp_url_hash_algorithm".to_string(), v);
        }
        if let Some(v) = temp_url_key {
            opts.insert("temp_url_key".to_string(), v);
        }
        if let Some(v) = token {
            opts.insert("token".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::SwiftConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The container for Swift.
    #[getter]
    fn container(&self) -> Option<String> {
        self.0.container.clone()
    }
    /// The endpoint for Swift.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// The root for Swift.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// The hash algorithm for TempURL signing.
    /// Supported values: `sha1`, `sha256`, `sha512`.
    /// Defaults to `sha256`.
    /// The cluster must have the chosen algorithm in its `tempurl.allowed_digests`
    /// (check `GET /info`).
    #[getter]
    fn temp_url_hash_algorithm(&self) -> Option<String> {
        self.0.temp_url_hash_algorithm.clone()
    }
    /// The TempURL key for generating presigned URLs.
    /// This corresponds to the `X-Account-Meta-Temp-URL-Key` or
    /// `X-Container-Meta-Temp-URL-Key` header value configured on the Swift account
    /// or container.
    #[getter]
    fn temp_url_key(&self) -> Option<String> {
        self.0.temp_url_key.clone()
    }
    /// The token for Swift.
    #[getter]
    fn token(&self) -> Option<String> {
        self.0.token.clone()
    }
}

#[cfg(feature = "services-tos")]
/// Configuration for the `tos` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct TosConfig(ocore::services::TosConfig);

#[cfg(feature = "services-tos")]
#[allow(deprecated)]
impl ConfigBuilder for TosConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "tos"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("bucket".to_string(), self.0.bucket.clone());
        if let Some(v) = &self.0.access_key_id {
            map.insert("access_key_id".to_string(), v.clone());
        }
        map.insert(
            "disable_config_load".to_string(),
            if self.0.disable_config_load {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.region {
            map.insert("region".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.secret_access_key {
            map.insert("secret_access_key".to_string(), v.clone());
        }
        if let Some(v) = &self.0.security_token {
            map.insert("security_token".to_string(), v.clone());
        }
        map.insert(
            "skip_signature".to_string(),
            if self.0.skip_signature {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-tos")]
#[allow(deprecated)]
#[pymethods]
impl TosConfig {
    #[new]
    #[pyo3(signature = (
        bucket,
        access_key_id = None,
        disable_config_load = None,
        endpoint = None,
        region = None,
        root = None,
        secret_access_key = None,
        security_token = None,
        skip_signature = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        access_key_id: Option<String>,
        disable_config_load: Option<bool>,
        endpoint: Option<String>,
        region: Option<String>,
        root: Option<crate::PyPath>,
        secret_access_key: Option<String>,
        security_token: Option<String>,
        skip_signature: Option<bool>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("bucket".to_string(), bucket);
        if let Some(v) = access_key_id {
            opts.insert("access_key_id".to_string(), v);
        }
        if let Some(v) = disable_config_load {
            opts.insert(
                "disable_config_load".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = region {
            opts.insert("region".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = secret_access_key {
            opts.insert("secret_access_key".to_string(), v);
        }
        if let Some(v) = security_token {
            opts.insert("security_token".to_string(), v);
        }
        if let Some(v) = skip_signature {
            opts.insert(
                "skip_signature".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::TosConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// bucket name of this backend.
    /// required.
    #[getter]
    fn bucket(&self) -> String {
        self.0.bucket.clone()
    }
    /// access_key_id of this backend.
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[getter]
    fn access_key_id(&self) -> Option<String> {
        self.0.access_key_id.clone()
    }
    /// Disable config load so that opendal will not load config from environment.
    /// For examples: - envs like `TOS_ACCESS_KEY_ID`
    #[getter]
    fn disable_config_load(&self) -> Option<bool> {
        Some(self.0.disable_config_load)
    }
    /// endpoint of this backend.
    /// Endpoint must be full uri, e.g.
    /// - TOS: `https://tos-cn-beijing.volces.com` - TOS with region:
    /// `https://tos-{region}.volces.com` If user inputs endpoint without scheme
    /// like "tos-cn-beijing.volces.com", we will prepend "https://" before it.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// Region represent the signing region of this endpoint.
    /// Required if endpoint is not provided.
    /// - If region is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - If still not set, default to `cn-beijing`.
    #[getter]
    fn region(&self) -> Option<String> {
        self.0.region.clone()
    }
    /// root of this backend.
    /// All operations will happen under this root.
    /// default to `/` if not set.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// secret_access_key of this backend.
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[getter]
    fn secret_access_key(&self) -> Option<String> {
        self.0.secret_access_key.clone()
    }
    /// security_token of this backend.
    /// This token will expire after sometime, it's recommended to set
    /// security_token by hand.
    #[getter]
    fn security_token(&self) -> Option<String> {
        self.0.security_token.clone()
    }
    /// Skip signature will skip loading credentials and signing requests.
    #[getter]
    fn skip_signature(&self) -> Option<bool> {
        Some(self.0.skip_signature)
    }
}

#[cfg(feature = "services-upyun")]
/// Configuration for the `upyun` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct UpyunConfig(ocore::services::UpyunConfig);

#[cfg(feature = "services-upyun")]
#[allow(deprecated)]
impl ConfigBuilder for UpyunConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "upyun"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("bucket".to_string(), self.0.bucket.clone());
        if let Some(v) = &self.0.operator {
            map.insert("operator".to_string(), v.clone());
        }
        if let Some(v) = &self.0.password {
            map.insert("password".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-upyun")]
#[allow(deprecated)]
#[pymethods]
impl UpyunConfig {
    #[new]
    #[pyo3(signature = (
        bucket,
        operator = None,
        password = None,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        operator: Option<String>,
        password: Option<String>,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("bucket".to_string(), bucket);
        if let Some(v) = operator {
            opts.insert("operator".to_string(), v);
        }
        if let Some(v) = password {
            opts.insert("password".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::UpyunConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// bucket address of this backend.
    #[getter]
    fn bucket(&self) -> String {
        self.0.bucket.clone()
    }
    /// username of this backend.
    #[getter]
    fn operator(&self) -> Option<String> {
        self.0.operator.clone()
    }
    /// password of this backend.
    #[getter]
    fn password(&self) -> Option<String> {
        self.0.password.clone()
    }
    /// root of this backend.
    /// All operations will happen under this root.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

#[cfg(feature = "services-vercel-artifacts")]
/// Configuration for the `vercel-artifacts` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct VercelArtifactsConfig(ocore::services::VercelArtifactsConfig);

#[cfg(feature = "services-vercel-artifacts")]
#[allow(deprecated)]
impl ConfigBuilder for VercelArtifactsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "vercel-artifacts"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.access_token {
            map.insert("access_token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.team_id {
            map.insert("team_id".to_string(), v.clone());
        }
        if let Some(v) = &self.0.team_slug {
            map.insert("team_slug".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-vercel-artifacts")]
#[allow(deprecated)]
#[pymethods]
impl VercelArtifactsConfig {
    #[new]
    #[pyo3(signature = (
        access_token = None,
        endpoint = None,
        team_id = None,
        team_slug = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        access_token: Option<String>,
        endpoint: Option<String>,
        team_id: Option<String>,
        team_slug: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = access_token {
            opts.insert("access_token".to_string(), v);
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = team_id {
            opts.insert("team_id".to_string(), v);
        }
        if let Some(v) = team_slug {
            opts.insert("team_slug".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg =
            <ocore::services::VercelArtifactsConfig as ocore::Configurator>::from_iter(opts)
                .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// The access token for Vercel.
    #[getter]
    fn access_token(&self) -> Option<String> {
        self.0.access_token.clone()
    }
    /// The endpoint for the Vercel artifacts API.
    /// Defaults to `https://api.vercel.com`.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// The Vercel team ID.
    /// When set, the `teamId` query parameter is appended to all API requests.
    #[getter]
    fn team_id(&self) -> Option<String> {
        self.0.team_id.clone()
    }
    /// The Vercel team slug.
    /// When set, the `slug` query parameter is appended to all API requests.
    #[getter]
    fn team_slug(&self) -> Option<String> {
        self.0.team_slug.clone()
    }
}

#[cfg(feature = "services-webdav")]
/// Configuration for the `webdav` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct WebdavConfig(ocore::services::WebdavConfig);

#[cfg(feature = "services-webdav")]
#[allow(deprecated)]
impl ConfigBuilder for WebdavConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "webdav"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert(
            "disable_copy".to_string(),
            if self.0.disable_copy { "true" } else { "false" }.to_string(),
        );
        map.insert(
            "disable_create_dir".to_string(),
            if self.0.disable_create_dir {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "enable_conditional_read".to_string(),
            if self.0.enable_conditional_read {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        map.insert(
            "enable_user_metadata".to_string(),
            if self.0.enable_user_metadata {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.password {
            map.insert("password".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.token {
            map.insert("token".to_string(), v.clone());
        }
        if let Some(v) = &self.0.user_metadata_prefix {
            map.insert("user_metadata_prefix".to_string(), v.clone());
        }
        if let Some(v) = &self.0.user_metadata_uri {
            map.insert("user_metadata_uri".to_string(), v.clone());
        }
        if let Some(v) = &self.0.username {
            map.insert("username".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-webdav")]
#[allow(deprecated)]
#[pymethods]
impl WebdavConfig {
    #[new]
    #[pyo3(signature = (
        disable_copy = None,
        disable_create_dir = None,
        enable_conditional_read = None,
        enable_user_metadata = None,
        endpoint = None,
        password = None,
        root = None,
        token = None,
        user_metadata_prefix = None,
        user_metadata_uri = None,
        username = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        disable_copy: Option<bool>,
        disable_create_dir: Option<bool>,
        enable_conditional_read: Option<bool>,
        enable_user_metadata: Option<bool>,
        endpoint: Option<String>,
        password: Option<String>,
        root: Option<crate::PyPath>,
        token: Option<String>,
        user_metadata_prefix: Option<String>,
        user_metadata_uri: Option<String>,
        username: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = disable_copy {
            opts.insert(
                "disable_copy".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = disable_create_dir {
            opts.insert(
                "disable_create_dir".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = enable_conditional_read {
            opts.insert(
                "enable_conditional_read".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = enable_user_metadata {
            opts.insert(
                "enable_user_metadata".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = password {
            opts.insert("password".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = token {
            opts.insert("token".to_string(), v);
        }
        if let Some(v) = user_metadata_prefix {
            opts.insert("user_metadata_prefix".to_string(), v);
        }
        if let Some(v) = user_metadata_uri {
            opts.insert("user_metadata_uri".to_string(), v);
        }
        if let Some(v) = username {
            opts.insert("username".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::WebdavConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// Deprecated: WebDAV copy capability is enabled by default.
    /// [Deprecated since 0.57.0] WebDAV copy capability is enabled by default and
    /// this option is no longer needed.
    #[getter]
    fn disable_copy(&self) -> Option<bool> {
        Some(self.0.disable_copy)
    }
    /// Disable automatic parent directory creation before write operations.
    /// By default, OpenDAL creates parent directories using MKCOL before writing
    /// files.
    /// This requires PROPFIND support to check directory existence.
    /// Some WebDAV-compatible servers (e.g., bazel-remote) don't support PROPFIND
    /// or don't require explicit directory creation.
    /// Enable this option to skip the MKCOL calls and write files directly.
    /// Default: false
    #[getter]
    fn disable_create_dir(&self) -> Option<bool> {
        Some(self.0.disable_create_dir)
    }
    /// Enable conditional read support.
    /// When enabled (the default), OpenDAL forwards the RFC 7232 headers
    /// `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since`
    /// to the server when callers provide them.
    /// Some WebDAV-compatible servers (e.g., nginx-dav) don't return ETags in
    /// PROPFIND or don't honor these headers on GET.
    /// Setting this to `false` drops the four `read_with_if_*` capabilities, so
    /// calls like `reader_with(path).if_match(...)` return `ErrorKind::Unsupported`
    /// locally instead of being silently ignored by the server.
    /// Default: true
    #[getter]
    fn enable_conditional_read(&self) -> Option<bool> {
        Some(self.0.enable_conditional_read)
    }
    /// Deprecated: WebDAV user metadata capability is enabled by default.
    /// [Deprecated since 0.57.0] WebDAV user metadata capability is enabled by
    /// default.
    /// Use CapabilityOverrideLayer to override write_with_user_metadata for
    /// endpoints without PROPPATCH support.
    #[getter]
    fn enable_user_metadata(&self) -> Option<bool> {
        Some(self.0.enable_user_metadata)
    }
    /// endpoint of this backend
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// password of this backend
    #[getter]
    fn password(&self) -> Option<String> {
        self.0.password.clone()
    }
    /// root of this backend
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// token of this backend
    #[getter]
    fn token(&self) -> Option<String> {
        self.0.token.clone()
    }
    /// The XML namespace prefix for user metadata properties.
    /// This prefix is used in PROPPATCH/PROPFIND XML requests.
    /// Different servers may require different prefixes.
    /// Default: "opendal"
    #[getter]
    fn user_metadata_prefix(&self) -> Option<String> {
        self.0.user_metadata_prefix.clone()
    }
    /// The XML namespace URI for user metadata properties.
    /// This URI uniquely identifies the namespace for custom properties.
    /// Different servers may require different namespace URIs.
    /// For example, Nextcloud might work better with its own namespace.
    /// Default: `https://opendal.apache.org/ns`
    #[getter]
    fn user_metadata_uri(&self) -> Option<String> {
        self.0.user_metadata_uri.clone()
    }
    /// username of this backend
    #[getter]
    fn username(&self) -> Option<String> {
        self.0.username.clone()
    }
}

#[cfg(feature = "services-webhdfs")]
/// Configuration for the `webhdfs` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct WebhdfsConfig(ocore::services::WebhdfsConfig);

#[cfg(feature = "services-webhdfs")]
#[allow(deprecated)]
impl ConfigBuilder for WebhdfsConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "webhdfs"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        if let Some(v) = &self.0.atomic_write_dir {
            map.insert("atomic_write_dir".to_string(), v.clone());
        }
        if let Some(v) = &self.0.delegation {
            map.insert("delegation".to_string(), v.clone());
        }
        map.insert(
            "disable_list_batch".to_string(),
            if self.0.disable_list_batch {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        if let Some(v) = &self.0.endpoint {
            map.insert("endpoint".to_string(), v.clone());
        }
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        if let Some(v) = &self.0.user_name {
            map.insert("user_name".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-webhdfs")]
#[allow(deprecated)]
#[pymethods]
impl WebhdfsConfig {
    #[new]
    #[pyo3(signature = (
        atomic_write_dir = None,
        delegation = None,
        disable_list_batch = None,
        endpoint = None,
        root = None,
        user_name = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        atomic_write_dir: Option<crate::PyPath>,
        delegation: Option<String>,
        disable_list_batch: Option<bool>,
        endpoint: Option<String>,
        root: Option<crate::PyPath>,
        user_name: Option<String>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        if let Some(v) = atomic_write_dir {
            opts.insert("atomic_write_dir".to_string(), v.into_string());
        }
        if let Some(v) = delegation {
            opts.insert("delegation".to_string(), v);
        }
        if let Some(v) = disable_list_batch {
            opts.insert(
                "disable_list_batch".to_string(),
                if v { "true" } else { "false" }.to_string(),
            );
        }
        if let Some(v) = endpoint {
            opts.insert("endpoint".to_string(), v);
        }
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        if let Some(v) = user_name {
            opts.insert("user_name".to_string(), v);
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::WebhdfsConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// atomic_write_dir of this backend
    #[getter]
    fn atomic_write_dir(&self) -> Option<crate::PyPath> {
        self.0.atomic_write_dir.clone().map(crate::PyPath::from)
    }
    /// Delegation token for webhdfs.
    #[getter]
    fn delegation(&self) -> Option<String> {
        self.0.delegation.clone()
    }
    /// Disable batch listing
    #[getter]
    fn disable_list_batch(&self) -> Option<bool> {
        Some(self.0.disable_list_batch)
    }
    /// Endpoint for webhdfs.
    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.0.endpoint.clone()
    }
    /// Root for webhdfs.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
    /// Name of the user for webhdfs.
    #[getter]
    fn user_name(&self) -> Option<String> {
        self.0.user_name.clone()
    }
}

#[cfg(feature = "services-yandex-disk")]
/// Configuration for the `yandex-disk` service.
#[pyclass(module = "opendal.services", extends = ServiceConfig, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct YandexDiskConfig(ocore::services::YandexDiskConfig);

#[cfg(feature = "services-yandex-disk")]
#[allow(deprecated)]
impl ConfigBuilder for YandexDiskConfig {
    fn build(&self) -> PyResult<ocore::Operator> {
        ocore::Operator::from_config(self.0.clone()).map_err(format_pyerr)
    }
    fn scheme(&self) -> &'static str {
        "yandex-disk"
    }
    fn to_map(&self) -> HashMap<String, String> {
        #[allow(unused_mut)]
        let mut map = HashMap::new();
        map.insert("access_token".to_string(), self.0.access_token.clone());
        if let Some(v) = &self.0.root {
            map.insert("root".to_string(), v.clone());
        }
        map
    }
    fn is_picklable(&self) -> bool {
        #[allow(unused_mut)]
        let mut ok = true;
        ok
    }
}

#[cfg(feature = "services-yandex-disk")]
#[allow(deprecated)]
#[pymethods]
impl YandexDiskConfig {
    #[new]
    #[pyo3(signature = (
        access_token,
        root = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        access_token: String,
        root: Option<crate::PyPath>,
    ) -> PyResult<pyo3::PyClassInitializer<Self>> {
        #[allow(unused_mut)]
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("access_token".to_string(), access_token);
        if let Some(v) = root {
            opts.insert("root".to_string(), v.into_string());
        }
        #[allow(unused_mut)]
        let mut cfg = <ocore::services::YandexDiskConfig as ocore::Configurator>::from_iter(opts)
            .map_err(format_pyerr)?;
        let this = Self(cfg);
        Ok(
            pyo3::PyClassInitializer::from(ServiceConfig(Box::new(this.clone())))
                .add_subclass(this),
        )
    }

    /// yandex disk oauth access_token.
    #[getter]
    fn access_token(&self) -> String {
        self.0.access_token.clone()
    }
    /// root of this backend.
    /// All operations will happen under this root.
    #[getter]
    fn root(&self) -> Option<crate::PyPath> {
        self.0.root.clone().map(crate::PyPath::from)
    }
}

// Defined here (not in `lib.rs`) so pyo3 introspection sees the config
// `#[pymodule_export]`s as literal items.
#[pyo3::pymodule(submodule, name = "services")]
pub mod services_submodule {
    #[cfg(feature = "services-aliyun-drive")]
    #[pymodule_export]
    use crate::AliyunDriveConfig;
    #[cfg(feature = "services-alluxio")]
    #[pymodule_export]
    use crate::AlluxioConfig;
    #[cfg(feature = "services-azblob")]
    #[pymodule_export]
    use crate::AzblobConfig;
    #[cfg(feature = "services-azdls")]
    #[pymodule_export]
    use crate::AzdlsConfig;
    #[cfg(feature = "services-azfile")]
    #[pymodule_export]
    use crate::AzfileConfig;
    #[cfg(feature = "services-b2")]
    #[pymodule_export]
    use crate::B2Config;
    #[cfg(feature = "services-cacache")]
    #[pymodule_export]
    use crate::CacacheConfig;
    #[cfg(feature = "services-cos")]
    #[pymodule_export]
    use crate::CosConfig;
    #[cfg(feature = "services-dashmap")]
    #[pymodule_export]
    use crate::DashmapConfig;
    #[cfg(feature = "services-dropbox")]
    #[pymodule_export]
    use crate::DropboxConfig;
    #[cfg(feature = "services-fs")]
    #[pymodule_export]
    use crate::FsConfig;
    #[cfg(feature = "services-ftp")]
    #[pymodule_export]
    use crate::FtpConfig;
    #[cfg(feature = "services-gcs")]
    #[pymodule_export]
    use crate::GcsConfig;
    #[cfg(feature = "services-gdrive")]
    #[pymodule_export]
    use crate::GdriveConfig;
    #[cfg(feature = "services-ghac")]
    #[pymodule_export]
    use crate::GhacConfig;
    #[cfg(feature = "services-goosefs")]
    #[pymodule_export]
    use crate::GoosefsConfig;
    #[cfg(feature = "services-gridfs")]
    #[pymodule_export]
    use crate::GridfsConfig;
    #[cfg(feature = "services-hdfs-native")]
    #[pymodule_export]
    use crate::HdfsNativeConfig;
    #[cfg(feature = "services-hf")]
    #[pymodule_export]
    use crate::HfConfig;
    #[cfg(feature = "services-http")]
    #[pymodule_export]
    use crate::HttpConfig;
    #[cfg(feature = "services-ipfs")]
    #[pymodule_export]
    use crate::IpfsConfig;
    #[cfg(feature = "services-ipmfs")]
    #[pymodule_export]
    use crate::IpmfsConfig;
    #[cfg(feature = "services-koofr")]
    #[pymodule_export]
    use crate::KoofrConfig;
    #[cfg(feature = "services-memcached")]
    #[pymodule_export]
    use crate::MemcachedConfig;
    #[cfg(feature = "services-memory")]
    #[pymodule_export]
    use crate::MemoryConfig;
    #[cfg(feature = "services-mini-moka")]
    #[pymodule_export]
    use crate::MiniMokaConfig;
    #[cfg(feature = "services-moka")]
    #[pymodule_export]
    use crate::MokaConfig;
    #[cfg(feature = "services-mongodb")]
    #[pymodule_export]
    use crate::MongodbConfig;
    #[cfg(feature = "services-mysql")]
    #[pymodule_export]
    use crate::MysqlConfig;
    #[cfg(feature = "services-obs")]
    #[pymodule_export]
    use crate::ObsConfig;
    #[cfg(feature = "services-onedrive")]
    #[pymodule_export]
    use crate::OnedriveConfig;
    #[cfg(feature = "services-oss")]
    #[pymodule_export]
    use crate::OssConfig;
    #[cfg(feature = "services-persy")]
    #[pymodule_export]
    use crate::PersyConfig;
    #[cfg(feature = "services-postgresql")]
    #[pymodule_export]
    use crate::PostgresqlConfig;
    #[cfg(feature = "services-redb")]
    #[pymodule_export]
    use crate::RedbConfig;
    #[cfg(feature = "services-redis")]
    #[pymodule_export]
    use crate::RedisConfig;
    #[cfg(feature = "services-s3")]
    #[pymodule_export]
    use crate::S3Config;
    #[pymodule_export]
    use crate::Scheme;
    #[cfg(feature = "services-seafile")]
    #[pymodule_export]
    use crate::SeafileConfig;
    #[pymodule_export]
    use crate::ServiceConfig;
    #[cfg(feature = "services-sftp")]
    #[pymodule_export]
    use crate::SftpConfig;
    #[cfg(feature = "services-sled")]
    #[pymodule_export]
    use crate::SledConfig;
    #[cfg(feature = "services-sqlite")]
    #[pymodule_export]
    use crate::SqliteConfig;
    #[cfg(feature = "services-swift")]
    #[pymodule_export]
    use crate::SwiftConfig;
    #[cfg(feature = "services-tos")]
    #[pymodule_export]
    use crate::TosConfig;
    #[cfg(feature = "services-upyun")]
    #[pymodule_export]
    use crate::UpyunConfig;
    #[cfg(feature = "services-vercel-artifacts")]
    #[pymodule_export]
    use crate::VercelArtifactsConfig;
    #[cfg(feature = "services-webdav")]
    #[pymodule_export]
    use crate::WebdavConfig;
    #[cfg(feature = "services-webhdfs")]
    #[pymodule_export]
    use crate::WebhdfsConfig;
    #[cfg(feature = "services-yandex-disk")]
    #[pymodule_export]
    use crate::YandexDiskConfig;
}
