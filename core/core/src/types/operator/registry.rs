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

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use crate::types::builder::{Builder, Configurator};
use crate::types::{IntoOperatorUri, OperatorUri};
use crate::{Error, ErrorKind, Operator, Result};

/// Factory signature used to construct [`Operator`] from a URI and extra options.
pub type OperatorFactory = fn(&OperatorUri) -> Result<Operator>;

/// Default registry initialized with builtin services.
pub static DEFAULT_OPERATOR_REGISTRY: LazyLock<OperatorRegistry> = LazyLock::new(|| {
    let registry = OperatorRegistry::new();
    register_builtin_services(&registry);
    registry
});

/// Global registry that maps schemes to [`OperatorFactory`] functions.
#[derive(Debug, Default)]
pub struct OperatorRegistry {
    factories: Mutex<HashMap<String, OperatorFactory>>,
}

impl OperatorRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            factories: Mutex::new(HashMap::new()),
        }
    }

    /// Register a builder for the given scheme.
    pub fn register<B: Builder>(&self, scheme: &str) {
        let key = scheme.to_ascii_lowercase();
        let mut guard = self
            .factories
            .lock()
            .expect("operator registry mutex poisoned");
        guard.insert(key, factory::<B::Config>);
    }

    /// Load an [`Operator`] via the factory registered for the URI's scheme.
    pub fn load(&self, uri: impl IntoOperatorUri) -> Result<Operator> {
        let parsed = uri.into_operator_uri()?;
        let scheme = parsed.scheme();

        let factory = self
            .factories
            .lock()
            .expect("operator registry mutex poisoned")
            .get(scheme)
            .copied()
            .ok_or_else(|| {
                Error::new(ErrorKind::Unsupported, "scheme is not registered")
                    .with_context("scheme", scheme.to_string())
            })?;

        factory(&parsed)
    }
}

fn register_builtin_services(registry: &OperatorRegistry) {
    #[cfg(feature = "services-aliyun-drive")]
    registry.register::<crate::services::AliyunDrive>(crate::services::ALIYUN_DRIVE_SCHEME);
    #[cfg(feature = "services-alluxio")]
    registry.register::<crate::services::Alluxio>(crate::services::ALLUXIO_SCHEME);
    #[cfg(feature = "services-azblob")]
    registry.register::<crate::services::Azblob>(crate::services::AZBLOB_SCHEME);
    #[cfg(feature = "services-azdls")]
    registry.register::<crate::services::Azdls>(crate::services::AZDLS_SCHEME);
    #[cfg(feature = "services-azfile")]
    registry.register::<crate::services::Azfile>(crate::services::AZFILE_SCHEME);
    #[cfg(feature = "services-b2")]
    registry.register::<crate::services::B2>(crate::services::B2_SCHEME);
    #[cfg(feature = "services-cacache")]
    registry.register::<crate::services::Cacache>(crate::services::CACACHE_SCHEME);
    #[cfg(feature = "services-cloudflare-kv")]
    registry.register::<crate::services::CloudflareKv>(crate::services::CLOUDFLARE_KV_SCHEME);
    #[cfg(feature = "services-compfs")]
    registry.register::<crate::services::Compfs>(crate::services::COMPFS_SCHEME);
    #[cfg(feature = "services-cos")]
    registry.register::<crate::services::Cos>(crate::services::COS_SCHEME);
    #[cfg(feature = "services-d1")]
    registry.register::<crate::services::D1>(crate::services::D1_SCHEME);
    #[cfg(feature = "services-dashmap")]
    registry.register::<crate::services::Dashmap>(crate::services::DASHMAP_SCHEME);
    #[cfg(feature = "services-dbfs")]
    registry.register::<crate::services::Dbfs>(crate::services::DBFS_SCHEME);
    #[cfg(feature = "services-dropbox")]
    registry.register::<crate::services::Dropbox>(crate::services::DROPBOX_SCHEME);
    #[cfg(feature = "services-etcd")]
    registry.register::<crate::services::Etcd>(crate::services::ETCD_SCHEME);
    #[cfg(feature = "services-foundationdb")]
    registry.register::<crate::services::Foundationdb>(crate::services::FOUNDATIONDB_SCHEME);
    #[cfg(feature = "services-fs")]
    registry.register::<crate::services::Fs>(crate::services::FS_SCHEME);
    #[cfg(feature = "services-ftp")]
    registry.register::<crate::services::Ftp>(crate::services::FTP_SCHEME);
    #[cfg(feature = "services-gcs")]
    registry.register::<crate::services::Gcs>(crate::services::GCS_SCHEME);
    #[cfg(feature = "services-gdrive")]
    registry.register::<crate::services::Gdrive>(crate::services::GDRIVE_SCHEME);
    #[cfg(feature = "services-ghac")]
    registry.register::<crate::services::Ghac>(crate::services::GHAC_SCHEME);
    #[cfg(feature = "services-github")]
    registry.register::<crate::services::Github>(crate::services::GITHUB_SCHEME);
    #[cfg(feature = "services-gridfs")]
    registry.register::<crate::services::Gridfs>(crate::services::GRIDFS_SCHEME);
    #[cfg(feature = "services-hdfs")]
    registry.register::<crate::services::Hdfs>(crate::services::HDFS_SCHEME);
    #[cfg(feature = "services-hdfs-native")]
    registry.register::<crate::services::HdfsNative>(crate::services::HDFS_NATIVE_SCHEME);
    #[cfg(feature = "services-http")]
    registry.register::<crate::services::Http>(crate::services::HTTP_SCHEME);
    #[cfg(feature = "services-huggingface")]
    registry.register::<crate::services::Huggingface>(crate::services::HUGGINGFACE_SCHEME);
    #[cfg(feature = "services-ipfs")]
    registry.register::<crate::services::Ipfs>(crate::services::IPFS_SCHEME);
    #[cfg(feature = "services-ipmfs")]
    registry.register::<crate::services::Ipmfs>(crate::services::IPMFS_SCHEME);
    #[cfg(feature = "services-koofr")]
    registry.register::<crate::services::Koofr>(crate::services::KOOFR_SCHEME);
    #[cfg(feature = "services-lakefs")]
    registry.register::<crate::services::Lakefs>(crate::services::LAKEFS_SCHEME);
    #[cfg(feature = "services-memcached")]
    registry.register::<crate::services::Memcached>(crate::services::MEMCACHED_SCHEME);
    #[cfg(feature = "services-memory")]
    registry.register::<crate::services::Memory>(crate::services::MEMORY_SCHEME);
    #[cfg(feature = "services-mini-moka")]
    registry.register::<crate::services::MiniMoka>(crate::services::MINI_MOKA_SCHEME);
    #[cfg(feature = "services-moka")]
    registry.register::<crate::services::Moka>(crate::services::MOKA_SCHEME);
    #[cfg(feature = "services-mongodb")]
    registry.register::<crate::services::Mongodb>(crate::services::MONGODB_SCHEME);
    #[cfg(feature = "services-monoiofs")]
    registry.register::<crate::services::Monoiofs>(crate::services::MONOIOFS_SCHEME);
    #[cfg(feature = "services-mysql")]
    registry.register::<crate::services::Mysql>(crate::services::MYSQL_SCHEME);
    #[cfg(feature = "services-obs")]
    registry.register::<crate::services::Obs>(crate::services::OBS_SCHEME);
    #[cfg(feature = "services-onedrive")]
    registry.register::<crate::services::Onedrive>(crate::services::ONEDRIVE_SCHEME);
    #[cfg(feature = "services-oss")]
    registry.register::<crate::services::Oss>(crate::services::OSS_SCHEME);
    #[cfg(feature = "services-pcloud")]
    registry.register::<crate::services::Pcloud>(crate::services::PCLOUD_SCHEME);
    #[cfg(feature = "services-persy")]
    registry.register::<crate::services::Persy>(crate::services::PERSY_SCHEME);
    #[cfg(feature = "services-postgresql")]
    registry.register::<crate::services::Postgresql>(crate::services::POSTGRESQL_SCHEME);
    #[cfg(feature = "services-redb")]
    registry.register::<crate::services::Redb>(crate::services::REDB_SCHEME);
    #[cfg(feature = "services-redis")]
    registry.register::<crate::services::Redis>(crate::services::REDIS_SCHEME);
    #[cfg(feature = "services-rocksdb")]
    registry.register::<crate::services::Rocksdb>(crate::services::ROCKSDB_SCHEME);
    #[cfg(feature = "services-s3")]
    registry.register::<crate::services::S3>(crate::services::S3_SCHEME);
    #[cfg(feature = "services-seafile")]
    registry.register::<crate::services::Seafile>(crate::services::SEAFILE_SCHEME);
    #[cfg(feature = "services-sftp")]
    registry.register::<crate::services::Sftp>(crate::services::SFTP_SCHEME);
    #[cfg(feature = "services-sled")]
    registry.register::<crate::services::Sled>(crate::services::SLED_SCHEME);
    #[cfg(feature = "services-sqlite")]
    registry.register::<crate::services::Sqlite>(crate::services::SQLITE_SCHEME);
    #[cfg(feature = "services-surrealdb")]
    registry.register::<crate::services::Surrealdb>(crate::services::SURREALDB_SCHEME);
    #[cfg(feature = "services-swift")]
    registry.register::<crate::services::Swift>(crate::services::SWIFT_SCHEME);
    #[cfg(feature = "services-tikv")]
    registry.register::<crate::services::Tikv>(crate::services::TIKV_SCHEME);
    #[cfg(feature = "services-upyun")]
    registry.register::<crate::services::Upyun>(crate::services::UPYUN_SCHEME);
    #[cfg(feature = "services-vercel-artifacts")]
    registry.register::<crate::services::VercelArtifacts>(crate::services::VERCEL_ARTIFACTS_SCHEME);
    #[cfg(feature = "services-vercel-blob")]
    registry.register::<crate::services::VercelBlob>(crate::services::VERCEL_BLOB_SCHEME);
    #[cfg(feature = "services-webdav")]
    registry.register::<crate::services::Webdav>(crate::services::WEBDAV_SCHEME);
    #[cfg(feature = "services-webhdfs")]
    registry.register::<crate::services::Webhdfs>(crate::services::WEBHDFS_SCHEME);
    #[cfg(feature = "services-yandex-disk")]
    registry.register::<crate::services::YandexDisk>(crate::services::YANDEX_DISK_SCHEME);
}

/// Factory adapter that builds an operator from a configurator type.
fn factory<C: Configurator>(uri: &OperatorUri) -> Result<Operator> {
    let cfg = C::from_uri(uri)?;
    let builder = Operator::from_config(cfg)?;
    Ok(builder.finish())
}
