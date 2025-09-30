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
use std::sync::LazyLock;

use http::Uri;

use crate::services::*;
use crate::*;

/// TODO: document this
pub static GLOBAL_OPERATOR_REGISTRY: LazyLock<OperatorRegistry> =
    LazyLock::new(OperatorRegistry::initialized);

/// TODO: document this
pub type OperatorFactory = fn(&Uri, HashMap<String, String>) -> Result<Operator>;

/// TODO: document this
#[derive(Clone, Debug, Default)]
pub struct OperatorRegistry {
    registry: HashMap<String, OperatorFactory>,
}

impl OperatorRegistry {
    /// TODO: document this
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
        }
    }
    /// TODO: document this
    pub fn initialized() -> Self {
        let mut registry = Self::new();

        #[cfg(feature = "services-aliyun-drive")]
        AliyunDrive::register_in_registry(&mut registry);
        #[cfg(feature = "services-atomicserver")]
        Atomicserver::register_in_registry(&mut registry);
        #[cfg(feature = "services-alluxio")]
        Alluxio::register_in_registry(&mut registry);
        #[cfg(feature = "services-azblob")]
        Azblob::register_in_registry(&mut registry);
        #[cfg(feature = "services-azdls")]
        Azdls::register_in_registry(&mut registry);
        #[cfg(feature = "services-azfile")]
        Azfile::register_in_registry(&mut registry);
        #[cfg(feature = "services-b2")]
        B2::register_in_registry(&mut registry);
        #[cfg(feature = "services-cacache")]
        Cacache::register_in_registry(&mut registry);
        #[cfg(feature = "services-cos")]
        Cos::register_in_registry(&mut registry);
        #[cfg(feature = "services-compfs")]
        Compfs::register_in_registry(&mut registry);
        #[cfg(feature = "services-dashmap")]
        Dashmap::register_in_registry(&mut registry);
        #[cfg(feature = "services-dropbox")]
        Dropbox::register_in_registry(&mut registry);
        #[cfg(feature = "services-etcd")]
        Etcd::register_in_registry(&mut registry);
        #[cfg(feature = "services-foundationdb")]
        Foundationdb::register_in_registry(&mut registry);
        #[cfg(feature = "services-fs")]
        Fs::register_in_registry(&mut registry);
        #[cfg(feature = "services-ftp")]
        Ftp::register_in_registry(&mut registry);
        #[cfg(feature = "services-gcs")]
        Gcs::register_in_registry(&mut registry);
        #[cfg(feature = "services-ghac")]
        Ghac::register_in_registry(&mut registry);
        #[cfg(feature = "services-hdfs")]
        Hdfs::register_in_registry(&mut registry);
        #[cfg(feature = "services-http")]
        Http::register_in_registry(&mut registry);
        #[cfg(feature = "services-huggingface")]
        Huggingface::register_in_registry(&mut registry);
        #[cfg(feature = "services-ipfs")]
        Ipfs::register_in_registry(&mut registry);
        #[cfg(feature = "services-ipmfs")]
        Ipmfs::register_in_registry(&mut registry);
        #[cfg(feature = "services-icloud")]
        Icloud::register_in_registry(&mut registry);
        #[cfg(feature = "services-memcached")]
        Memcached::register_in_registry(&mut registry);
        #[cfg(feature = "services-memory")]
        Memory::register_in_registry(&mut registry);
        #[cfg(feature = "services-mini-moka")]
        MiniMoka::register_in_registry(&mut registry);
        #[cfg(feature = "services-moka")]
        Moka::register_in_registry(&mut registry);
        #[cfg(feature = "services-monoiofs")]
        Monoiofs::register_in_registry(&mut registry);
        #[cfg(feature = "services-mysql")]
        Mysql::register_in_registry(&mut registry);
        #[cfg(feature = "services-obs")]
        Obs::register_in_registry(&mut registry);
        #[cfg(feature = "services-onedrive")]
        Onedrive::register_in_registry(&mut registry);
        #[cfg(feature = "services-postgresql")]
        Postgresql::register_in_registry(&mut registry);
        #[cfg(feature = "services-gdrive")]
        Gdrive::register_in_registry(&mut registry);
        #[cfg(feature = "services-oss")]
        Oss::register_in_registry(&mut registry);
        #[cfg(feature = "services-persy")]
        Persy::register_in_registry(&mut registry);
        #[cfg(feature = "services-redis")]
        Redis::register_in_registry(&mut registry);
        #[cfg(feature = "services-rocksdb")]
        Rocksdb::register_in_registry(&mut registry);
        #[cfg(feature = "services-s3")]
        S3::register_in_registry(&mut registry);
        #[cfg(feature = "services-seafile")]
        Seafile::register_in_registry(&mut registry);
        #[cfg(feature = "services-upyun")]
        Upyun::register_in_registry(&mut registry);
        #[cfg(feature = "services-yandex-disk")]
        YandexDisk::register_in_registry(&mut registry);
        #[cfg(feature = "services-pcloud")]
        Pcloud::register_in_registry(&mut registry);
        #[cfg(feature = "services-sftp")]
        Sftp::register_in_registry(&mut registry);
        #[cfg(feature = "services-sled")]
        Sled::register_in_registry(&mut registry);
        #[cfg(feature = "services-sqlite")]
        Sqlite::register_in_registry(&mut registry);
        #[cfg(feature = "services-swift")]
        Swift::register_in_registry(&mut registry);
        #[cfg(feature = "services-tikv")]
        Tikv::register_in_registry(&mut registry);
        #[cfg(feature = "services-vercel-artifacts")]
        VercelArtifacts::register_in_registry(&mut registry);
        #[cfg(feature = "services-vercel-blob")]
        VercelBlob::register_in_registry(&mut registry);
        #[cfg(feature = "services-webdav")]
        Webdav::register_in_registry(&mut registry);
        #[cfg(feature = "services-webhdfs")]
        Webhdfs::register_in_registry(&mut registry);
        #[cfg(feature = "services-redb")]
        Redb::register_in_registry(&mut registry);
        #[cfg(feature = "services-mongodb")]
        Mongodb::register_in_registry(&mut registry);
        #[cfg(feature = "services-hdfs-native")]
        HdfsNative::register_in_registry(&mut registry);
        #[cfg(feature = "services-surrealdb")]
        Surrealdb::register_in_registry(&mut registry);
        #[cfg(feature = "services-lakefs")]
        Lakefs::register_in_registry(&mut registry);
        #[cfg(feature = "services-nebula-graph")]
        NebulaGraph::register_in_registry(&mut registry);

        registry
    }

    /// TODO: document this
    pub fn register<T: Into<String>>(&mut self, scheme: T, factory: OperatorFactory) {
        self.registry.insert(scheme.into(), factory);
    }

    /// TODO: document this
    pub fn parse(
        &self,
        uri: &str,
        options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        let parsed_uri = uri.parse::<Uri>().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "uri is invalid")
                .with_context("uri", uri)
                .set_source(err)
        })?;

        let scheme = parsed_uri.scheme_str().ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "uri is missing scheme").with_context("uri", uri)
        })?;

        let factory = self.registry.get(scheme).ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "could not find any operator factory for the given scheme",
            )
            .with_context("uri", uri)
            .with_context("scheme", scheme)
        })?;

        let options = options.into_iter().collect();

        factory(&parsed_uri, options)
    }
}
