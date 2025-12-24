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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/opendal/main/website/static/img/logo.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! Facade crate that re-exports all public APIs from `opendal-core` and optional services/layers.
#![deny(missing_docs)]

pub use opendal_core::*;

#[cfg(feature = "tests")]
pub use opendal_testkit as tests;

#[ctor::ctor]
fn register_default_operator_registry() {
    let registry = &opendal_core::DEFAULT_OPERATOR_REGISTRY;

    #[cfg(feature = "services-memory")]
    registry.register::<opendal_core::services::Memory>(opendal_core::services::MEMORY_SCHEME);

    #[cfg(feature = "services-aliyun-drive")]
    registry.register::<opendal_service_aliyun_drive::AliyunDrive>(
        opendal_service_aliyun_drive::ALIYUN_DRIVE_SCHEME,
    );
    #[cfg(feature = "services-alluxio")]
    registry.register::<opendal_service_alluxio::Alluxio>(opendal_service_alluxio::ALLUXIO_SCHEME);
    #[cfg(feature = "services-azblob")]
    registry.register::<opendal_service_azblob::Azblob>(opendal_service_azblob::AZBLOB_SCHEME);
    #[cfg(feature = "services-azdls")]
    registry.register::<opendal_service_azdls::Azdls>(opendal_service_azdls::AZDLS_SCHEME);
    #[cfg(feature = "services-azfile")]
    registry.register::<opendal_service_azfile::Azfile>(opendal_service_azfile::AZFILE_SCHEME);
    #[cfg(feature = "services-b2")]
    registry.register::<opendal_service_b2::B2>(opendal_service_b2::B2_SCHEME);
    #[cfg(feature = "services-cacache")]
    registry.register::<opendal_service_cacache::Cacache>(opendal_service_cacache::CACACHE_SCHEME);
    #[cfg(feature = "services-cloudflare-kv")]
    registry.register::<opendal_service_cloudflare_kv::CloudflareKv>(
        opendal_service_cloudflare_kv::CLOUDFLARE_KV_SCHEME,
    );
    #[cfg(feature = "services-compfs")]
    registry.register::<opendal_service_compfs::Compfs>(opendal_service_compfs::COMPFS_SCHEME);
    #[cfg(feature = "services-cos")]
    registry.register::<opendal_service_cos::Cos>(opendal_service_cos::COS_SCHEME);
    #[cfg(feature = "services-d1")]
    registry.register::<opendal_service_d1::D1>(opendal_service_d1::D1_SCHEME);
    #[cfg(feature = "services-dashmap")]
    registry.register::<opendal_service_dashmap::Dashmap>(opendal_service_dashmap::DASHMAP_SCHEME);
    #[cfg(feature = "services-dbfs")]
    registry.register::<opendal_service_dbfs::Dbfs>(opendal_service_dbfs::DBFS_SCHEME);
    #[cfg(feature = "services-dropbox")]
    registry.register::<opendal_service_dropbox::Dropbox>(opendal_service_dropbox::DROPBOX_SCHEME);
    #[cfg(feature = "services-etcd")]
    registry.register::<opendal_service_etcd::Etcd>(opendal_service_etcd::ETCD_SCHEME);
    #[cfg(feature = "services-foundationdb")]
    registry.register::<opendal_service_foundationdb::Foundationdb>(
        opendal_service_foundationdb::FOUNDATIONDB_SCHEME,
    );
    #[cfg(feature = "services-fs")]
    {
        registry.register::<opendal_service_fs::Fs>(opendal_service_fs::FS_SCHEME);
        registry.register::<opendal_service_fs::Fs>(opendal_service_fs::FILE_SCHEME);
    }
    #[cfg(feature = "services-ftp")]
    registry.register::<opendal_service_ftp::Ftp>(opendal_service_ftp::FTP_SCHEME);
    #[cfg(feature = "services-gcs")]
    registry.register::<opendal_service_gcs::Gcs>(opendal_service_gcs::GCS_SCHEME);
    #[cfg(feature = "services-gdrive")]
    registry.register::<opendal_service_gdrive::Gdrive>(opendal_service_gdrive::GDRIVE_SCHEME);
    #[cfg(feature = "services-ghac")]
    registry.register::<opendal_service_ghac::Ghac>(opendal_service_ghac::GHAC_SCHEME);
    #[cfg(feature = "services-github")]
    registry.register::<opendal_service_github::Github>(opendal_service_github::GITHUB_SCHEME);
    #[cfg(feature = "services-gridfs")]
    registry.register::<opendal_service_gridfs::Gridfs>(opendal_service_gridfs::GRIDFS_SCHEME);
    #[cfg(feature = "services-hdfs")]
    registry.register::<opendal_service_hdfs::Hdfs>(opendal_service_hdfs::HDFS_SCHEME);
    #[cfg(feature = "services-hdfs-native")]
    registry.register::<opendal_service_hdfs_native::HdfsNative>(
        opendal_service_hdfs_native::HDFS_NATIVE_SCHEME,
    );
    #[cfg(feature = "services-http")]
    registry.register::<opendal_service_http::Http>(opendal_service_http::HTTP_SCHEME);
    #[cfg(feature = "services-huggingface")]
    registry.register::<opendal_service_huggingface::Huggingface>(
        opendal_service_huggingface::HUGGINGFACE_SCHEME,
    );
    #[cfg(feature = "services-ipfs")]
    registry.register::<opendal_service_ipfs::Ipfs>(opendal_service_ipfs::IPFS_SCHEME);
    #[cfg(feature = "services-ipmfs")]
    registry.register::<opendal_service_ipmfs::Ipmfs>(opendal_service_ipmfs::IPMFS_SCHEME);
    #[cfg(feature = "services-koofr")]
    registry.register::<opendal_service_koofr::Koofr>(opendal_service_koofr::KOOFR_SCHEME);
    #[cfg(feature = "services-lakefs")]
    registry.register::<opendal_service_lakefs::Lakefs>(opendal_service_lakefs::LAKEFS_SCHEME);
    #[cfg(feature = "services-memcached")]
    registry.register::<opendal_service_memcached::Memcached>(
        opendal_service_memcached::MEMCACHED_SCHEME,
    );
    #[cfg(feature = "services-mini-moka")]
    registry.register::<opendal_service_mini_moka::MiniMoka>(
        opendal_service_mini_moka::MINI_MOKA_SCHEME,
    );
    #[cfg(feature = "services-moka")]
    registry.register::<opendal_service_moka::Moka>(opendal_service_moka::MOKA_SCHEME);
    #[cfg(feature = "services-mongodb")]
    registry.register::<opendal_service_mongodb::Mongodb>(opendal_service_mongodb::MONGODB_SCHEME);
    #[cfg(feature = "services-monoiofs")]
    registry
        .register::<opendal_service_monoiofs::Monoiofs>(opendal_service_monoiofs::MONOIOFS_SCHEME);
    #[cfg(feature = "services-mysql")]
    registry.register::<opendal_service_mysql::Mysql>(opendal_service_mysql::MYSQL_SCHEME);
    #[cfg(feature = "services-obs")]
    registry.register::<opendal_service_obs::Obs>(opendal_service_obs::OBS_SCHEME);
    #[cfg(feature = "services-onedrive")]
    registry
        .register::<opendal_service_onedrive::Onedrive>(opendal_service_onedrive::ONEDRIVE_SCHEME);
    #[cfg(all(target_arch = "wasm32", feature = "services-opfs"))]
    registry.register::<opendal_service_opfs::Opfs>(opendal_service_opfs::OPFS_SCHEME);
    #[cfg(feature = "services-oss")]
    registry.register::<opendal_service_oss::Oss>(opendal_service_oss::OSS_SCHEME);
    #[cfg(feature = "services-pcloud")]
    registry.register::<opendal_service_pcloud::Pcloud>(opendal_service_pcloud::PCLOUD_SCHEME);
    #[cfg(feature = "services-persy")]
    registry.register::<opendal_service_persy::Persy>(opendal_service_persy::PERSY_SCHEME);
    #[cfg(feature = "services-postgresql")]
    registry.register::<opendal_service_postgresql::Postgresql>(
        opendal_service_postgresql::POSTGRESQL_SCHEME,
    );
    #[cfg(feature = "services-redb")]
    registry.register::<opendal_service_redb::Redb>(opendal_service_redb::REDB_SCHEME);
    #[cfg(any(feature = "services-redis", feature = "services-redis-native-tls"))]
    registry.register::<opendal_service_redis::Redis>(opendal_service_redis::REDIS_SCHEME);
    #[cfg(feature = "services-rocksdb")]
    registry.register::<opendal_service_rocksdb::Rocksdb>(opendal_service_rocksdb::ROCKSDB_SCHEME);
    #[cfg(feature = "services-s3")]
    registry.register::<opendal_service_s3::S3>(opendal_service_s3::S3_SCHEME);
    #[cfg(feature = "services-seafile")]
    registry.register::<opendal_service_seafile::Seafile>(opendal_service_seafile::SEAFILE_SCHEME);
    #[cfg(feature = "services-sftp")]
    registry.register::<opendal_service_sftp::Sftp>(opendal_service_sftp::SFTP_SCHEME);
    #[cfg(feature = "services-sled")]
    registry.register::<opendal_service_sled::Sled>(opendal_service_sled::SLED_SCHEME);
    #[cfg(feature = "services-sqlite")]
    registry.register::<opendal_service_sqlite::Sqlite>(opendal_service_sqlite::SQLITE_SCHEME);
    #[cfg(feature = "services-surrealdb")]
    registry.register::<opendal_service_surrealdb::Surrealdb>(
        opendal_service_surrealdb::SURREALDB_SCHEME,
    );
    #[cfg(feature = "services-swift")]
    registry.register::<opendal_service_swift::Swift>(opendal_service_swift::SWIFT_SCHEME);
    #[cfg(feature = "services-tikv")]
    registry.register::<opendal_service_tikv::Tikv>(opendal_service_tikv::TIKV_SCHEME);
    #[cfg(feature = "services-upyun")]
    registry.register::<opendal_service_upyun::Upyun>(opendal_service_upyun::UPYUN_SCHEME);
    #[cfg(feature = "services-vercel-artifacts")]
    registry.register::<opendal_service_vercel_artifacts::VercelArtifacts>(
        opendal_service_vercel_artifacts::VERCEL_ARTIFACTS_SCHEME,
    );
    #[cfg(feature = "services-vercel-blob")]
    registry.register::<opendal_service_vercel_blob::VercelBlob>(
        opendal_service_vercel_blob::VERCEL_BLOB_SCHEME,
    );
    #[cfg(feature = "services-webdav")]
    registry.register::<opendal_service_webdav::Webdav>(opendal_service_webdav::WEBDAV_SCHEME);
    #[cfg(feature = "services-webhdfs")]
    registry.register::<opendal_service_webhdfs::Webhdfs>(opendal_service_webhdfs::WEBHDFS_SCHEME);
    #[cfg(feature = "services-yandex-disk")]
    registry.register::<opendal_service_yandex_disk::YandexDisk>(
        opendal_service_yandex_disk::YANDEX_DISK_SCHEME,
    );
}

/// Re-export of service implementations.
pub mod services {
    pub use opendal_core::services::*;
    #[cfg(feature = "services-aliyun-drive")]
    pub use opendal_service_aliyun_drive::*;
    #[cfg(feature = "services-alluxio")]
    pub use opendal_service_alluxio::*;
    #[cfg(feature = "services-azblob")]
    pub use opendal_service_azblob::*;
    #[cfg(feature = "services-azdls")]
    pub use opendal_service_azdls::*;
    #[cfg(feature = "services-azfile")]
    pub use opendal_service_azfile::*;
    #[cfg(feature = "services-b2")]
    pub use opendal_service_b2::*;
    #[cfg(feature = "services-cacache")]
    pub use opendal_service_cacache::*;
    #[cfg(feature = "services-cloudflare-kv")]
    pub use opendal_service_cloudflare_kv::*;
    #[cfg(feature = "services-compfs")]
    pub use opendal_service_compfs::*;
    #[cfg(feature = "services-cos")]
    pub use opendal_service_cos::*;
    #[cfg(feature = "services-d1")]
    pub use opendal_service_d1::*;
    #[cfg(feature = "services-dashmap")]
    pub use opendal_service_dashmap::*;
    #[cfg(feature = "services-dbfs")]
    pub use opendal_service_dbfs::*;
    #[cfg(feature = "services-dropbox")]
    pub use opendal_service_dropbox::*;
    #[cfg(feature = "services-etcd")]
    pub use opendal_service_etcd::*;
    #[cfg(feature = "services-foundationdb")]
    pub use opendal_service_foundationdb::*;
    #[cfg(feature = "services-fs")]
    pub use opendal_service_fs::*;
    #[cfg(feature = "services-ftp")]
    pub use opendal_service_ftp::*;
    #[cfg(feature = "services-gcs")]
    pub use opendal_service_gcs::*;
    #[cfg(feature = "services-gdrive")]
    pub use opendal_service_gdrive::*;
    #[cfg(feature = "services-ghac")]
    pub use opendal_service_ghac::*;
    #[cfg(feature = "services-github")]
    pub use opendal_service_github::*;
    #[cfg(feature = "services-gridfs")]
    pub use opendal_service_gridfs::*;
    #[cfg(feature = "services-hdfs")]
    pub use opendal_service_hdfs::*;
    #[cfg(feature = "services-hdfs-native")]
    pub use opendal_service_hdfs_native::*;
    #[cfg(feature = "services-http")]
    pub use opendal_service_http::*;
    #[cfg(feature = "services-huggingface")]
    pub use opendal_service_huggingface::*;
    #[cfg(feature = "services-ipfs")]
    pub use opendal_service_ipfs::*;
    #[cfg(feature = "services-ipmfs")]
    pub use opendal_service_ipmfs::*;
    #[cfg(feature = "services-koofr")]
    pub use opendal_service_koofr::*;
    #[cfg(feature = "services-lakefs")]
    pub use opendal_service_lakefs::*;
    #[cfg(feature = "services-memcached")]
    pub use opendal_service_memcached::*;
    #[cfg(feature = "services-mini-moka")]
    pub use opendal_service_mini_moka::*;
    #[cfg(feature = "services-moka")]
    pub use opendal_service_moka::*;
    #[cfg(feature = "services-mongodb")]
    pub use opendal_service_mongodb::*;
    #[cfg(feature = "services-monoiofs")]
    pub use opendal_service_monoiofs::*;
    #[cfg(feature = "services-mysql")]
    pub use opendal_service_mysql::*;
    #[cfg(feature = "services-obs")]
    pub use opendal_service_obs::*;
    #[cfg(feature = "services-onedrive")]
    pub use opendal_service_onedrive::*;
    #[cfg(all(target_arch = "wasm32", feature = "services-opfs"))]
    pub use opendal_service_opfs::*;
    #[cfg(feature = "services-oss")]
    pub use opendal_service_oss::*;
    #[cfg(feature = "services-pcloud")]
    pub use opendal_service_pcloud::*;
    #[cfg(feature = "services-persy")]
    pub use opendal_service_persy::*;
    #[cfg(feature = "services-postgresql")]
    pub use opendal_service_postgresql::*;
    #[cfg(feature = "services-redb")]
    pub use opendal_service_redb::*;
    #[cfg(feature = "services-redis")]
    pub use opendal_service_redis::*;
    #[cfg(feature = "services-rocksdb")]
    pub use opendal_service_rocksdb::*;
    #[cfg(feature = "services-s3")]
    pub use opendal_service_s3::*;
    #[cfg(feature = "services-seafile")]
    pub use opendal_service_seafile::*;
    #[cfg(feature = "services-sftp")]
    pub use opendal_service_sftp::*;
    #[cfg(feature = "services-sled")]
    pub use opendal_service_sled::*;
    #[cfg(feature = "services-sqlite")]
    pub use opendal_service_sqlite::*;
    #[cfg(feature = "services-surrealdb")]
    pub use opendal_service_surrealdb::*;
    #[cfg(feature = "services-swift")]
    pub use opendal_service_swift::*;
    #[cfg(feature = "services-tikv")]
    pub use opendal_service_tikv::*;
    #[cfg(feature = "services-upyun")]
    pub use opendal_service_upyun::*;
    #[cfg(feature = "services-vercel-artifacts")]
    pub use opendal_service_vercel_artifacts::*;
    #[cfg(feature = "services-vercel-blob")]
    pub use opendal_service_vercel_blob::*;
    #[cfg(feature = "services-webdav")]
    pub use opendal_service_webdav::*;
    #[cfg(feature = "services-webhdfs")]
    pub use opendal_service_webhdfs::*;
    #[cfg(feature = "services-yandex-disk")]
    pub use opendal_service_yandex_disk::*;
}

/// Re-export of layers.
pub mod layers {
    pub use opendal_core::layers::*;
    #[cfg(feature = "layers-async-backtrace")]
    pub use opendal_layer_async_backtrace::*;
    #[cfg(feature = "layers-await-tree")]
    pub use opendal_layer_await_tree::*;
    #[cfg(feature = "layers-capability-check")]
    pub use opendal_layer_capability_check::*;
    #[cfg(feature = "layers-chaos")]
    pub use opendal_layer_chaos::*;
    #[cfg(feature = "layers-concurrent-limit")]
    pub use opendal_layer_concurrent_limit::*;
    #[cfg(all(target_os = "linux", feature = "layers-dtrace"))]
    pub use opendal_layer_dtrace::*;
    #[cfg(feature = "layers-fastmetrics")]
    pub use opendal_layer_fastmetrics::*;
    #[cfg(feature = "layers-fastrace")]
    pub use opendal_layer_fastrace::*;
    #[cfg(feature = "layers-hotpath")]
    pub use opendal_layer_hotpath::*;
    #[cfg(feature = "layers-immutable-index")]
    pub use opendal_layer_immutable_index::*;
    #[cfg(feature = "layers-logging")]
    pub use opendal_layer_logging::*;
    #[cfg(feature = "layers-metrics")]
    pub use opendal_layer_metrics::*;
    #[cfg(feature = "layers-mime-guess")]
    pub use opendal_layer_mime_guess::*;
    #[cfg(feature = "layers-otel-metrics")]
    pub use opendal_layer_otelmetrics::*;
    #[cfg(feature = "layers-otel-trace")]
    pub use opendal_layer_oteltrace::*;
    #[cfg(feature = "layers-prometheus")]
    pub use opendal_layer_prometheus::*;
    #[cfg(feature = "layers-prometheus-client")]
    pub use opendal_layer_prometheus_client::*;
    #[cfg(feature = "layers-retry")]
    pub use opendal_layer_retry::*;
    #[cfg(feature = "layers-tail-cut")]
    pub use opendal_layer_tail_cut::*;
    #[cfg(feature = "layers-throttle")]
    pub use opendal_layer_throttle::*;
    #[cfg(feature = "layers-timeout")]
    pub use opendal_layer_timeout::*;
    #[cfg(feature = "layers-tracing")]
    pub use opendal_layer_tracing::*;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_uri_works_for_memory_by_default() -> Result<(), Error> {
        let op = Operator::from_uri("memory:///")?;
        assert_eq!(op.info().scheme(), "memory");
        Ok(())
    }
}
