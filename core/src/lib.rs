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

#![doc(html_logo_url = "https://opendal.apache.org/img/logo.svg")]
#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

pub use opendal_core::*;

/// Runtime executor implementations.
pub mod executors {
    #[cfg(all(feature = "executors-compio", not(target_arch = "wasm32")))]
    pub use opendal_executor_compio::CompioExecutor;
    #[cfg(feature = "executors-tokio")]
    pub use opendal_executor_tokio::TokioExecutor;
}

#[cfg(any(
    feature = "http-transport-cyper-native-tls",
    feature = "http-transport-cyper-rustls"
))]
pub use opendal_http_transport_cyper::CyperTransport;

#[cfg(feature = "tests")]
pub extern crate opendal_testkit as tests;

/// Install the global defaults provided by the facade crate.
///
/// This function is safe to call multiple times. It registers enabled services
/// and installs the default executor and HTTP transport when their features are
/// enabled.
///
/// # Process-wide defaults
///
/// When `auto-register-services` is enabled, a process constructor calls this
/// function before `main`. The first installed HTTP transport and executor
/// remain the process-wide defaults.
///
/// Applications that need to install their own process-wide transport should
/// disable `auto-register-services`, call [`init_default_registry`] explicitly
/// when URI-based construction is needed, and install the transport themselves.
/// If `auto-register-services` is disabled, call this function to install the
/// facade defaults, or configure your own defaults with
/// [`HttpTransporter::install_default`] and [`Executor::install_default`].
/// Use [`OperationContext`] to configure runtime resources for one operator.
pub fn install_default() {
    init_default_registry();

    #[cfg(feature = "executors-tokio")]
    opendal_executor_tokio::install_default();

    #[cfg(any(
        feature = "http-transport-reqwest",
        feature = "http-transport-reqwest-native-tls",
        feature = "http-transport-reqwest-rustls",
        feature = "http-transport-reqwest-rustls-no-provider"
    ))]
    opendal_http_transport_reqwest::install_default();
}

#[cfg(all(
    test,
    feature = "auto-register-services",
    feature = "executors-tokio",
    not(target_arch = "wasm32")
))]
mod default_executor_tests {
    #[tokio::test]
    async fn default_executor_runs_concurrent_reads() -> crate::Result<()> {
        let op = crate::Operator::via_iter("memory", [])?;
        op.write("file", "abcdefgh").await?;
        let content = op
            .read_with("file")
            .range(0..8)
            .chunk(2)
            .concurrent(2)
            .await?;
        assert_eq!(content.to_bytes(), "abcdefgh");
        Ok(())
    }
}

/// Initialize the global [`OperatorRegistry`] with enabled services.
///
/// This function is safe to call multiple times and will only perform
/// initialization once.
///
/// # Notes
///
/// Some bindings link `opendal` as a `staticlib`, where `#[ctor::ctor]`
/// initializers may not be executed due to linker behavior. Those bindings
/// should call [`install_default`] explicitly before using `Operator::from_uri`
/// or `Operator::via_iter`.
pub fn init_default_registry() {
    static DEFAULT_REGISTRY_INIT: std::sync::Once = std::sync::Once::new();
    DEFAULT_REGISTRY_INIT.call_once(|| init_default_registry_inner(OperatorRegistry::get()));
}

fn init_default_registry_inner(registry: &OperatorRegistry) {
    opendal_core::services::register_memory_service(registry);

    #[cfg(feature = "services-aliyun-drive")]
    opendal_service_aliyun_drive::register_aliyun_drive_service(registry);

    #[cfg(feature = "services-alluxio")]
    opendal_service_alluxio::register_alluxio_service(registry);

    #[cfg(feature = "services-azblob")]
    opendal_service_azblob::register_azblob_service(registry);

    #[cfg(feature = "services-azdls")]
    opendal_service_azdls::register_azdls_service(registry);

    #[cfg(feature = "services-azfile")]
    opendal_service_azfile::register_azfile_service(registry);

    #[cfg(feature = "services-b2")]
    opendal_service_b2::register_b2_service(registry);

    #[cfg(feature = "services-b2-s3")]
    opendal_service_s3::register_b2_s3_service(registry);

    #[cfg(feature = "services-cacache")]
    opendal_service_cacache::register_cacache_service(registry);

    #[cfg(feature = "services-cloudflare-kv")]
    opendal_service_cloudflare_kv::register_cloudflare_kv_service(registry);

    #[cfg(feature = "services-compfs")]
    opendal_service_compfs::register_compfs_service(registry);

    #[cfg(feature = "services-cos")]
    opendal_service_cos::register_cos_service(registry);

    #[cfg(feature = "services-cos-s3")]
    opendal_service_s3::register_cos_s3_service(registry);

    #[cfg(feature = "services-d1")]
    opendal_service_d1::register_d1_service(registry);

    #[cfg(feature = "services-dashmap")]
    opendal_service_dashmap::register_dashmap_service(registry);

    #[cfg(feature = "services-dbfs")]
    opendal_service_dbfs::register_dbfs_service(registry);

    #[cfg(feature = "services-dropbox")]
    opendal_service_dropbox::register_dropbox_service(registry);

    #[cfg(feature = "services-etcd")]
    opendal_service_etcd::register_etcd_service(registry);

    #[cfg(feature = "services-foundationdb")]
    opendal_service_foundationdb::register_foundationdb_service(registry);

    #[cfg(feature = "services-foyer")]
    opendal_service_foyer::register_foyer_service(registry);

    #[cfg(feature = "services-fs")]
    opendal_service_fs::register_fs_service(registry);

    #[cfg(feature = "services-ftp")]
    opendal_service_ftp::register_ftp_service(registry);

    #[cfg(feature = "services-gcs")]
    opendal_service_gcs::register_gcs_service(registry);

    #[cfg(feature = "services-gcs-s3")]
    opendal_service_s3::register_gcs_s3_service(registry);

    #[cfg(feature = "services-gcs-grpc")]
    opendal_service_gcs_grpc::register_gcs_grpc_service(registry);

    #[cfg(feature = "services-gdrive")]
    opendal_service_gdrive::register_gdrive_service(registry);

    #[cfg(feature = "services-ghac")]
    opendal_service_ghac::register_ghac_service(registry);

    #[cfg(feature = "services-git")]
    opendal_service_git::register_git_service(registry);

    #[cfg(feature = "services-github")]
    opendal_service_github::register_github_service(registry);

    #[cfg(feature = "services-goosefs")]
    opendal_service_goosefs::register_goosefs_service(registry);

    #[cfg(feature = "services-gridfs")]
    opendal_service_gridfs::register_gridfs_service(registry);

    #[cfg(feature = "services-hdfs")]
    opendal_service_hdfs::register_hdfs_service(registry);

    #[cfg(feature = "services-hdfs-native")]
    opendal_service_hdfs_native::register_hdfs_native_service(registry);

    #[cfg(feature = "services-http")]
    opendal_service_http::register_http_service(registry);

    #[cfg(feature = "services-hf")]
    opendal_service_hf::register_hf_service(registry);

    #[cfg(all(feature = "services-hf3fs", target_os = "linux"))]
    opendal_service_hf3fs::register_hf3fs_service(registry);

    #[cfg(feature = "services-ipfs")]
    opendal_service_ipfs::register_ipfs_service(registry);

    #[cfg(feature = "services-ipmfs")]
    opendal_service_ipmfs::register_ipmfs_service(registry);

    #[cfg(feature = "services-koofr")]
    opendal_service_koofr::register_koofr_service(registry);

    #[cfg(feature = "services-lakefs")]
    opendal_service_lakefs::register_lakefs_service(registry);

    #[cfg(feature = "services-lakefs-s3")]
    opendal_service_s3::register_lakefs_s3_service(registry);

    #[cfg(feature = "services-memcached")]
    opendal_service_memcached::register_memcached_service(registry);

    #[cfg(feature = "services-mini-moka")]
    opendal_service_mini_moka::register_mini_moka_service(registry);

    #[cfg(feature = "services-moka")]
    opendal_service_moka::register_moka_service(registry);

    #[cfg(feature = "services-mongodb")]
    opendal_service_mongodb::register_mongodb_service(registry);

    #[cfg(feature = "services-monoiofs")]
    opendal_service_monoiofs::register_monoiofs_service(registry);

    #[cfg(feature = "services-mysql")]
    opendal_service_mysql::register_mysql_service(registry);

    #[cfg(feature = "services-obs")]
    opendal_service_obs::register_obs_service(registry);

    #[cfg(feature = "services-obs-s3")]
    opendal_service_s3::register_obs_s3_service(registry);

    #[cfg(feature = "services-onedrive")]
    opendal_service_onedrive::register_onedrive_service(registry);

    #[cfg(feature = "services-oss")]
    opendal_service_oss::register_oss_service(registry);

    #[cfg(feature = "services-oss-s3")]
    opendal_service_s3::register_oss_s3_service(registry);

    #[cfg(feature = "services-pcloud")]
    opendal_service_pcloud::register_pcloud_service(registry);

    #[cfg(feature = "services-persy")]
    opendal_service_persy::register_persy_service(registry);

    #[cfg(feature = "services-postgresql")]
    opendal_service_postgresql::register_postgresql_service(registry);

    #[cfg(feature = "services-redb")]
    opendal_service_redb::register_redb_service(registry);

    #[cfg(feature = "services-rocksdb")]
    opendal_service_rocksdb::register_rocksdb_service(registry);

    #[cfg(feature = "services-s3")]
    opendal_service_s3::register_s3_service(registry);

    #[cfg(feature = "services-r2")]
    opendal_service_s3::register_r2_service(registry);

    #[cfg(feature = "services-minio")]
    opendal_service_s3::register_minio_service(registry);

    #[cfg(feature = "services-akamai")]
    opendal_service_s3::register_akamai_service(registry);

    #[cfg(feature = "services-ceph-rgw")]
    opendal_service_s3::register_ceph_rgw_service(registry);

    #[cfg(feature = "services-exoscale")]
    opendal_service_s3::register_exoscale_service(registry);

    #[cfg(feature = "services-garage")]
    opendal_service_s3::register_garage_service(registry);

    #[cfg(feature = "services-hetzner")]
    opendal_service_s3::register_hetzner_service(registry);

    #[cfg(feature = "services-ibm-cos")]
    opendal_service_s3::register_ibm_cos_service(registry);

    #[cfg(feature = "services-idrive-e2")]
    opendal_service_s3::register_idrive_e2_service(registry);

    #[cfg(feature = "services-ionos")]
    opendal_service_s3::register_ionos_service(registry);

    #[cfg(feature = "services-neon")]
    opendal_service_s3::register_neon_service(registry);

    #[cfg(feature = "services-oci")]
    opendal_service_s3::register_oci_service(registry);

    #[cfg(feature = "services-ovhcloud")]
    opendal_service_s3::register_ovhcloud_service(registry);

    #[cfg(feature = "services-qingstor")]
    opendal_service_s3::register_qingstor_service(registry);

    #[cfg(feature = "services-rustfs")]
    opendal_service_s3::register_rustfs_service(registry);

    #[cfg(feature = "services-scaleway")]
    opendal_service_s3::register_scaleway_service(registry);

    #[cfg(feature = "services-seaweedfs")]
    opendal_service_s3::register_seaweedfs_service(registry);

    #[cfg(feature = "services-spaces")]
    opendal_service_s3::register_spaces_service(registry);

    #[cfg(feature = "services-storj")]
    opendal_service_s3::register_storj_service(registry);

    #[cfg(feature = "services-supabase")]
    opendal_service_s3::register_supabase_service(registry);

    #[cfg(feature = "services-tigris")]
    opendal_service_s3::register_tigris_service(registry);

    #[cfg(feature = "services-wasabi")]
    opendal_service_s3::register_wasabi_service(registry);

    #[cfg(feature = "services-seafile")]
    opendal_service_seafile::register_seafile_service(registry);

    #[cfg(feature = "services-sftp")]
    opendal_service_sftp::register_sftp_service(registry);

    #[cfg(feature = "services-sled")]
    opendal_service_sled::register_sled_service(registry);

    #[cfg(feature = "services-sqlite")]
    opendal_service_sqlite::register_sqlite_service(registry);

    #[cfg(feature = "services-surrealdb")]
    opendal_service_surrealdb::register_surrealdb_service(registry);

    #[cfg(feature = "services-swift")]
    opendal_service_swift::register_swift_service(registry);

    #[cfg(feature = "services-swift-s3api")]
    opendal_service_s3::register_swift_s3api_service(registry);

    #[cfg(feature = "services-tikv")]
    opendal_service_tikv::register_tikv_service(registry);

    #[cfg(feature = "services-tos")]
    opendal_service_tos::register_tos_service(registry);

    #[cfg(feature = "services-tos-s3")]
    opendal_service_s3::register_tos_s3_service(registry);

    #[cfg(feature = "services-upyun")]
    opendal_service_upyun::register_upyun_service(registry);

    #[cfg(feature = "services-vercel-artifacts")]
    opendal_service_vercel_artifacts::register_vercel_artifacts_service(registry);

    #[cfg(feature = "services-vercel-blob")]
    opendal_service_vercel_blob::register_vercel_blob_service(registry);

    #[cfg(feature = "services-webdav")]
    opendal_service_webdav::register_webdav_service(registry);

    #[cfg(feature = "services-webhdfs")]
    opendal_service_webhdfs::register_webhdfs_service(registry);

    #[cfg(feature = "services-yandex-disk")]
    opendal_service_yandex_disk::register_yandex_disk_service(registry);

    #[cfg(all(target_arch = "wasm32", feature = "services-opfs"))]
    opendal_service_opfs::register_opfs_service(registry);

    #[cfg(any(feature = "services-redis", feature = "services-redis-native-tls"))]
    opendal_service_redis::register_redis_service(registry);
}

#[cfg(feature = "auto-register-services")]
#[ctor::ctor(unsafe)]
fn register_default_operator_registry() {
    install_default();
}

/// Service builders enabled through `services-*` Cargo features.
///
/// [`Memory`](services::Memory) is always available. Other builders are
/// re-exported when their matching facade feature is enabled; for example,
/// `services-r2` enables the `R2` builder.
///
/// Pass a configured builder to [`Operator::new`]. The facade automatically
/// registers enabled services for [`Operator::from_uri`] and
/// [`Operator::via_iter`] when the `auto-register-services` feature is enabled.
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
    #[cfg(feature = "services-foyer")]
    pub use opendal_service_foyer::*;
    #[cfg(feature = "services-fs")]
    pub use opendal_service_fs::*;
    #[cfg(feature = "services-ftp")]
    pub use opendal_service_ftp::*;
    #[cfg(feature = "services-gcs")]
    pub use opendal_service_gcs::*;
    #[cfg(feature = "services-gcs-grpc")]
    pub use opendal_service_gcs_grpc::*;
    #[cfg(feature = "services-gdrive")]
    pub use opendal_service_gdrive::*;
    #[cfg(feature = "services-ghac")]
    pub use opendal_service_ghac::*;
    #[cfg(feature = "services-git")]
    pub use opendal_service_git::*;
    #[cfg(feature = "services-github")]
    pub use opendal_service_github::*;
    #[cfg(feature = "services-goosefs")]
    pub use opendal_service_goosefs::*;
    #[cfg(feature = "services-gridfs")]
    pub use opendal_service_gridfs::*;
    #[cfg(feature = "services-hdfs")]
    pub use opendal_service_hdfs::*;
    #[cfg(feature = "services-hdfs-native")]
    pub use opendal_service_hdfs_native::*;
    #[cfg(feature = "services-hf")]
    pub use opendal_service_hf::*;
    #[cfg(all(feature = "services-hf3fs", target_os = "linux"))]
    pub use opendal_service_hf3fs::*;
    #[cfg(feature = "services-http")]
    pub use opendal_service_http::*;
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
    #[cfg(feature = "services-akamai")]
    pub use opendal_service_s3::{AKAMAI_SCHEME, Akamai, AkamaiConfig, register_akamai_service};
    #[cfg(feature = "services-b2-s3")]
    pub use opendal_service_s3::{B2_S3_SCHEME, B2S3, B2S3Config, register_b2_s3_service};
    #[cfg(feature = "services-ceph-rgw")]
    pub use opendal_service_s3::{
        CEPH_RGW_SCHEME, CephRgw, CephRgwConfig, register_ceph_rgw_service,
    };
    #[cfg(feature = "services-cos-s3")]
    pub use opendal_service_s3::{COS_S3_SCHEME, CosS3, CosS3Config, register_cos_s3_service};
    #[cfg(feature = "services-exoscale")]
    pub use opendal_service_s3::{
        EXOSCALE_SCHEME, Exoscale, ExoscaleConfig, register_exoscale_service,
    };
    #[cfg(feature = "services-garage")]
    pub use opendal_service_s3::{GARAGE_SCHEME, Garage, GarageConfig, register_garage_service};
    #[cfg(feature = "services-gcs-s3")]
    pub use opendal_service_s3::{GCS_S3_SCHEME, GcsS3, GcsS3Config, register_gcs_s3_service};
    #[cfg(feature = "services-hetzner")]
    pub use opendal_service_s3::{
        HETZNER_SCHEME, Hetzner, HetznerConfig, register_hetzner_service,
    };
    #[cfg(feature = "services-ibm-cos")]
    pub use opendal_service_s3::{IBM_COS_SCHEME, IbmCos, IbmCosConfig, register_ibm_cos_service};
    #[cfg(feature = "services-idrive-e2")]
    pub use opendal_service_s3::{
        IDRIVE_E2_SCHEME, IdriveE2, IdriveE2Config, register_idrive_e2_service,
    };
    #[cfg(feature = "services-ionos")]
    pub use opendal_service_s3::{IONOS_SCHEME, Ionos, IonosConfig, register_ionos_service};
    #[cfg(feature = "services-lakefs-s3")]
    pub use opendal_service_s3::{
        LAKEFS_S3_SCHEME, LakefsS3, LakefsS3Config, register_lakefs_s3_service,
    };
    #[cfg(feature = "services-minio")]
    pub use opendal_service_s3::{MINIO_SCHEME, Minio, MinioConfig, register_minio_service};
    #[cfg(feature = "services-neon")]
    pub use opendal_service_s3::{NEON_SCHEME, Neon, NeonConfig, register_neon_service};
    #[cfg(feature = "services-obs-s3")]
    pub use opendal_service_s3::{OBS_S3_SCHEME, ObsS3, ObsS3Config, register_obs_s3_service};
    #[cfg(feature = "services-oci")]
    pub use opendal_service_s3::{OCI_SCHEME, Oci, OciConfig, register_oci_service};
    #[cfg(feature = "services-oss-s3")]
    pub use opendal_service_s3::{OSS_S3_SCHEME, OssS3, OssS3Config, register_oss_s3_service};
    #[cfg(feature = "services-ovhcloud")]
    pub use opendal_service_s3::{
        OVHCLOUD_SCHEME, Ovhcloud, OvhcloudConfig, register_ovhcloud_service,
    };
    #[cfg(feature = "services-qingstor")]
    pub use opendal_service_s3::{
        QINGSTOR_SCHEME, Qingstor, QingstorConfig, register_qingstor_service,
    };
    #[cfg(feature = "services-r2")]
    pub use opendal_service_s3::{R2, R2_SCHEME, R2Config, register_r2_service};
    #[cfg(feature = "services-rustfs")]
    pub use opendal_service_s3::{RUSTFS_SCHEME, Rustfs, RustfsConfig, register_rustfs_service};
    #[cfg(feature = "services-s3")]
    pub use opendal_service_s3::{S3, S3_SCHEME, S3Config, register_s3_service, reqsign_aws_v4};
    #[cfg(feature = "services-scaleway")]
    pub use opendal_service_s3::{
        SCALEWAY_SCHEME, Scaleway, ScalewayConfig, register_scaleway_service,
    };
    #[cfg(feature = "services-seaweedfs")]
    pub use opendal_service_s3::{
        SEAWEEDFS_SCHEME, Seaweedfs, SeaweedfsConfig, register_seaweedfs_service,
    };
    #[cfg(feature = "services-spaces")]
    pub use opendal_service_s3::{SPACES_SCHEME, Spaces, SpacesConfig, register_spaces_service};
    #[cfg(feature = "services-storj")]
    pub use opendal_service_s3::{STORJ_SCHEME, Storj, StorjConfig, register_storj_service};
    #[cfg(feature = "services-supabase")]
    pub use opendal_service_s3::{
        SUPABASE_SCHEME, Supabase, SupabaseConfig, register_supabase_service,
    };
    #[cfg(feature = "services-swift-s3api")]
    pub use opendal_service_s3::{
        SWIFT_S3API_SCHEME, SwiftS3api, SwiftS3apiConfig, register_swift_s3api_service,
    };
    #[cfg(feature = "services-tigris")]
    pub use opendal_service_s3::{TIGRIS_SCHEME, Tigris, TigrisConfig, register_tigris_service};
    #[cfg(feature = "services-tos-s3")]
    pub use opendal_service_s3::{TOS_S3_SCHEME, TosS3, TosS3Config, register_tos_s3_service};
    #[cfg(feature = "services-wasabi")]
    pub use opendal_service_s3::{WASABI_SCHEME, Wasabi, WasabiConfig, register_wasabi_service};
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
    #[cfg(feature = "services-tos")]
    pub use opendal_service_tos::*;
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

#[cfg(test)]
mod service_feature_tests {
    use super::*;

    #[test]
    fn registers_enabled_s3_provider_schemes() {
        init_default_registry();
        let schemes = OperatorRegistry::get().schemes();

        assert!(schemes.contains("memory"));

        #[cfg(feature = "services-s3")]
        {
            let chain =
                reqsign_core::ProvideCredentialChain::<services::reqsign_aws_v4::Credential>::new();
            let _ = services::S3::default().credential_provider_chain(chain);
            assert!(schemes.contains("s3"));
        }
        #[cfg(not(feature = "services-s3"))]
        assert!(!schemes.contains("s3"));

        #[cfg(feature = "services-r2")]
        {
            let _ = services::R2::default();
            assert!(schemes.contains("r2"));
        }
        #[cfg(not(feature = "services-r2"))]
        assert!(!schemes.contains("r2"));

        #[cfg(feature = "services-minio")]
        {
            let _ = services::Minio::default();
            assert!(schemes.contains("minio"));
        }
        #[cfg(not(feature = "services-minio"))]
        assert!(!schemes.contains("minio"));

        for (scheme, enabled) in [
            ("akamai", cfg!(feature = "services-akamai")),
            ("ceph-rgw", cfg!(feature = "services-ceph-rgw")),
            ("exoscale", cfg!(feature = "services-exoscale")),
            ("garage", cfg!(feature = "services-garage")),
            ("hetzner", cfg!(feature = "services-hetzner")),
            ("ibm-cos", cfg!(feature = "services-ibm-cos")),
            ("idrive-e2", cfg!(feature = "services-idrive-e2")),
            ("ionos", cfg!(feature = "services-ionos")),
            ("neon", cfg!(feature = "services-neon")),
            ("oci", cfg!(feature = "services-oci")),
            ("ovhcloud", cfg!(feature = "services-ovhcloud")),
            ("qingstor", cfg!(feature = "services-qingstor")),
            ("rustfs", cfg!(feature = "services-rustfs")),
            ("scaleway", cfg!(feature = "services-scaleway")),
            ("seaweedfs", cfg!(feature = "services-seaweedfs")),
            ("spaces", cfg!(feature = "services-spaces")),
            ("storj", cfg!(feature = "services-storj")),
            ("supabase", cfg!(feature = "services-supabase")),
            ("tigris", cfg!(feature = "services-tigris")),
            ("wasabi", cfg!(feature = "services-wasabi")),
            ("b2-s3", cfg!(feature = "services-b2-s3")),
            ("cos-s3", cfg!(feature = "services-cos-s3")),
            ("gcs-s3", cfg!(feature = "services-gcs-s3")),
            ("lakefs-s3", cfg!(feature = "services-lakefs-s3")),
            ("obs-s3", cfg!(feature = "services-obs-s3")),
            ("oss-s3", cfg!(feature = "services-oss-s3")),
            ("swift-s3api", cfg!(feature = "services-swift-s3api")),
            ("tos-s3", cfg!(feature = "services-tos-s3")),
            ("b2", cfg!(feature = "services-b2")),
            ("cos", cfg!(feature = "services-cos")),
            ("cosn", cfg!(feature = "services-cos")),
            ("gcs", cfg!(feature = "services-gcs")),
            ("gcs-grpc", cfg!(feature = "services-gcs-grpc")),
            ("lakefs", cfg!(feature = "services-lakefs")),
            ("obs", cfg!(feature = "services-obs")),
            ("oss", cfg!(feature = "services-oss")),
            ("swift", cfg!(feature = "services-swift")),
            ("tos", cfg!(feature = "services-tos")),
        ] {
            assert_eq!(schemes.contains(scheme), enabled, "{scheme}");
        }

        #[cfg(feature = "services-obs")]
        {
            let op = Operator::from_uri((
                "obs://example-bucket/root",
                [("endpoint", "https://obs.cn-north-1.myhuaweicloud.com")],
            ))
            .unwrap();
            assert_eq!(op.info().scheme(), "obs");
            assert_eq!(op.info().name(), "example-bucket");
            assert_eq!(op.info().root(), "/root/");
        }

        #[cfg(feature = "services-obs-s3")]
        {
            let op = Operator::from_uri("obs-s3://example-bucket/root?region=cn-north-1").unwrap();
            assert_eq!(op.info().scheme(), "obs-s3");
            assert_eq!(op.info().name(), "example-bucket");
            assert_eq!(op.info().root(), "/root/");

            let op = Operator::via_iter(
                "obs-s3",
                [
                    ("bucket".to_owned(), "example-bucket".to_owned()),
                    ("region".to_owned(), "cn-north-1".to_owned()),
                ],
            )
            .unwrap();
            assert_eq!(op.info().scheme(), "obs-s3");
            assert_eq!(op.info().name(), "example-bucket");
            assert_eq!(op.info().root(), "/");
        }

        #[cfg(feature = "services-tigris")]
        {
            let op = Operator::from_uri("tigris://example-bucket/root").unwrap();
            assert_eq!(op.info().scheme(), "tigris");
            assert_eq!(op.info().name(), "example-bucket");
            assert_eq!(op.info().root(), "/root/");
        }
    }

    #[cfg(all(
        feature = "services-b2",
        feature = "services-cos",
        feature = "services-gcs",
        feature = "services-lakefs",
        feature = "services-oss",
        feature = "services-swift",
        feature = "services-tos",
    ))]
    #[test]
    fn native_uris_still_use_native_builders() -> Result<()> {
        init_default_registry();

        type Options<'a> = &'a [(&'a str, &'a str)];
        let cases: &[(&str, Options<'_>, &str)] = &[
            (
                "b2://example-bucket/root",
                &[
                    ("bucket_id", "bucket-id"),
                    ("application_key_id", "app-id"),
                    ("application_key", "app-key"),
                ],
                "b2",
            ),
            (
                "cos://example-bucket-1250000000/root",
                &[("endpoint", "https://cos.ap-guangzhou.myqcloud.com")],
                "cos",
            ),
            (
                "cosn://example-bucket-1250000000/root",
                &[("endpoint", "https://cos.ap-guangzhou.myqcloud.com")],
                "cos",
            ),
            ("gcs://example-bucket/root", &[], "gcs"),
            (
                "lakefs://lakefs.example.com/repo/dev/root",
                &[("username", "test-user"), ("password", "test-secret")],
                "lakefs",
            ),
            (
                "oss://example-bucket/root",
                &[("endpoint", "https://oss-cn-hangzhou.aliyuncs.com")],
                "oss",
            ),
            (
                "swift://swift.example.com/container/root",
                &[("token", "swift-token")],
                "swift",
            ),
            ("tos://example-bucket/root", &[], "tos"),
        ];
        for &(uri, options, scheme) in cases {
            let op = Operator::from_uri((uri, options.iter().copied()))?;
            assert_eq!(op.info().scheme(), scheme, "{uri}");
        }
        #[cfg(feature = "services-gcs-grpc")]
        {
            let op = Operator::from_uri("gcs-grpc://example-bucket/root")?;
            assert_eq!(op.info().scheme(), "gcs-grpc");
        }
        Ok(())
    }

    #[cfg(feature = "services-garage")]
    #[test]
    fn garage_builds_from_uri_iter_and_typed_config() {
        use crate::services::{GARAGE_SCHEME, Garage, GarageConfig};

        let options = [("endpoint", "http://127.0.0.1:3900"), ("region", "garage")];
        let from_uri = Operator::from_uri(("garage://example-bucket/root", options)).unwrap();
        let from_iter = Operator::via_iter(
            GARAGE_SCHEME,
            options
                .into_iter()
                .chain([("bucket", "example-bucket"), ("root", "root")])
                .map(|(key, value)| (key.to_owned(), value.to_owned())),
        )
        .unwrap();
        let mut config = GarageConfig::default();
        config.bucket = "example-bucket".into();
        config.root = Some("root".into());
        config.endpoint = "http://127.0.0.1:3900".into();
        config.region = "garage".into();
        let from_config = Operator::from_config(config).unwrap();
        let from_builder = Operator::new(
            Garage::default()
                .bucket("example-bucket")
                .root("root")
                .endpoint("http://127.0.0.1:3900")
                .region("garage"),
        )
        .unwrap();

        for op in [from_uri, from_iter, from_config, from_builder] {
            assert_eq!(op.info().scheme(), GARAGE_SCHEME);
            assert_eq!(op.info().name(), "example-bucket");
            assert_eq!(op.info().root(), "/root/");
        }
    }
}

/// Layers enabled through `layers-*` Cargo features.
///
/// A layer adds cross-service behavior to an [`Operator`]. Enable the matching
/// facade feature, construct the layer, and pass it to [`Operator::layer`].
/// Layer order is observable, so review each layer's composition notes when
/// combining retries, timeouts, caching, or observability.
pub mod layers {
    pub use opendal_core::layers::*;
    #[cfg(feature = "layers-aimd")]
    pub use opendal_layer_aimd::*;
    #[cfg(feature = "layers-async-backtrace")]
    pub use opendal_layer_async_backtrace::*;
    #[cfg(feature = "layers-await-tree")]
    pub use opendal_layer_await_tree::*;
    #[cfg(feature = "layers-cache")]
    pub use opendal_layer_cache::{CacheDecision, CacheLayer, CachePolicy, CacheReadRequest};
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
    #[cfg(feature = "layers-foyer")]
    pub use opendal_layer_foyer::*;
    #[cfg(feature = "layers-hedge")]
    pub use opendal_layer_hedge::*;
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
    #[cfg(feature = "layers-route")]
    pub use opendal_layer_route::*;
    #[cfg(feature = "layers-sanity-check")]
    pub use opendal_layer_sanity_check::*;
    #[cfg(feature = "layers-tail-cut")]
    pub use opendal_layer_tail_cut::*;
    #[cfg(feature = "layers-throttle")]
    pub use opendal_layer_throttle::*;
    #[cfg(feature = "layers-timeout")]
    pub use opendal_layer_timeout::*;
    #[cfg(feature = "layers-tracing")]
    pub use opendal_layer_tracing::*;
}
