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
    #[cfg(feature = "layers-fastmetrics")]
    pub use opendal_layer_fastmetrics::*;
    #[cfg(feature = "layers-fastrace")]
    pub use opendal_layer_fastrace::*;
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
