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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

mod backend;
mod config;
mod copier;
mod core;
mod deleter;
mod lister;
mod minio;
mod minio_config;
mod preset;
mod r2;
mod r2_config;
mod reader;
mod writer;

mod akamai_config;
mod ceph_rgw_config;
mod exoscale_config;
mod garage_config;
mod hetzner_config;
mod ibm_cos_config;
mod idrive_e2_config;
mod ionos_config;
mod neon_config;
mod oci_config;
mod ovhcloud_config;
mod qingstor_config;
mod rustfs_config;
mod scaleway_config;
mod seaweedfs_config;
mod spaces_config;
mod storj_config;
mod supabase_config;
mod tigris_config;
mod wasabi_config;

#[cfg(test)]
mod preset_tests;

pub use akamai_config::{AKAMAI_SCHEME, Akamai, AkamaiConfig, register_akamai_service};
pub use backend::S3Builder as S3;
pub use ceph_rgw_config::{CEPH_RGW_SCHEME, CephRgw, CephRgwConfig, register_ceph_rgw_service};
pub use config::S3Config;
pub use exoscale_config::{EXOSCALE_SCHEME, Exoscale, ExoscaleConfig, register_exoscale_service};
pub use garage_config::{GARAGE_SCHEME, Garage, GarageConfig, register_garage_service};
pub use hetzner_config::{HETZNER_SCHEME, Hetzner, HetznerConfig, register_hetzner_service};
pub use ibm_cos_config::{IBM_COS_SCHEME, IbmCos, IbmCosConfig, register_ibm_cos_service};
pub use idrive_e2_config::{
    IDRIVE_E2_SCHEME, IdriveE2, IdriveE2Config, register_idrive_e2_service,
};
pub use ionos_config::{IONOS_SCHEME, Ionos, IonosConfig, register_ionos_service};
pub use minio::MinioBuilder as Minio;
pub use minio_config::MinioConfig;
pub use neon_config::{NEON_SCHEME, Neon, NeonConfig, register_neon_service};
pub use oci_config::{OCI_SCHEME, Oci, OciConfig, register_oci_service};
pub use ovhcloud_config::{OVHCLOUD_SCHEME, Ovhcloud, OvhcloudConfig, register_ovhcloud_service};
pub use qingstor_config::{QINGSTOR_SCHEME, Qingstor, QingstorConfig, register_qingstor_service};
pub use r2::R2Builder as R2;
pub use r2_config::R2Config;
pub use rustfs_config::{RUSTFS_SCHEME, Rustfs, RustfsConfig, register_rustfs_service};
pub use scaleway_config::{SCALEWAY_SCHEME, Scaleway, ScalewayConfig, register_scaleway_service};
pub use seaweedfs_config::{
    SEAWEEDFS_SCHEME, Seaweedfs, SeaweedfsConfig, register_seaweedfs_service,
};
pub use spaces_config::{SPACES_SCHEME, Spaces, SpacesConfig, register_spaces_service};
pub use storj_config::{STORJ_SCHEME, Storj, StorjConfig, register_storj_service};
pub use supabase_config::{SUPABASE_SCHEME, Supabase, SupabaseConfig, register_supabase_service};
pub use tigris_config::{TIGRIS_SCHEME, Tigris, TigrisConfig, register_tigris_service};
pub use wasabi_config::{WASABI_SCHEME, Wasabi, WasabiConfig, register_wasabi_service};

/// Re-export of the [`reqsign_aws_v4`] crate that this service signs requests
/// with.
///
/// [`S3::credential_provider_chain`] takes a chain of providers for
/// [`reqsign_aws_v4::Credential`]. Implement providers against this re-export and
/// [`opendal_core::reqsign_core`] rather than depending on the `reqsign` crates
/// directly, so that the types always match the ones this service is built with.
pub use reqsign_aws_v4;

/// URI scheme used for service registration and scheme-driven construction.
pub const S3_SCHEME: &str = "s3";
/// URI scheme for the Cloudflare R2 preset.
pub const R2_SCHEME: &str = "r2";
/// URI scheme for the MinIO preset.
pub const MINIO_SCHEME: &str = "minio";

/// Register the Amazon S3 URI scheme with an operator registry.
///
/// Registration enables scheme-driven construction through
/// [`opendal_core::Operator::from_uri`] and
/// [`opendal_core::Operator::via_iter`]. Direct construction through
/// [`opendal_core::Operator::new`] does not require registration.
pub fn register_s3_service(registry: &opendal_core::OperatorRegistry) {
    registry.register::<S3>(S3_SCHEME);
}

/// Register the Cloudflare R2 URI scheme with an operator registry.
///
/// Registration enables scheme-driven construction through
/// [`opendal_core::Operator::from_uri`] and
/// [`opendal_core::Operator::via_iter`]. Direct construction through
/// [`opendal_core::Operator::new`] does not require registration.
pub fn register_r2_service(registry: &opendal_core::OperatorRegistry) {
    registry.register::<R2>(R2_SCHEME);
}

/// Register the MinIO URI scheme with an operator registry.
///
/// Registration enables scheme-driven construction through
/// [`opendal_core::Operator::from_uri`] and
/// [`opendal_core::Operator::via_iter`]. Direct construction through
/// [`opendal_core::Operator::new`] does not require registration.
pub fn register_minio_service(registry: &opendal_core::OperatorRegistry) {
    registry.register::<Minio>(MINIO_SCHEME);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_s3_and_provider_schemes() {
        let registry = opendal_core::OperatorRegistry::get();
        register_s3_service(registry);
        register_r2_service(registry);
        register_minio_service(registry);

        let schemes = registry.schemes();
        assert!(schemes.contains(S3_SCHEME));
        assert!(schemes.contains(R2_SCHEME));
        assert!(schemes.contains(MINIO_SCHEME));

        let r2 = registry
            .load(("r2://bucket/root", [("account_id", "example-account")]))
            .unwrap();
        assert_eq!(r2.info().scheme(), R2_SCHEME);

        let minio = registry
            .load((
                "minio://bucket/root",
                [("endpoint", "http://127.0.0.1:9000")],
            ))
            .unwrap();
        assert_eq!(minio.info().scheme(), MINIO_SCHEME);
    }
}
