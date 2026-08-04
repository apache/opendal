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

pub use backend::S3Builder as S3;
pub use config::S3Config;
pub use minio::MinioBuilder as Minio;
pub use minio_config::MinioConfig;
pub use r2::R2Builder as R2;
pub use r2_config::R2Config;

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
