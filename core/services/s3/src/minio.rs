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

use std::fmt::Debug;

use opendal_core::Builder;
use opendal_core::Result;
use opendal_core::raw::Service;
use reqsign_aws_v4::Credential;
use reqsign_core::ProvideCredentialChain;

use crate::MINIO_SCHEME;
use crate::backend::S3Builder;
use crate::config::S3Config;
use crate::minio_config::MinioConfig;
use crate::preset;

/// Builds a MinIO service with provider-specific configuration.
#[doc = include_str!("minio.md")]
#[derive(Default)]
pub struct MinioBuilder {
    config: MinioConfig,
}

impl Debug for MinioBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MinioBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl MinioBuilder {
    pub(crate) fn from_config(config: MinioConfig) -> Self {
        Self { config }
    }

    /// Set the root within the bucket.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }

    /// Set the bucket name.
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();
        self
    }

    /// Set the MinIO endpoint.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = Some(endpoint.to_string());
        self
    }

    /// Set the signing region.
    pub fn region(mut self, region: &str) -> Self {
        self.config.region = Some(region.to_string());
        self
    }

    /// Set the access key ID.
    pub fn access_key_id(mut self, access_key_id: &str) -> Self {
        self.config.access_key_id = Some(access_key_id.to_string());
        self
    }

    /// Set the secret access key.
    pub fn secret_access_key(mut self, secret_access_key: &str) -> Self {
        self.config.secret_access_key = Some(secret_access_key.to_string());
        self
    }

    /// Set the session token for temporary credentials.
    pub fn session_token(mut self, session_token: &str) -> Self {
        self.config.session_token = Some(session_token.to_string());
        self
    }

    /// Send requests without signing them.
    pub fn skip_signature(mut self) -> Self {
        self.config.skip_signature = true;
        self
    }

    fn into_s3_config(self) -> Result<(S3Config, ProvideCredentialChain<Credential>)> {
        let MinioConfig {
            root,
            bucket,
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            session_token,
            skip_signature,
        } = self.config;

        preset::validate_required(&bucket, MINIO_SCHEME, "bucket must not be empty")?;
        preset::validate_optional(
            endpoint.as_deref(),
            MINIO_SCHEME,
            "endpoint must not be empty when set",
        )?;
        preset::validate_optional(
            region.as_deref(),
            MINIO_SCHEME,
            "region must not be empty when set",
        )?;

        let endpoint = endpoint
            .ok_or_else(|| preset::config_error(MINIO_SCHEME, "endpoint is required for MinIO"))?;

        if skip_signature
            && (access_key_id.is_some() || secret_access_key.is_some() || session_token.is_some())
        {
            return Err(preset::config_error(
                MINIO_SCHEME,
                "skip_signature cannot be combined with direct credentials",
            ));
        }

        preset::validate_credentials(
            access_key_id.as_deref(),
            secret_access_key.as_deref(),
            session_token.as_deref(),
            MINIO_SCHEME,
        )?;

        let credential_providers = preset::credential_chain(
            access_key_id.as_deref(),
            secret_access_key.as_deref(),
            session_token.as_deref(),
        );

        let config = S3Config {
            root,
            bucket,
            endpoint: Some(endpoint),
            region: Some(region.unwrap_or_else(|| "auto".to_string())),
            access_key_id,
            secret_access_key,
            session_token,
            disable_ec2_metadata: true,
            skip_signature,
            ..Default::default()
        };

        Ok((config, credential_providers))
    }
}

impl Builder for MinioBuilder {
    type Config = MinioConfig;

    fn build(self) -> Result<impl Service> {
        let (config, credential_providers) = self.into_s3_config()?;
        S3Builder::from_provider_config(config, credential_providers)
            .build_with_scheme(MINIO_SCHEME)
    }
}

#[cfg(test)]
mod tests {
    use opendal_core::ErrorKind;
    use opendal_core::Operator;

    use super::*;

    fn build_config(builder: MinioBuilder) -> S3Config {
        builder.into_s3_config().unwrap().0
    }

    #[test]
    fn defaults_region_to_auto() {
        let config = build_config(
            MinioBuilder::default()
                .bucket("bucket")
                .endpoint("http://127.0.0.1:9000"),
        );
        assert_eq!(config.region.as_deref(), Some("auto"));
    }

    #[test]
    fn accepts_explicit_region() {
        let config = build_config(
            MinioBuilder::default()
                .bucket("bucket")
                .endpoint("http://127.0.0.1:9000")
                .region("us-east-1"),
        );
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn requires_endpoint() {
        let err = MinioBuilder::default()
            .bucket("bucket")
            .into_s3_config()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn rejects_incomplete_credentials() {
        let err = MinioBuilder::default()
            .bucket("bucket")
            .endpoint("http://127.0.0.1:9000")
            .session_token("token")
            .into_s3_config()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn rejects_credentials_in_anonymous_mode() {
        let err = MinioBuilder::default()
            .bucket("bucket")
            .endpoint("http://127.0.0.1:9000")
            .access_key_id("access")
            .secret_access_key("secret")
            .skip_signature()
            .into_s3_config()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn reports_minio_scheme() {
        let operator = Operator::new(
            MinioBuilder::default()
                .bucket("bucket")
                .endpoint("http://127.0.0.1:9000")
                .access_key_id("access")
                .secret_access_key("secret"),
        )
        .unwrap();
        assert_eq!(operator.info().scheme(), MINIO_SCHEME);
    }
}
