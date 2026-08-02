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

use crate::R2_SCHEME;
use crate::backend::S3Builder;
use crate::config::S3Config;
use crate::preset;
use crate::r2_config::R2Config;

/// Builds a Cloudflare R2 service with provider-specific configuration.
#[doc = include_str!("r2.md")]
#[derive(Default)]
pub struct R2Builder {
    config: R2Config,
}

impl Debug for R2Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("R2Builder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl R2Builder {
    pub(crate) fn from_config(config: R2Config) -> Self {
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

    /// Set the Cloudflare account ID used to derive the endpoint.
    pub fn account_id(mut self, account_id: &str) -> Self {
        self.config.account_id = Some(account_id.to_string());
        self
    }

    /// Set the R2 jurisdiction to `eu` or `fedramp`.
    pub fn jurisdiction(mut self, jurisdiction: &str) -> Self {
        self.config.jurisdiction = Some(jurisdiction.to_string());
        self
    }

    /// Set an explicit R2-compatible endpoint.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = Some(endpoint.to_string());
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

    fn into_s3_config(self) -> Result<(S3Config, ProvideCredentialChain<Credential>)> {
        let R2Config {
            root,
            bucket,
            account_id,
            jurisdiction,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
        } = self.config;

        preset::validate_required(&bucket, R2_SCHEME, "bucket must not be empty")?;
        preset::validate_optional(
            account_id.as_deref(),
            R2_SCHEME,
            "account_id must not be empty when set",
        )?;
        preset::validate_optional(
            jurisdiction.as_deref(),
            R2_SCHEME,
            "jurisdiction must not be empty when set",
        )?;
        preset::validate_optional(
            endpoint.as_deref(),
            R2_SCHEME,
            "endpoint must not be empty when set",
        )?;
        preset::validate_credentials(
            access_key_id.as_deref(),
            secret_access_key.as_deref(),
            session_token.as_deref(),
            R2_SCHEME,
        )?;

        let endpoint = match (account_id.as_deref(), endpoint.as_deref()) {
            (Some(account_id), None) => match jurisdiction.as_deref() {
                None => format!("https://{account_id}.r2.cloudflarestorage.com"),
                Some(jurisdiction @ ("eu" | "fedramp")) => {
                    format!("https://{account_id}.{jurisdiction}.r2.cloudflarestorage.com")
                }
                Some(_) => {
                    return Err(preset::config_error(
                        R2_SCHEME,
                        "jurisdiction must be either eu or fedramp",
                    ));
                }
            },
            (None, Some(endpoint)) if jurisdiction.is_none() => endpoint.to_string(),
            (None, Some(_)) => {
                return Err(preset::config_error(
                    R2_SCHEME,
                    "jurisdiction requires account_id and cannot be used with endpoint",
                ));
            }
            (Some(_), Some(_)) => {
                return Err(preset::config_error(
                    R2_SCHEME,
                    "account_id and endpoint are mutually exclusive",
                ));
            }
            (None, None) => {
                return Err(preset::config_error(
                    R2_SCHEME,
                    "exactly one of account_id and endpoint is required",
                ));
            }
        };

        let credential_providers = preset::credential_chain(
            access_key_id.as_deref(),
            secret_access_key.as_deref(),
            session_token.as_deref(),
        );

        let config = S3Config {
            root,
            bucket,
            endpoint: Some(endpoint),
            region: Some("auto".to_string()),
            access_key_id,
            secret_access_key,
            session_token,
            disable_ec2_metadata: true,
            ..Default::default()
        };

        Ok((config, credential_providers))
    }
}

impl Builder for R2Builder {
    type Config = R2Config;

    fn build(self) -> Result<impl Service> {
        let (config, credential_providers) = self.into_s3_config()?;
        S3Builder::from_provider_config(config, credential_providers).build_with_scheme(R2_SCHEME)
    }
}

#[cfg(test)]
mod tests {
    use opendal_core::ErrorKind;
    use opendal_core::Operator;

    use super::*;

    fn build_config(builder: R2Builder) -> S3Config {
        builder.into_s3_config().unwrap().0
    }

    #[test]
    fn derives_default_endpoint() {
        let config = build_config(R2Builder::default().bucket("bucket").account_id("account"));
        assert_eq!(
            config.endpoint.as_deref(),
            Some("https://account.r2.cloudflarestorage.com")
        );
        assert_eq!(config.region.as_deref(), Some("auto"));
    }

    #[test]
    fn derives_jurisdiction_endpoints() {
        for jurisdiction in ["eu", "fedramp"] {
            let config = build_config(
                R2Builder::default()
                    .bucket("bucket")
                    .account_id("account")
                    .jurisdiction(jurisdiction),
            );
            let expected = format!("https://account.{jurisdiction}.r2.cloudflarestorage.com");
            assert_eq!(config.endpoint.as_deref(), Some(expected.as_str()));
        }
    }

    #[test]
    fn accepts_explicit_endpoint() {
        let config = build_config(
            R2Builder::default()
                .bucket("bucket")
                .endpoint("http://127.0.0.1:9000"),
        );
        assert_eq!(config.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    }

    #[test]
    fn rejects_invalid_endpoint_and_jurisdiction_combinations() {
        let cases = [
            R2Builder::default().bucket("bucket"),
            R2Builder::default()
                .bucket("bucket")
                .account_id("account")
                .endpoint("https://example.com"),
            R2Builder::default()
                .bucket("bucket")
                .endpoint("https://example.com")
                .jurisdiction("eu"),
            R2Builder::default()
                .bucket("bucket")
                .account_id("account")
                .jurisdiction("invalid"),
        ];

        for builder in cases {
            assert_eq!(
                builder.into_s3_config().unwrap_err().kind(),
                ErrorKind::ConfigInvalid
            );
        }
    }

    #[test]
    fn rejects_incomplete_credentials() {
        let err = R2Builder::default()
            .bucket("bucket")
            .account_id("account")
            .session_token("token")
            .into_s3_config()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn reports_r2_scheme() {
        let operator = Operator::new(
            R2Builder::default()
                .bucket("bucket")
                .account_id("account")
                .access_key_id("access")
                .secret_access_key("secret"),
        )
        .unwrap();
        assert_eq!(operator.info().scheme(), R2_SCHEME);
    }
}
