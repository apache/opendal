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

use opendal_core::Configurator;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::OperatorUri;
use opendal_core::Result;
use reqsign_aws_v4::Credential;
use reqsign_aws_v4::EnvCredentialProvider;
use reqsign_aws_v4::ProfileCredentialProvider;
use reqsign_aws_v4::StaticCredentialProvider;
use reqsign_core::ProvideCredentialChain;
use url::Host;
use url::Url;

use crate::backend::S3Backend;
use crate::backend::S3Builder;
use crate::config::S3Config;

pub(crate) fn from_uri<C: Configurator>(uri: &OperatorUri) -> Result<C> {
    let mut map = uri.options().clone();

    if let Some(name) = uri.name() {
        map.insert("bucket".to_string(), name.to_string());
    }

    if let Some(root) = uri.root() {
        map.insert("root".to_string(), root.to_string());
    }

    C::from_iter(map)
}

pub(crate) fn config_error(service: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::ConfigInvalid, message)
        .with_operation("Builder::build")
        .with_context("service", service)
}

pub(crate) fn validate_required(
    value: &str,
    service: &'static str,
    message: &'static str,
) -> Result<()> {
    if value.trim().is_empty() {
        return Err(config_error(service, message));
    }
    Ok(())
}

pub(crate) fn validate_optional(
    value: Option<&str>,
    service: &'static str,
    message: &'static str,
) -> Result<()> {
    if value.is_some_and(|value| value.trim().is_empty()) {
        return Err(config_error(service, message));
    }
    Ok(())
}

pub(crate) fn validate_credentials(
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    session_token: Option<&str>,
    service: &'static str,
) -> Result<()> {
    validate_optional(
        access_key_id,
        service,
        "access_key_id must not be empty when set",
    )?;
    validate_optional(
        secret_access_key,
        service,
        "secret_access_key must not be empty when set",
    )?;
    validate_optional(
        session_token,
        service,
        "session_token must not be empty when set",
    )?;

    match (access_key_id, secret_access_key, session_token) {
        (None, None, None) | (Some(_), Some(_), None | Some(_)) => Ok(()),
        _ => Err(config_error(
            service,
            "access_key_id and secret_access_key must be provided together; session_token requires both",
        )),
    }
}

pub(crate) fn credential_chain(
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    session_token: Option<&str>,
) -> ProvideCredentialChain<Credential> {
    let mut chain = ProvideCredentialChain::new()
        .push(EnvCredentialProvider::new())
        .push(ProfileCredentialProvider::new());

    if let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) {
        let provider = if let Some(session_token) = session_token {
            StaticCredentialProvider::new(access_key_id, secret_access_key)
                .with_session_token(session_token)
        } else {
            StaticCredentialProvider::new(access_key_id, secret_access_key)
        };
        chain = chain.push_front(provider);
    }

    chain
}

pub(crate) fn dns_label<'a>(
    value: &'a str,
    scheme: &'static str,
    field: &'static str,
) -> Result<&'a str> {
    let value_bytes = value.as_bytes();
    if value_bytes.len() <= 63
        && value_bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && value_bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && value_bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
    {
        return Ok(value);
    }
    Err(config_error(scheme, "expected a lowercase DNS label").with_context("field", field))
}

pub(crate) struct Connection {
    pub(crate) endpoint: String,
    pub(crate) region: String,
    pub(crate) virtual_host_style: bool,
}

impl Connection {
    pub(crate) fn build(self, mut config: S3Config, scheme: &'static str) -> Result<S3Backend> {
        validate_required(&config.bucket, scheme, "bucket must not be empty")?;
        validate_required(&self.region, scheme, "region must not be empty")?;
        validate_credentials(
            config.access_key_id.as_deref(),
            config.secret_access_key.as_deref(),
            config.session_token.as_deref(),
            scheme,
        )?;

        let invalid_endpoint = || {
            config_error(
                scheme,
                "endpoint must be an HTTP(S) base URL without credentials, a query, or a fragment",
            )
        };
        let (_, authority_and_path) = self
            .endpoint
            .split_once("://")
            .ok_or_else(invalid_endpoint)?;
        let mut endpoint = Url::parse(&self.endpoint).map_err(|_| invalid_endpoint())?;
        if authority_and_path.starts_with(['/', '\\'])
            || !matches!(endpoint.scheme(), "http" | "https")
            || !endpoint.has_host()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(invalid_endpoint());
        }

        // Preset endpoints are base URLs. The first host label can be a location
        // or namespace that happens to match the bucket name.
        if self.virtual_host_style {
            let Some(Host::Domain(host)) = endpoint.host() else {
                return Err(config_error(
                    scheme,
                    "virtual-hosted-style requests require a DNS endpoint",
                ));
            };
            let bucket = dns_label(&config.bucket, scheme, "bucket")?;
            endpoint
                .set_host(Some(&format!("{bucket}.{host}")))
                .map_err(|_| invalid_endpoint())?;
        } else {
            if matches!(config.bucket.as_str(), "." | "..") {
                return Err(config_error(scheme, "bucket cannot be a dot segment"));
            }
            endpoint
                .path_segments_mut()
                .expect("HTTP(S) URLs support path segments")
                .pop_if_empty()
                .push(&config.bucket);
        }

        let credential_providers = credential_chain(
            config.access_key_id.as_deref(),
            config.secret_access_key.as_deref(),
            config.session_token.as_deref(),
        );
        config.region = Some(self.region);
        config.enable_virtual_host_style = self.virtual_host_style;
        config.disable_ec2_metadata = true;
        S3Builder::from_provider_config(config, credential_providers).build_with_resolved_endpoint(
            scheme,
            endpoint.as_str().trim_end_matches('/').to_owned(),
        )
    }
}

// Each config declares its own fields and resolves its own connection. The
// builder and registry implement the same OpenDAL contracts for every preset.
macro_rules! provider_preset {
    (
        $config:ident, $builder:ident, $scheme:ident = $name:literal, $register:ident, $guide:literal;
        required { $( $required:ident : $required_doc:literal ),+ $(,)? }
        optional { $( $optional:ident : $optional_doc:literal ),+ $(,)? }
        flags { $( $flag:ident : $flag_doc:literal ),* $(,)? }
        $( token: $session_token:ident; )?
    ) => {
        #[doc = concat!("URI scheme for the `", $name, "` preset.")]
        pub const $scheme: &str = $name;

        #[doc = concat!("Register the `", $name, "` URI scheme with an operator registry.")]
        pub fn $register(registry: &opendal_core::OperatorRegistry) {
            registry.register::<$builder>($scheme);
        }

        #[doc = include_str!($guide)]
        #[derive(Debug, Default)]
        pub struct $builder {
            config: $config,
        }

        impl $builder {
            $(
                #[doc = $required_doc]
                pub fn $required(mut self, value: &str) -> Self {
                    self.config.$required = value.to_owned();
                    self
                }
            )+
            $(
                #[doc = $optional_doc]
                pub fn $optional(mut self, value: &str) -> Self {
                    self.config.$optional = Some(value.to_owned());
                    self
                }
            )+
            $(
                #[doc = $flag_doc]
                pub fn $flag(mut self, value: bool) -> Self {
                    self.config.$flag = Some(value);
                    self
                }
            )*
        }

        impl opendal_core::Builder for $builder {
            type Config = $config;

            fn build(self) -> opendal_core::Result<impl opendal_core::raw::Service> {
                let connection = self.config.connection()?;
                let config = crate::config::S3Config {
                    root: self.config.root,
                    bucket: self.config.bucket,
                    access_key_id: self.config.access_key_id,
                    secret_access_key: self.config.secret_access_key,
                    $( session_token: self.config.$session_token, )?
                    ..Default::default()
                };
                connection.build(config, $scheme)
            }
        }

        impl opendal_core::Configurator for $config {
            type Builder = $builder;

            fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
                $crate::preset::from_uri(uri)
            }

            fn into_builder(self) -> Self::Builder {
                $builder { config: self }
            }
        }

        impl std::fmt::Debug for $config {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($config))
                    .field("root", &self.root)
                    .field("bucket", &self.bucket)
                    .finish_non_exhaustive()
            }
        }
    };
}

pub(crate) use provider_preset;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use reqsign_core::Context;
    use reqsign_core::ProvideCredential;
    use reqsign_core::StaticEnv;

    use super::*;

    #[test]
    fn credential_chain_contains_only_declared_sources() {
        assert_eq!(credential_chain(None, None, None).len(), 2);
        assert_eq!(
            credential_chain(Some("access"), Some("secret"), Some("token")).len(),
            3
        );
    }

    #[tokio::test]
    async fn direct_credentials_take_priority_over_environment() {
        let context = Context::new().with_env(StaticEnv {
            home_dir: None,
            envs: HashMap::from([
                ("AWS_ACCESS_KEY_ID".to_string(), "env-access".to_string()),
                (
                    "AWS_SECRET_ACCESS_KEY".to_string(),
                    "env-secret".to_string(),
                ),
            ]),
        });
        let chain = credential_chain(Some("direct-access"), Some("direct-secret"), None);

        let credential = chain.provide_credential(&context).await.unwrap().unwrap();
        assert_eq!(credential.access_key_id, "direct-access");
        assert_eq!(credential.secret_access_key, "direct-secret");
    }

    #[tokio::test]
    async fn loads_credentials_from_standard_environment() {
        let context = Context::new().with_env(StaticEnv {
            home_dir: None,
            envs: HashMap::from([
                ("AWS_ACCESS_KEY_ID".to_string(), "env-access".to_string()),
                (
                    "AWS_SECRET_ACCESS_KEY".to_string(),
                    "env-secret".to_string(),
                ),
            ]),
        });
        let chain = credential_chain(None, None, None);

        let credential = chain.provide_credential(&context).await.unwrap().unwrap();
        assert_eq!(credential.access_key_id, "env-access");
        assert_eq!(credential.secret_access_key, "env-secret");
    }
}
