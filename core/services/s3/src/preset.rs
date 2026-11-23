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
