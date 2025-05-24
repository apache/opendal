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
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::*;

use super::endpoint::DfsEndpoint;

/// Azure Data Lake Storage Gen2 Support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AzdlsConfig {
    /// Root of this backend.
    pub root: Option<String>,
    /// Filesystem name of this backend.
    pub filesystem: String,
    /// Endpoint of this backend.
    pub endpoint: Option<String>,
    /// Account name of this backend.
    pub account_name: Option<String>,
    /// Account key of this backend.
    /// - required for shared_key authentication
    pub account_key: Option<String>,
    /// client_secret
    /// The client secret of the service principal.
    /// - required for client_credentials authentication
    pub client_secret: Option<String>,
    /// tenant_id
    /// The tenant id of the service principal.
    /// - required for client_credentials authentication
    pub tenant_id: Option<String>,
    /// client_id
    /// The client id of the service principal.
    /// - required for client_credentials authentication
    pub client_id: Option<String>,
    /// sas_token
    /// The shared access signature token.
    /// - required for sas authentication
    pub sas_token: Option<String>,
    /// authority_host
    /// The authority host of the service principal.
    /// - required for client_credentials authentication
    /// - default value: `https://login.microsoftonline.com`
    pub authority_host: Option<String>,
}

impl AzdlsConfig {
    const AZURITE_DEFAULT_STORAGE_ACCOUNT_NAME: &'static str = "devstoreaccount1";
    const AZURITE_DEFAULT_STORAGE_ACCOUNT_KEY: &'static str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    /// Create a new `AzdlsConfig` instance from an [Azure Storage connection string][1].
    ///
    /// [1]: https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
    ///
    /// # Example
    /// ```
    /// use opendal::services::AzdlsConfig;
    ///
    /// let conn_str = "AccountName=example;DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net";
    ///
    /// let mut config = AzdlsConfig::try_from_connection_string(&conn_str).unwrap();
    ///
    /// // Set other parameters if needed
    /// config.client_id = Some("myClientId".to_string());
    /// config.client_secret = Some("myClientSecret".to_string());
    /// config.tenant_id = Some("myTenantId".to_string());
    /// ```
    pub fn try_from_connection_string(conn_str: &str) -> Result<Self> {
        let key_value_pairs = parse_connection_string(conn_str)?;

        let mut config = Self::default();

        // ADLS doesn't support a dedicated parameter like `BlobEndpoint`,
        // `FileEndpoint`, etc.
        // Instead, we need to build up the endpoint from its parts.
        let mut endpoint = DfsEndpoint::default();

        // Values used to parse and validate Azurite parameters.
        let mut use_development_storage = false;
        let mut development_storage_proxy_uri = None;

        for (key, value) in key_value_pairs {
            match key {
                "AccountName" => {
                    config.account_name = Some(value.to_string());
                    endpoint.set_account_name(value)?;
                }
                "AccountKey" => {
                    config.account_key = Some(value.to_string());
                }
                "DefaultEndpointsProtocol" => {
                    endpoint.set_protocol(value)?;
                }
                "EndpointSuffix" => {
                    endpoint.set_suffix(value)?;
                }
                "SharedAccessSignature" => {
                    config.sas_token = Some(value.to_string());
                }
                "BlobEndpoint" | "FileEndpoint" | "QueueEndpoint" | "TableEndpoint" => {
                    // ADLSv2 endpoints need to be built up explicitly from the
                    // `AccountName`, `EndpointSuffix` and `DefaultEndpointsProtocol`
                    // fields instead. There is currently no equivalent for DFS.
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        format!("{} is not supported with Azdls", key),
                    ));
                }
                "UseDevelopmentStorage" => {
                    if value == "true" {
                        use_development_storage = true;
                    }
                }
                "DevelopmentStorageProxyUri" => {
                    development_storage_proxy_uri = Some(value.to_string());
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        format!("Unexpected key in connection string: {}", key),
                    ));
                }
            }
        }

        if use_development_storage {
            apply_azurite_parameters(&mut config, development_storage_proxy_uri)?;
        } else {
            endpoint.validate()?;
            config.endpoint = Some(endpoint.to_string());
        }

        Ok(config)
    }
}

/// Takes a semicolon-delimited Azure Storage connection string and returns
/// key-value pairs split from it.
fn parse_connection_string(conn_str: &str) -> Result<Vec<(&str, &str)>> {
    conn_str
        .split(';')
        .map(|field| {
            field.split_once('=').ok_or(Error::new(
                ErrorKind::ConfigInvalid,
                format!(
                    "Invalid connection string, expected '=' in field: {}",
                    field
                ),
            ))
        })
        .collect()
}

/// Applies development storage-specific defaults.
fn apply_azurite_parameters(
    config: &mut AzdlsConfig,
    development_storage_proxy_uri: Option<String>,
) -> Result<()> {
    if development_storage_proxy_uri.is_none() {
        // `UseDevelopmentStorage` was set without `DevelopmentStorageProxyUri`.
        //
        // At time of this writing, Azurite doesn't yet support ADLS (see
        // https://github.com/Azure/Azurite/issues/553 for tracking).
        //
        // This implementation tries to be forward-compatible for when
        // Azurite starts to support ADLSv2.
        // More specifically, we can re-use defaults for account name and key,
        // but the endpoint is dependent on the storage implementation. So,
        // when support arrives, users will need to pass
        // `DevelopmentStorageProxyUri` explicitly.
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            "AzdlsConfig: UseDevelopmentStorage isn't supported without DevelopmentStorageProxyUri",
        ));
    }

    // `DevelopmentStorage` aka Azurite was enabled and we have proxy
    // URI available. Let's set the passed URI and the corresponding defaults.
    config.endpoint = development_storage_proxy_uri;

    // We don't want to overwrite explicitly provided account values.
    if config.account_name.is_none() {
        config.account_name = Some(AzdlsConfig::AZURITE_DEFAULT_STORAGE_ACCOUNT_NAME.to_string());
    }

    if config.account_key.is_none() {
        config.account_key = Some(AzdlsConfig::AZURITE_DEFAULT_STORAGE_ACCOUNT_KEY.to_string());
    }

    Ok(())
}

impl Debug for AzdlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzdlsConfig");

        ds.field("root", &self.root);
        ds.field("filesystem", &self.filesystem);
        ds.field("endpoint", &self.endpoint);

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }
        if self.client_secret.is_some() {
            ds.field("client_secret", &"<redacted>");
        }
        if self.tenant_id.is_some() {
            ds.field("tenant_id", &"<redacted>");
        }
        if self.client_id.is_some() {
            ds.field("client_id", &"<redacted>");
        }
        if self.sas_token.is_some() {
            ds.field("sas_token", &"<redacted>");
        }
        ds.finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::services::AzdlsConfig;

    #[test]
    fn test_azdlsconfig_try_from_connection_string() {
        let test_cases = vec![
        (
            "with minimal parameters",
            "AccountName=example;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https",
            AzdlsConfig {
                endpoint: Some("https://example.dfs.core.windows.net".to_string()),
                account_name: Some("example".to_string()),
                ..Default::default()
            },
        ),
        (
            "more parameters",
            "AccountName=example;AccountKey=myKey;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https;SharedAccessSignature=mySas",
            AzdlsConfig {
                endpoint: Some("https://example.dfs.core.windows.net".to_string()),
                account_name: Some("example".to_string()),
                account_key: Some("myKey".to_string()),
                sas_token: Some("mySas".to_string()),
                ..Default::default()
            },
        ),
        (
            "development storage",
            "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1:12345",
            AzdlsConfig {
                endpoint: Some("http://127.0.0.1:12345".to_string()),
                account_name: Some(AzdlsConfig::AZURITE_DEFAULT_STORAGE_ACCOUNT_NAME.to_string()),
                account_key: Some(AzdlsConfig::AZURITE_DEFAULT_STORAGE_ACCOUNT_KEY.to_string()),
                ..Default::default()
            },
        ),
        (
            "development storage with custom account values",
            "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1:12345;AccountName=myAccount;AccountKey=myKey",
            AzdlsConfig {
                endpoint: Some("http://127.0.0.1:12345".to_string()),
                account_name: Some("myAccount".to_string()),
                account_key: Some("myKey".to_string()),
                ..Default::default()
            },
        )];

        for (name, conn_str, expected) in test_cases {
            let config = AzdlsConfig::try_from_connection_string(conn_str).unwrap();
            assert_eq!(config, expected, "Test case: {}", name);
        }
    }

    #[test]
    fn test_azdlsconfig_try_from_connection_string_fails() {
        let test_cases = vec![
            (
                "missing equals",
                "AccountNameexample;AccountKey=example;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https",
            ),
            (
                "invalid key",
                "InvalidKey=example;AccountName=example;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https",
            ),
            (
                "with endpoint key",
                "BlobEndpoint=https://example.blob.core.windows.net;AccountName=example",
            ),
            (
                "with unknown endpoint suffix",
                "AccountName=example;EndpointSuffix=some.where.unknown.com;DefaultEndpointsProtocol=https",
            ),
            (
                "with invalid protocol",
                "AccountName=example;EndpointSuffix=some.where.unknown.com;DefaultEndpointsProtocol=ftp",
            ),
            (
                "with unspecified use of development storage",
                "UseDevelopmentStorage=true"
            ),
        ];

        for (name, conn_str) in test_cases {
            let result = AzdlsConfig::try_from_connection_string(conn_str);
            assert!(result.is_err(), "Test case: {}", name);
        }
    }
}
