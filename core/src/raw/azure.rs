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

//! Azure Storage helpers.
//!
//! This module provides utilities and shared abstractions for services built
//! on Azure Storage, such as Azure Blob Storage (`services-azblob`) or
//! Azure Data Lake Storage (`services-azdls`).

use std::collections::HashMap;

use reqsign::{AzureStorageConfig, AzureStorageCredential};

use crate::{Error, ErrorKind, Result};

/// Parses an [Azure connection string][1] into a configuration object.
///
/// The connection string doesn't have to specify all required parameters
/// because the user is still allowed to set them later directly on the object.
///
/// The function takes an AzureStorageService parameter because it determines
/// the fields used to parse the endpoint.
///
/// [1]: https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
pub(crate) fn azure_config_from_connection_string(
    conn_str: &str,
    storage: AzureStorageService,
) -> Result<AzureStorageConfig> {
    let key_values = parse_connection_string(conn_str)?;

    let mut config = AzureStorageConfig::default();

    config.account_name = key_values.get("AccountName").cloned();
    config.endpoint = parse_endpoint(&key_values, storage)?;
    if let Some(creds) = parse_credentials(&key_values) {
        set_credentials(&mut config, creds);
    };

    Ok(config)
}

/// The service that a connection string refers to. The type influences
/// interpretation of endpoint-related fields during parsing.
pub(crate) enum AzureStorageService {
    /// Azure Blob Storage.
    Blob,

    /// Azure Data Lake Storage Gen2.
    /// Backed by Blob Storage but exposed through a different endpoint (`dfs`).
    Adls,
}

pub(crate) fn azure_account_name_from_endpoint(endpoint: &str) -> Option<String> {
    /// Known Azure Storage endpoint suffixes.
    const KNOWN_ENDPOINT_SUFFIXES: &[&str] = &[
        "core.windows.net",       // Azure public cloud
        "core.usgovcloudapi.net", // Azure US Government
        "core.chinacloudapi.cn",  // Azure China
    ];

    let endpoint: &str = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);

    let (account_name, service_endpoint) = endpoint.split_once('.')?;
    let (_storage_service, endpoint_suffix) = service_endpoint.split_once('.')?;

    if KNOWN_ENDPOINT_SUFFIXES.contains(&endpoint_suffix.trim_end_matches('/')) {
        Some(account_name.to_string())
    } else {
        None
    }
}

/// Takes a semicolon-delimited Azure Storage connection string and returns
/// key-value pairs split from it.
fn parse_connection_string(conn_str: &str) -> Result<HashMap<String, String>> {
    conn_str
        .trim()
        .replace("\n", "")
        .split(';')
        .filter(|&field| !field.is_empty())
        .map(|field| {
            let (key, value) = field.trim().split_once('=').ok_or(Error::new(
                ErrorKind::ConfigInvalid,
                format!(
                    "Invalid connection string, expected '=' in field: {}",
                    field
                ),
            ))?;
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

// TODO: Add support for Azurite values
/// Parses an endpoint from the key-value pairs if possible.
///
/// Users are still able to later supplement configuration with an endpoint,
/// so endpoint-related fields aren't enforced.
fn parse_endpoint(
    key_values: &HashMap<String, String>,
    storage: AzureStorageService,
) -> Result<Option<String>> {
    match storage {
        AzureStorageService::Blob => {
            if let Some(blob_endpoint) = key_values.get("BlobEndpoint") {
                Ok(Some(blob_endpoint.clone()))
            } else if let Some(blob_endpoint) = endpoint_from_parts(key_values, "blob")? {
                Ok(Some(blob_endpoint.clone()))
            } else {
                Ok(None)
            }
        }
        AzureStorageService::Adls => {
            if let Some(dfs_endpoint) = endpoint_from_parts(key_values, "dfs")? {
                Ok(Some(dfs_endpoint.clone()))
            } else {
                Ok(None)
            }
        }
    }
}

fn parse_credentials(key_values: &HashMap<String, String>) -> Option<AzureStorageCredential> {
    if let Some(sas_token) = key_values.get("SharedAccessSignature") {
        Some(AzureStorageCredential::SharedAccessSignature(
            sas_token.clone(),
        ))
    } else if let (Some(account_name), Some(account_key)) =
        (key_values.get("AccountName"), key_values.get("AccountKey"))
    {
        Some(AzureStorageCredential::SharedKey(
            account_name.clone(),
            account_key.clone(),
        ))
    } else {
        // We default to no authentication. This is not an error because e.g.
        // Azure Active Directory configuration is typically not passed via
        // connection strings.
        // Users may also set credentials manually on the configuration.
        None
    }
}

fn set_credentials(config: &mut AzureStorageConfig, creds: AzureStorageCredential) {
    match creds {
        AzureStorageCredential::SharedAccessSignature(sas_token) => {
            config.sas_token = Some(sas_token);
        }
        AzureStorageCredential::SharedKey(account_name, account_key) => {
            config.account_name = Some(account_name);
            config.account_key = Some(account_key);
        }
        AzureStorageCredential::BearerToken(_, _) => {
            // Bearer tokens shouldn't be passed via connection strings.
        }
    }
}

fn endpoint_from_parts(
    key_values: &HashMap<String, String>,
    storage_endpoint_name: &str,
) -> Result<Option<String>> {
    let (account_name, endpoint_suffix) = match (
        key_values.get("AccountName"),
        key_values.get("EndpointSuffix"),
    ) {
        (Some(name), Some(suffix)) => (name, suffix),
        _ => return Ok(None), // Can't build an endpoint if one of them is missing
    };

    let protocol = key_values
        .get("DefaultEndpointsProtocol")
        .map(String::as_str)
        .unwrap_or("https"); // Default to HTTPS if not specified
    if protocol != "http" && protocol != "https" {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            format!("Invalid DefaultEndpointsProtocol: {}", protocol),
        ));
    }

    Ok(Some(format!(
        "{protocol}://{account_name}.{storage_endpoint_name}.{endpoint_suffix}"
    )))
}

#[cfg(test)]
mod tests {
    use reqsign::AzureStorageConfig;

    use super::{
        azure_account_name_from_endpoint, azure_config_from_connection_string, AzureStorageService,
    };

    #[test]
    fn test_azure_connection_config_from_connection_string() {
        let test_cases = vec![
            ("minimal fields",
                (AzureStorageService::Blob, "BlobEndpoint=https://testaccount.blob.core.windows.net/"),
                Some(AzureStorageConfig{
                    endpoint: Some("https://testaccount.blob.core.windows.net/".to_string()),
                    ..Default::default()
                }),
            ),
            ("basic creds and blob endpoint",
                (AzureStorageService::Blob, "AccountName=testaccount;AccountKey=testkey;BlobEndpoint=https://testaccount.blob.core.windows.net/"),
                Some(AzureStorageConfig{
                    account_name: Some("testaccount".to_string()),
                    account_key: Some("testkey".to_string()),
                    endpoint: Some("https://testaccount.blob.core.windows.net/".to_string()),
                     ..Default::default()
                    }),
            ),
            ("SAS token",
                (AzureStorageService::Blob, "SharedAccessSignature=blablabla"),
                Some(AzureStorageConfig{
                    sas_token: Some("blablabla".to_string()),
                    ..Default::default()
                }),
            ),
            ("endpoint from parts",
                (AzureStorageService::Blob, "AccountName=testaccount;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https"),
                Some(AzureStorageConfig{
                    endpoint: Some("https://testaccount.blob.core.windows.net".to_string()),
                    account_name: Some("testaccount".to_string()),
                    ..Default::default()
                }),
            ),
            ("endpoint from parts and no protocol",
                (AzureStorageService::Blob, "AccountName=testaccount;EndpointSuffix=core.windows.net"),
                Some(AzureStorageConfig{
                    // Defaults to https
                    endpoint: Some("https://testaccount.blob.core.windows.net".to_string()),
                    account_name: Some("testaccount".to_string()),
                    ..Default::default()
                }),
            ),
            ("adls endpoint from parts",
                (AzureStorageService::Adls, "AccountName=testaccount;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https"),
                Some(AzureStorageConfig{
                    account_name: Some("testaccount".to_string()),
                    endpoint: Some("https://testaccount.dfs.core.windows.net".to_string()),
                    ..Default::default()
                }),
            ),
            ("prefers sas over key",
                (AzureStorageService::Blob, "AccountName=testaccount;AccountKey=testkey;SharedAccessSignature=sas_token"),
                Some(AzureStorageConfig{
                    sas_token: Some("sas_token".to_string()),
                    account_name: Some("testaccount".to_string()),
                    ..Default::default()
                }),
            ),
            (
                "unknown key is ignored",
                (AzureStorageService::Blob, "SomeUnknownKey=123;BlobEndpoint=https://testaccount.blob.core.windows.net/"),
                Some(AzureStorageConfig{
                    endpoint: Some("https://testaccount.blob.core.windows.net/".to_string()),
                    ..Default::default()
                }),
            ),
            (
                "leading and trailing `;`",
                (AzureStorageService::Blob, ";AccountName=testaccount;"),
                Some(AzureStorageConfig {
                    account_name: Some("testaccount".to_string()),
                    ..Default::default()
                }),
            ),
            (
                "line breaks",
                (AzureStorageService::Blob, r#"
                    AccountName=testaccount;
                    AccountKey=testkey;
                    EndpointSuffix=core.windows.net;
                    DefaultEndpointsProtocol=https"#),
                Some(AzureStorageConfig {
                    account_name: Some("testaccount".to_string()),
                    account_key: Some("testkey".to_string()),
                    endpoint: Some("https://testaccount.blob.core.windows.net".to_string()),
                    ..Default::default()
                }),
            ),
            (
                "missing equals",
                (AzureStorageService::Blob, "AccountNameexample;AccountKey=example;EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https",),
                None, // This should fail due to missing '='
            ),
            (
                "with invalid protocol",
                (AzureStorageService::Blob, "DefaultEndpointsProtocol=ftp;AccountName=example;EndpointSuffix=core.windows.net",),
                None, // This should fail due to invalid protocol
            ),

        ];

        for (name, (storage, conn_str), expected) in test_cases {
            let actual = azure_config_from_connection_string(conn_str, storage);

            if let Some(expected) = expected {
                assert_azure_storage_config_eq(&actual.expect(name), &expected, name);
            } else {
                assert!(actual.is_err(), "Expected error for case: {}", name);
            }
        }
    }

    #[test]
    fn test_infer_account_name_from_endpoint() {
        let test_cases = vec![
            ("https://account.blob.core.windows.net", Some("account")),
            (
                "https://account.blob.core.usgovcloudapi.net",
                Some("account"),
            ),
            (
                "https://account.blob.core.chinacloudapi.cn",
                Some("account"),
            ),
            ("https://account.dfs.core.windows.net", Some("account")),
            ("https://account.blob.core.windows.net/", Some("account")),
            ("https://account.blob.unknown.suffix.com", None),
            ("http://blob.core.windows.net", None),
        ];
        for (endpoint, expected_account_name) in test_cases {
            let account_name = azure_account_name_from_endpoint(endpoint);
            assert_eq!(
                account_name,
                expected_account_name.map(|s| s.to_string()),
                "Endpoint: {}",
                endpoint
            );
        }
    }

    /// Helper function to compare AzureStorageConfig fields manually.
    fn assert_azure_storage_config_eq(
        actual: &AzureStorageConfig,
        expected: &AzureStorageConfig,
        name: &str,
    ) {
        assert_eq!(
            actual.account_name, expected.account_name,
            "account_name mismatch: {}",
            name
        );
        assert_eq!(
            actual.account_key, expected.account_key,
            "account_key mismatch: {}",
            name
        );
        assert_eq!(
            actual.endpoint, expected.endpoint,
            "endpoint mismatch: {}",
            name
        );
        assert_eq!(
            actual.sas_token, expected.sas_token,
            "sas_token mismatch: {}",
            name
        );
    }
}
