// Shared Azure helpers for azblob/azdls/azfile services.
// Simplified from old raw::azure, without cross-service abstraction.

use std::collections::HashMap;

use http::Uri;
use http::response::Parts;
use reqsign::{AzureStorageConfig, AzureStorageCredential};

use crate::{Error, ErrorKind, Result};

#[derive(Debug, PartialEq)]
pub enum AzureStorageService {
    Blob,
    File,
    Adls,
}

pub fn azure_config_from_connection_string(
    conn_str: &str,
    storage: AzureStorageService,
) -> Result<AzureStorageConfig> {
    let key_values = parse_connection_string(conn_str)?;

    if storage == AzureStorageService::Blob {
        if let Some(dev) = collect_blob_development_config(&key_values) {
            return Ok(AzureStorageConfig {
                account_name: Some(dev.account_name),
                account_key: Some(dev.account_key),
                endpoint: Some(dev.endpoint),
                ..Default::default()
            });
        }
    }

    let mut config = AzureStorageConfig {
        account_name: key_values.get("AccountName").cloned(),
        endpoint: collect_endpoint(&key_values, &storage)?,
        ..Default::default()
    };

    if let Some(creds) = collect_credentials(&key_values) {
        set_credentials(&mut config, creds);
    }

    Ok(config)
}

pub fn azure_account_name_from_endpoint(endpoint: &str) -> Option<String> {
    const KNOWN_ENDPOINT_SUFFIXES: &[&str] = &[
        "core.windows.net",
        "core.usgovcloudapi.net",
        "core.chinacloudapi.cn",
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

pub fn with_azure_error_response_context(mut err: Error, mut parts: Parts) -> Error {
    if let Some(uri) = parts.extensions.get::<Uri>() {
        err = err.with_context("uri", censor_sas_uri(uri));
    }

    parts.headers.remove("Set-Cookie");
    parts.headers.remove("WWW-Authenticate");
    parts.headers.remove("Proxy-Authenticate");

    err.with_context("response", format!("{parts:?}"))
}

fn censor_sas_uri(uri: &Uri) -> String {
    if let Some(query) = uri.query() {
        let path = uri.path();
        let new_query: String = query
            .split('&')
            .filter(|p| !p.starts_with("sig="))
            .collect::<Vec<_>>()
            .join("&");
        let mut parts = uri.clone().into_parts();
        parts.path_and_query = Some(format!("{path}?{new_query}").try_into().unwrap());
        Uri::from_parts(parts).unwrap().to_string()
    } else {
        uri.to_string()
    }
}

fn parse_connection_string(conn_str: &str) -> Result<HashMap<String, String>> {
    conn_str
        .trim()
        .replace('\n', "")
        .split(';')
        .filter(|&field| !field.is_empty())
        .map(|field| {
            let (key, value) = field.trim().split_once('=').ok_or(Error::new(
                ErrorKind::ConfigInvalid,
                format!("Invalid connection string, expected '=' in field: {field}"),
            ))?;
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

fn collect_blob_development_config(
    key_values: &HashMap<String, String>,
) -> Option<DevelopmentStorageConfig> {
    if key_values.get("UseDevelopmentStorage") != Some(&"true".to_string()) {
        return None;
    }

    const AZURITE_DEFAULT_STORAGE_ACCOUNT_NAME: &str = "devstoreaccount1";
    const AZURITE_DEFAULT_STORAGE_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    const AZURITE_DEFAULT_BLOB_URI: &str = "http://127.0.0.1:10000";

    let account_name = key_values
        .get("AccountName")
        .cloned()
        .unwrap_or_else(|| AZURITE_DEFAULT_STORAGE_ACCOUNT_NAME.to_string());
    let account_key = key_values
        .get("AccountKey")
        .cloned()
        .unwrap_or_else(|| AZURITE_DEFAULT_STORAGE_ACCOUNT_KEY.to_string());
    let development_proxy_uri = key_values
        .get("DevelopmentStorageProxyUri")
        .cloned()
        .unwrap_or_else(|| AZURITE_DEFAULT_BLOB_URI.to_string());

    Some(DevelopmentStorageConfig {
        endpoint: format!("{development_proxy_uri}/{account_name}"),
        account_name,
        account_key,
    })
}

struct DevelopmentStorageConfig {
    account_name: String,
    account_key: String,
    endpoint: String,
}

fn collect_endpoint(
    key_values: &HashMap<String, String>,
    storage: &AzureStorageService,
) -> Result<Option<String>> {
    match storage {
        AzureStorageService::Blob => collect_or_build_endpoint(key_values, "BlobEndpoint", "blob"),
        AzureStorageService::File => collect_or_build_endpoint(key_values, "FileEndpoint", "file"),
        AzureStorageService::Adls => collect_endpoint_from_parts(key_values, "dfs"),
    }
}

fn collect_credentials(key_values: &HashMap<String, String>) -> Option<AzureStorageCredential> {
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
        AzureStorageCredential::BearerToken(_, _) => {}
    }
}

fn collect_or_build_endpoint(
    key_values: &HashMap<String, String>,
    endpoint_key: &str,
    service_name: &str,
) -> Result<Option<String>> {
    if let Some(endpoint) = key_values.get(endpoint_key) {
        Ok(Some(endpoint.clone()))
    } else {
        collect_endpoint_from_parts(key_values, service_name)
    }
}

fn collect_endpoint_from_parts(
    key_values: &HashMap<String, String>,
    storage_endpoint_name: &str,
) -> Result<Option<String>> {
    let (account_name, endpoint_suffix) = match (
        key_values.get("AccountName"),
        key_values.get("EndpointSuffix"),
    ) {
        (Some(name), Some(suffix)) => (name, suffix),
        _ => return Ok(None),
    };

    let protocol = key_values
        .get("DefaultEndpointsProtocol")
        .map(String::as_str)
        .unwrap_or("https");
    if protocol != "http" && protocol != "https" {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            format!("Invalid DefaultEndpointsProtocol: {protocol}"),
        ));
    }

    Ok(Some(format!(
        "{protocol}://{account_name}.{storage_endpoint_name}.{endpoint_suffix}"
    )))
}
