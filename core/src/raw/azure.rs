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

pub(crate) fn azure_account_name_from_endpoint(endpoint: &str) -> Option<String> {
    /// Known Azure Storage endpoint suffixes.
    const KNOWN_ENDPOINT_SUFFIXES: &[&str] = &[
        "core.windows.net",       //  Azure public cloud
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

#[cfg(test)]
mod tests {
    use super::azure_account_name_from_endpoint;

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
}
