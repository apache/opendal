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

use std::fmt::{Display, Formatter};

use crate::*;

/// DFS Endpoints are the endpoints used to access Azure Data Lake Storage Gen2.
///
/// This struct is used to build, parse and validate endpoint strings.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct DfsEndpoint {
    // Should be either "http" or "https".
    protocol: String,

    account_name: String,

    // This is the suffix for the endpoint, e.g. "core.windows.net".
    suffix: String,
}

impl DfsEndpoint {
    /// Known endpoint suffix Azure Data Lake Storage Gen2 URI syntax.
    ///
    /// Azure public cloud: https://accountname.dfs.core.windows.net
    /// Azure US Government: https://accountname.dfs.core.usgovcloudapi.net
    /// Azure China: https://accountname.dfs.core.chinacloudapi.cn
    const KNOWN_SUFFIXES: &'static [&'static str] = &[
        "core.windows.net",
        "core.usgovcloudapi.net",
        "core.chinacloudapi.cn",
    ];

    const STORAGE: &'static str = "dfs";

    pub(crate) fn set_protocol(&mut self, protocol: &str) -> Result<()> {
        if protocol != "http" && protocol != "https" {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("Invalid protocol: {} should be http or https", protocol),
            ));
        }

        self.protocol = protocol.to_string();
        Ok(())
    }

    // We only call account_name() _indirectly_ through `TryFrom`. The compiler
    // doesn't recognize this and marks the method as dead code.
    #[allow(dead_code)]
    pub(crate) fn account_name(&self) -> &str {
        &self.account_name
    }

    pub(crate) fn set_account_name(&mut self, account_name: &str) -> Result<()> {
        if account_name.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "Account name cannot be empty".to_string(),
            ));
        }

        self.account_name = account_name.to_string();
        Ok(())
    }

    pub(crate) fn set_suffix(&mut self, suffix: &str) -> Result<()> {
        if !Self::KNOWN_SUFFIXES.contains(&suffix) {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("Unexpected endpoint suffix: {}", suffix),
            ));
        }

        self.suffix = suffix.to_string();
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "DfsEndpoint: protocol cannot be empty".to_string(),
            ));
        }

        if self.account_name.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "DfsEndpoint: account name cannot be empty".to_string(),
            ));
        }

        if self.suffix.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "DfsEndpoint suffix cannot be empty".to_string(),
            ));
        }

        Ok(())
    }
}

impl Display for DfsEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.validate().is_err() {
            return Err(std::fmt::Error);
        }

        write!(
            f,
            "{}://{}.{}.{}",
            self.protocol,
            self.account_name,
            Self::STORAGE,
            self.suffix
        )
    }
}

impl TryFrom<&str> for DfsEndpoint {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let (protocol, endpoint) = value.split_once("://").ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                format!("DfsEndpoint: Missing protocol: {}", value),
            )
        })?;

        if protocol != "http" && protocol != "https" {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("DfsEndpoint: Invalid protocol: {}", protocol),
            ));
        }

        let (account_name, storage_endpoint) = endpoint.split_once('.').ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                format!("DfsEndpoint: Invalid format: {}", value),
            )
        })?;

        if account_name.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("DfsEndpoint: Empty account name: {}", value),
            ));
        }

        let (storage, endpoint_suffix) = storage_endpoint.split_once('.').ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                format!("DfsEndpoint: Invalid endpoint format: {}", value),
            )
        })?;

        // ADLSv2 is backed by hierarchical namespaces built on top of blob storage.
        // We therefore expect the "Distributed File System" storage endpoint.
        if storage != Self::STORAGE {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("DfsEndpoint: Invalid storage {}", storage),
            ));
        }

        let endpoint_suffix = endpoint_suffix.trim_end_matches('/').to_lowercase();

        if !Self::KNOWN_SUFFIXES.contains(&endpoint_suffix.as_str()) {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!(
                    "DfsEndpoint: Unexpected endpoint suffix: {}",
                    endpoint_suffix
                ),
            ));
        }

        Ok(DfsEndpoint {
            protocol: protocol.to_string(),
            account_name: account_name.to_string(),
            suffix: endpoint_suffix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::DfsEndpoint;

    #[test]
    fn test_dfsendpoint_to_string() {
        let endpoint = DfsEndpoint {
            protocol: "https".to_string(),
            account_name: "accountname".to_string(),
            suffix: "core.windows.net".to_string(),
        };

        let expected = "https://accountname.dfs.core.windows.net";
        let result = endpoint.to_string();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dfsendpoint_try_from_str() {
        let test_cases = vec![
            (
                "just a connection string",
                "https://accountname.dfs.core.windows.net",
                DfsEndpoint {
                    protocol: "https".to_string(),
                    account_name: "accountname".to_string(),
                    suffix: "core.windows.net".to_string(),
                },
            ),
            (
                "plain text",
                "http://accountname.dfs.core.windows.net",
                DfsEndpoint {
                    protocol: "http".to_string(),
                    account_name: "accountname".to_string(),
                    suffix: "core.windows.net".to_string(),
                },
            ),
            (
                "a different endpoint suffix",
                "https://accountname.dfs.core.usgovcloudapi.net",
                DfsEndpoint {
                    protocol: "https".to_string(),
                    account_name: "accountname".to_string(),
                    suffix: "core.usgovcloudapi.net".to_string(),
                },
            ),
        ];

        for (name, input, expected) in test_cases {
            let endpoint = super::DfsEndpoint::try_from(input).unwrap();
            assert_eq!(endpoint, expected, "Test case: {}", name);
        }
    }

    #[test]
    fn test_dfsendpoint_try_from_str_fails() {
        let test_cases = vec![
            ("missing protocol", "accountname.dfs.core.windows.net"),
            ("invalid protocol", "ftp://accountname.dfs.core.windows.net"),
            ("missing account name", "https://.dfs.core.windows.net"),
            ("missing storage", "https://accountname..core.windows.net"),
            (
                "invalid storage",
                "https://accountname.blob.core.windows.net",
            ),
            (
                "invalid format",
                "https://no-account-storage-sep.core.windows.net",
            ),
        ];

        for (name, input) in test_cases {
            let result = super::DfsEndpoint::try_from(input);
            assert!(result.is_err(), "Test case: {}", name);
        }
    }
}
