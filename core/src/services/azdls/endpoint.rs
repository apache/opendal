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
    const KNOWN_SUFFIXES: &[&str] = &[
        "core.windows.net",
        "core.usgovcloudapi.net",
        "core.chinacloudapi.cn",
    ];

    const STORAGE: &str = "dfs";

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

#[cfg(test)]
mod test {
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
}
