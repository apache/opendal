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

use opendal_core::raw::percent_encode_path;
use opendal_core::{Configurator, OperatorUri, Result};
use serde::Deserialize;
use serde::Serialize;

use super::backend::SharepointBuilder;

/// Config for [SharePoint](https://www.microsoft.com/en-us/microsoft-365/sharepoint/collaboration)
/// backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SharepointConfig {
    /// The browser URL of the folder inside a SharePoint document library that this
    /// operator is anchored to.
    ///
    /// For example
    /// `https://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports`.
    ///
    /// The value is used verbatim, so paste the URL as it appears in the browser
    /// address bar with percent-encoding intact.
    pub folder_url: Option<String>,
    /// The root path under `folder_url` for the file access
    pub root: Option<String>,
    /// Microsoft Entra tenant ID used to build the OAuth 2.0 token endpoint.
    ///
    /// Defaults to `common` when unset.
    pub tenant_id: Option<String>,
    /// Microsoft Graph API access token
    pub access_token: Option<String>,
    /// Microsoft Graph API refresh token
    pub refresh_token: Option<String>,
    /// Microsoft Graph API Application (client) ID that is in the Azure's app registration portal
    pub client_id: Option<String>,
    /// Microsoft Graph API Application client secret that is in the Azure's app registration portal
    pub client_secret: Option<String>,
}

impl Debug for SharepointConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharepointConfig")
            .field("folder_url", &self.folder_url)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl Configurator for SharepointConfig {
    type Builder = SharepointBuilder;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut map = uri.options().clone();

        // A URI such as
        // `sharepoint://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports`
        // carries the anchor folder in the authority plus path. `OperatorUri` hands the
        // path back percent-decoded, so re-encode it to rebuild the original URL.
        if !map.contains_key("folder_url")
            && let Some(authority) = uri.authority()
        {
            let path = uri.root().unwrap_or_default();
            let folder_url = if path.is_empty() {
                format!("https://{authority}")
            } else {
                format!("https://{authority}/{}", percent_encode_path(path))
            };
            map.insert("folder_url".to_string(), folder_url);
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        SharepointBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_builds_folder_url() {
        let uri = OperatorUri::new(
            "sharepoint://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SharepointConfig::from_uri(&uri).unwrap();
        assert_eq!(
            cfg.folder_url.as_deref(),
            Some("https://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports")
        );
    }

    #[test]
    fn from_uri_keeps_explicit_folder_url_option() {
        let uri = OperatorUri::new(
            "sharepoint://contoso.sharepoint.com/ignored",
            vec![(
                "folder_url".to_string(),
                "https://contoso.sharepoint.com/sites/HR/Documents".to_string(),
            )],
        )
        .unwrap();

        let cfg = SharepointConfig::from_uri(&uri).unwrap();
        assert_eq!(
            cfg.folder_url.as_deref(),
            Some("https://contoso.sharepoint.com/sites/HR/Documents")
        );
    }

    #[test]
    fn from_uri_passes_root_through_options() {
        let uri = OperatorUri::new(
            "sharepoint://contoso.sharepoint.com/sites/Finance/Documents?root=/reports",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SharepointConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.root.as_deref(), Some("/reports"));
        assert_eq!(
            cfg.folder_url.as_deref(),
            Some("https://contoso.sharepoint.com/sites/Finance/Documents")
        );
    }
}
