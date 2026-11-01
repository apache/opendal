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

use std::sync::Arc;

use http::StatusCode;

use opendal_core::raw::*;
use opendal_core::*;

use super::core::SharePointCore;
use super::core::parse_error;
use super::deleter::SharePointDeleter;
use super::lister::SharePointLister;
use super::reader::*;
use super::writer::SharePointWriter;

use std::fmt::Debug;

use log::debug;
use mea::mutex::Mutex;
use mea::once::OnceCell;

use super::SHAREPOINT_SCHEME;
use super::config::SharepointConfig;
use super::core::DEFAULT_TENANT_ID;
use super::core::SharePointSigner;

/// Microsoft [SharePoint](https://www.microsoft.com/en-us/microsoft-365/sharepoint/collaboration)
/// backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SharepointBuilder {
    pub(super) config: SharepointConfig,
}

impl Debug for SharepointBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharepointBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl SharepointBuilder {
    /// Set the browser URL of the folder that this operator is anchored to.
    ///
    /// For example
    /// `https://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports`.
    ///
    /// # Note
    ///
    /// - This is required.
    /// - Paste the URL as it appears in the browser address bar. It is used
    ///   verbatim, so percent-encoding must already be applied.
    /// - The URL is resolved to a drive item once, on first use. Because the
    ///   resolved item id is immutable, later renames of the site, the document
    ///   library, or the folder itself do not break an existing operator.
    pub fn folder_url(mut self, folder_url: &str) -> Self {
        self.config.folder_url = if folder_url.is_empty() {
            None
        } else {
            Some(folder_url.to_string())
        };

        self
    }

    /// Set root path of the SharePoint folder, relative to `folder_url`.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set the Microsoft Entra tenant ID used to build the OAuth 2.0 token endpoint.
    ///
    /// Defaults to `common`. Work and school accounts, which is the usual case
    /// for SharePoint, normally require the tenant ID or the tenant's domain name.
    pub fn tenant_id(mut self, tenant_id: &str) -> Self {
        self.config.tenant_id = Some(tenant_id.to_string());
        self
    }

    /// Set the access token for a time limited access to Microsoft Graph API.
    ///
    /// Microsoft Graph API uses a typical OAuth 2.0 flow for authentication and authorization.
    /// You can get a access token from [Microsoft Graph Explore](https://developer.microsoft.com/en-us/graph/graph-explorer).
    ///
    /// # Note
    ///
    /// - An access token is short-lived.
    /// - Use a refresh_token if you want to use the API for an extended period of time.
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = Some(access_token.to_string());
        self
    }

    /// Set the refresh token for long term access to Microsoft Graph API.
    ///
    /// OpenDAL will use a refresh token to maintain a fresh access token automatically.
    ///
    /// # Note
    ///
    /// - A refresh token is available through a OAuth 2.0 flow, with an additional scope `offline_access`.
    pub fn refresh_token(mut self, refresh_token: &str) -> Self {
        self.config.refresh_token = Some(refresh_token.to_string());
        self
    }

    /// Set the client_id for a Microsoft Graph API application (available though Azure's registration portal)
    ///
    /// Required when using the refresh token.
    pub fn client_id(mut self, client_id: &str) -> Self {
        self.config.client_id = Some(client_id.to_string());
        self
    }

    /// Set the client_secret for a Microsoft Graph API application
    ///
    /// Required for Web app when using the refresh token.
    /// Don't use a client secret when use in a native app since the native app can't store the secret reliably.
    pub fn client_secret(mut self, client_secret: &str) -> Self {
        self.config.client_secret = Some(client_secret.to_string());
        self
    }
}

impl Builder for SharepointBuilder {
    type Config = SharepointConfig;

    fn build(self) -> Result<impl Service> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let folder_url = self.config.folder_url.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "folder_url must be set")
                .with_context("service", SHAREPOINT_SCHEME)
        })?;

        let info = ServiceInfo::new(SHAREPOINT_SCHEME, &root, "");
        let capability = Capability {
            read: true,
            read_with_suffix: true,

            write: true,
            // SharePoint supports the file size up to 250GB
            // Read more at https://support.microsoft.com/en-us/office/restrictions-and-limitations-in-onedrive-and-sharepoint-64883a5d-228e-48f5-b3d2-eb39e07630fa#individualfilesize
            // However, we can't enable this, otherwise OpenDAL behavior tests will try to test creating huge
            // file up to this size.
            // write_total_max_size: Some(250 * 1024 * 1024 * 1024),
            copy: true,
            rename: true,

            stat: true,

            delete: true,
            create_dir: true,

            list: true,
            list_with_limit: true,

            shared: true,

            // Deliberately not declared yet, pending verification against a real
            // document library:
            //
            // - `stat_with_version` / `list_with_versions`: whether versions are
            //   returned depends on the library's versioning settings.
            // - `read_with_if_none_match` / `stat_with_if_none_match` /
            //   `write_with_if_match`: these ride on `eTag`, which on Graph covers
            //   metadata *and* content. SharePoint libraries carry metadata columns
            //   that can change `eTag` without the bytes changing, which would make
            //   conditional requests fail spuriously. `cTag` is content-only but is
            //   not returned for folders on SharePoint.
            //
            // The request builders already honor these arguments, so enabling a flag
            // here is all that is needed once the behavior tests confirm it.
            ..Default::default()
        };

        let mut signer = SharePointSigner::new();
        signer.tenant_id = self
            .config
            .tenant_id
            .filter(|tenant| !tenant.is_empty())
            .unwrap_or_else(|| DEFAULT_TENANT_ID.to_string());

        // Requires OAuth 2.0 tokens:
        // - `access_token` (the short-lived token)
        // - `refresh_token` flow (the long term token)
        // to be mutually exclusive for setting up for implementation simplicity
        match (self.config.access_token, self.config.refresh_token) {
            (Some(access_token), None) => {
                signer.access_token = access_token;
                signer.expires_in = Timestamp::MAX;
            }
            (None, Some(refresh_token)) => {
                let client_id = self.config.client_id.ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "client_id must be set when refresh_token is set",
                    )
                    .with_context("service", SHAREPOINT_SCHEME)
                })?;

                signer.refresh_token = refresh_token;
                signer.client_id = client_id;
                if let Some(client_secret) = self.config.client_secret {
                    signer.client_secret = client_secret;
                }
            }
            (Some(_), Some(_)) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token and refresh_token cannot be set at the same time",
                )
                .with_context("service", SHAREPOINT_SCHEME));
            }
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token or refresh_token must be set",
                )
                .with_context("service", SHAREPOINT_SCHEME));
            }
        };

        let core = Arc::new(SharePointCore {
            info,
            capability,
            root,
            folder_url,
            signer: Arc::new(Mutex::new(signer)),
            anchor: OnceCell::new(),
        });

        Ok(SharepointBackend { core })
    }
}

#[derive(Clone, Debug)]
pub struct SharepointBackend {
    pub core: Arc<SharePointCore>,
}

impl Service for SharepointBackend {
    type Reader = oio::StreamReader<SharepointReader>;
    type Writer = oio::OneShotWriter<SharePointWriter>;
    type Lister = oio::PageLister<SharePointLister>;
    type Deleter = oio::OneShotDeleter<SharePointDeleter>;
    type Copier = oio::OneShotCopier;

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        if path == "/" {
            // skip, the anchor folder already exists.
            return Ok(RpCreateDir::default());
        }

        let response = self.core.sharepoint_create_dir(ctx, path).await?;
        match response.status() {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(response)),
        }
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let meta = self.core.sharepoint_stat(ctx, path, args).await?;

        Ok(RpStat::new(meta))
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(oio::StreamReader::new(SharepointReader::new(
            self.clone(),
            ctx.clone(),
            path,
            args,
        )))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(oio::OneShotWriter::new(SharePointWriter::new(
            self.core.clone(),
            ctx.clone(),
            args,
            path.to_string(),
        )))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(oio::OneShotDeleter::new(SharePointDeleter::new(
            self.core.clone(),
            ctx.clone(),
        )))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();

        Ok(oio::OneShotCopier::new(async move {
            let monitor_url = core.initialize_copy(&ctx, &from, &to).await?;
            core.wait_until_complete(&ctx, monitor_url).await?;
            Ok(Metadata::default())
        }))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        if from == to {
            return Ok(RpRename::default());
        }

        self.core.sharepoint_move(ctx, from, to).await?;

        Ok(RpRename::default())
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let l = SharePointLister::new(
            path.to_string(),
            self.core.clone(),
            ctx.clone(),
            self.core.capability,
            &args,
        );

        Ok(oio::PageLister::new(l))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FOLDER_URL: &str = "https://contoso.sharepoint.com/sites/Finance/Documents";

    #[test]
    fn build_requires_folder_url() {
        let err = SharepointBuilder::default()
            .access_token("token")
            .build()
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(err.to_string().contains("folder_url"));
    }

    #[test]
    fn build_accepts_access_token_alone() {
        SharepointBuilder::default()
            .folder_url(FOLDER_URL)
            .access_token("token")
            .build()
            .expect("access_token alone is a valid configuration");
    }

    #[test]
    fn build_accepts_refresh_token_with_client_id() {
        SharepointBuilder::default()
            .folder_url(FOLDER_URL)
            .refresh_token("refresh")
            .client_id("client")
            .build()
            .expect("refresh_token with client_id is a valid configuration");
    }

    #[test]
    fn build_rejects_refresh_token_without_client_id() {
        let err = SharepointBuilder::default()
            .folder_url(FOLDER_URL)
            .refresh_token("refresh")
            .build()
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(err.to_string().contains("client_id"));
    }

    #[test]
    fn build_rejects_both_tokens() {
        let err = SharepointBuilder::default()
            .folder_url(FOLDER_URL)
            .access_token("token")
            .refresh_token("refresh")
            .client_id("client")
            .build()
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(err.to_string().contains("same time"));
    }

    #[test]
    fn build_rejects_missing_tokens() {
        let err = SharepointBuilder::default()
            .folder_url(FOLDER_URL)
            .build()
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(err.to_string().contains("must be set"));
    }
}
