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
use std::sync::Arc;

use http::Response;
use http::StatusCode;
use log::debug;
use reqsign::AzureStorageConfig;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use super::AZDLS_SCHEME;
use super::config::AzdlsConfig;
use super::core::AzdlsCore;
use super::core::DIRECTORY;
use super::deleter::AzdlsDeleter;
use super::error::parse_error;
use super::lister::AzdlsLister;
use super::writer::AzdlsWriter;
use super::writer::AzdlsWriters;
use crate::raw::*;
use crate::*;

impl From<AzureStorageConfig> for AzdlsConfig {
    fn from(config: AzureStorageConfig) -> Self {
        AzdlsConfig {
            endpoint: config.endpoint,
            account_name: config.account_name,
            account_key: config.account_key,
            client_secret: config.client_secret,
            tenant_id: config.tenant_id,
            client_id: config.client_id,
            sas_token: config.sas_token,
            authority_host: config.authority_host,
            ..Default::default()
        }
    }
}

/// Azure Data Lake Storage Gen2 Support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct AzdlsBuilder {
    pub(super) config: AzdlsConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    pub(super) http_client: Option<HttpClient>,
}

impl Debug for AzdlsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzdlsBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AzdlsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set filesystem name of this backend.
    pub fn filesystem(mut self, filesystem: &str) -> Self {
        self.config.filesystem = filesystem.to_string();

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azblob: `https://accountname.blob.core.windows.net`
    /// - Azurite: `http://127.0.0.1:10000/devstoreaccount1`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set account_name of this backend.
    ///
    /// - If account_name is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn account_name(mut self, account_name: &str) -> Self {
        if !account_name.is_empty() {
            self.config.account_name = Some(account_name.to_string());
        }

        self
    }

    /// Set account_key of this backend.
    ///
    /// - If account_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn account_key(mut self, account_key: &str) -> Self {
        if !account_key.is_empty() {
            self.config.account_key = Some(account_key.to_string());
        }

        self
    }

    /// Set client_secret of this backend.
    ///
    /// - If client_secret is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - required for client_credentials authentication
    pub fn client_secret(mut self, client_secret: &str) -> Self {
        if !client_secret.is_empty() {
            self.config.client_secret = Some(client_secret.to_string());
        }

        self
    }

    /// Set tenant_id of this backend.
    ///
    /// - If tenant_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - required for client_credentials authentication
    pub fn tenant_id(mut self, tenant_id: &str) -> Self {
        if !tenant_id.is_empty() {
            self.config.tenant_id = Some(tenant_id.to_string());
        }

        self
    }

    /// Set client_id of this backend.
    ///
    /// - If client_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - required for client_credentials authentication
    pub fn client_id(mut self, client_id: &str) -> Self {
        if !client_id.is_empty() {
            self.config.client_id = Some(client_id.to_string());
        }

        self
    }

    /// Set the sas_token of this backend.
    pub fn sas_token(mut self, sas_token: &str) -> Self {
        if !sas_token.is_empty() {
            self.config.sas_token = Some(sas_token.to_string());
        }

        self
    }

    /// Set authority_host of this backend.
    ///
    /// - If authority_host is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - default value: `https://login.microsoftonline.com`
    pub fn authority_host(mut self, authority_host: &str) -> Self {
        if !authority_host.is_empty() {
            self.config.authority_host = Some(authority_host.to_string());
        }

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Create a new `AzdlsBuilder` instance from an [Azure Storage connection string][1].
    ///
    /// [1]: https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
    ///
    /// # Example
    /// ```
    /// use opendal::Builder;
    /// use opendal::services::Azdls;
    ///
    /// let conn_str = "AccountName=example;DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net";
    ///
    /// let mut config = Azdls::from_connection_string(&conn_str)
    ///     .unwrap()
    ///     // Add additional configuration if needed
    ///     .filesystem("myFilesystem")
    ///     .client_id("myClientId")
    ///     .client_secret("myClientSecret")
    ///     .tenant_id("myTenantId")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn from_connection_string(conn_str: &str) -> Result<Self> {
        let config =
            raw::azure_config_from_connection_string(conn_str, raw::AzureStorageService::Adls)?;

        Ok(AzdlsConfig::from(config).into_builder())
    }
}

impl Builder for AzdlsBuilder {
    type Config = AzdlsConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        // Handle endpoint, region and container name.
        let filesystem = match self.config.filesystem.is_empty() {
            false => Ok(&self.config.filesystem),
            true => Err(Error::new(ErrorKind::ConfigInvalid, "filesystem is empty")
                .with_operation("Builder::build")
                .with_context("service", AZDLS_SCHEME)),
        }?;
        debug!("backend use filesystem {}", &filesystem);

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone().trim_end_matches('/').to_string()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", AZDLS_SCHEME)),
        }?;
        debug!("backend use endpoint {}", &endpoint);

        let config_loader = AzureStorageConfig {
            account_name: self
                .config
                .account_name
                .clone()
                .or_else(|| raw::azure_account_name_from_endpoint(endpoint.as_str())),
            account_key: self.config.account_key.clone(),
            sas_token: self.config.sas_token,
            client_id: self.config.client_id.clone(),
            client_secret: self.config.client_secret.clone(),
            tenant_id: self.config.tenant_id.clone(),
            authority_host: self.config.authority_host.clone(),
            ..Default::default()
        };

        let cred_loader = AzureStorageLoader::new(config_loader);
        let signer = AzureStorageSigner::new();
        Ok(AzdlsBackend {
            core: Arc::new(AzdlsCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(AZDLS_SCHEME)
                        .set_root(&root)
                        .set_name(filesystem)
                        .set_native_capability(Capability {
                            stat: true,

                            read: true,

                            write: true,
                            write_can_append: true,
                            write_can_multi: true,
                            write_with_if_none_match: true,
                            write_with_if_not_exists: true,
                            write_with_user_metadata: true,

                            create_dir: true,
                            delete: true,
                            rename: true,

                            list: true,

                            shared: true,

                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = self.http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
                },
                filesystem: self.config.filesystem.clone(),
                root,
                endpoint,
                loader: cred_loader,
                signer,
            }),
        })
    }
}

/// Backend for azblob services.
#[derive(Debug, Clone)]
pub struct AzdlsBackend {
    core: Arc<AzdlsCore>,
}

impl Access for AzdlsBackend {
    type Reader = HttpBody;
    type Writer = AzdlsWriters;
    type Lister = oio::PageLister<AzdlsLister>;
    type Deleter = oio::OneShotDeleter<AzdlsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let resp = self
            .core
            .azdls_create(path, DIRECTORY, &OpWrite::default())
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        // TODO: include metadata for the root (#4746)
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let metadata = self.core.azdls_stat_metadata(path).await?;
        Ok(RpStat::new(metadata))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.azdls_read(path, args.range()).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok((RpRead::new(), resp.into_body())),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.append() {
            let w = AzdlsWriter::new(self.core.clone(), args.clone(), path.to_string());
            return Ok((
                RpWrite::default(),
                AzdlsWriters::Two(oio::AppendWriter::new(w)),
            ));
        }

        let w = AzdlsWriter::create(self.core.clone(), args.clone(), path.to_string()).await?;
        let w = oio::PositionWriter::new(self.info().clone(), w, args.concurrent());
        Ok((RpWrite::default(), AzdlsWriters::One(w)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(AzdlsDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = AzdlsLister::new(self.core.clone(), path.to_string(), args.limit());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        if let Some(resp) = self.core.azdls_ensure_parent_path(to).await? {
            let status = resp.status();
            match status {
                StatusCode::CREATED | StatusCode::CONFLICT => {}
                _ => return Err(parse_error(resp)),
            }
        }

        let resp = self.core.azdls_rename(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED => Ok(RpRename::default()),
            _ => Err(parse_error(resp)),
        }
    }
}
