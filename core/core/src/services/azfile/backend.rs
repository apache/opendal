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

use super::AZFILE_SCHEME;
use super::config::AzfileConfig;
use super::core::AzfileCore;
use super::core::X_MS_META_PREFIX;
use super::deleter::AzfileDeleter;
use super::error::parse_error;
use super::lister::AzfileLister;
use super::writer::AzfileWriter;
use super::writer::AzfileWriters;
use crate::raw::*;
use crate::*;

impl From<AzureStorageConfig> for AzfileConfig {
    fn from(config: AzureStorageConfig) -> Self {
        AzfileConfig {
            account_name: config.account_name,
            account_key: config.account_key,
            sas_token: config.sas_token,
            endpoint: config.endpoint,
            root: None,                // root is not part of AzureStorageConfig
            share_name: String::new(), // share_name is not part of AzureStorageConfig
        }
    }
}

/// Azure File services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct AzfileBuilder {
    pub(super) config: AzfileConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    pub(super) http_client: Option<HttpClient>,
}

impl Debug for AzfileBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzfileBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AzfileBuilder {
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

    /// Set endpoint of this backend.
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

    /// Set file share name of this backend.
    ///
    /// # Notes
    /// You can find more about from: <https://learn.microsoft.com/en-us/rest/api/storageservices/operations-on-shares--file-service>
    pub fn share_name(mut self, share_name: &str) -> Self {
        if !share_name.is_empty() {
            self.config.share_name = share_name.to_string();
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

    /// Create a new `AfileBuilder` instance from an [Azure Storage connection string][1].
    ///
    /// [1]: https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
    ///
    /// # Example
    /// ```
    /// use opendal::Builder;
    /// use opendal::services::Azfile;
    ///
    /// let conn_str = "AccountName=example;DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net";
    ///
    /// let mut config = Azfile::from_connection_string(&conn_str)
    ///     .unwrap()
    ///     // Add additional configuration if needed
    ///     .share_name("myShare")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn from_connection_string(conn_str: &str) -> Result<Self> {
        let config =
            raw::azure_config_from_connection_string(conn_str, raw::AzureStorageService::File)?;

        Ok(AzfileConfig::from(config).into_builder())
    }
}

impl Builder for AzfileBuilder {
    type Config = AzfileConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", AZFILE_SCHEME)),
        }?;
        debug!("backend use endpoint {}", &endpoint);

        let account_name_option = self
            .config
            .account_name
            .clone()
            .or_else(|| raw::azure_account_name_from_endpoint(endpoint.as_str()));

        let account_name = match account_name_option {
            Some(account_name) => Ok(account_name),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "account_name is empty")
                    .with_operation("Builder::build")
                    .with_context("service", AZFILE_SCHEME),
            ),
        }?;

        let config_loader = AzureStorageConfig {
            account_name: Some(account_name),
            account_key: self.config.account_key.clone(),
            sas_token: self.config.sas_token.clone(),
            ..Default::default()
        };

        let cred_loader = AzureStorageLoader::new(config_loader);
        let signer = AzureStorageSigner::new();
        Ok(AzfileBackend {
            core: Arc::new(AzfileCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(AZFILE_SCHEME)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,

                            read: true,

                            write: true,
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
                root,
                endpoint,
                loader: cred_loader,
                signer,
                share_name: self.config.share_name.clone(),
            }),
        })
    }
}

/// Backend for azfile services.
#[derive(Debug, Clone)]
pub struct AzfileBackend {
    core: Arc<AzfileCore>,
}

impl Access for AzfileBackend {
    type Reader = HttpBody;
    type Writer = AzfileWriters;
    type Lister = oio::PageLister<AzfileLister>;
    type Deleter = oio::OneShotDeleter<AzfileDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.ensure_parent_dir_exists(path).await?;
        let resp = self.core.azfile_create_dir(path).await?;
        let status = resp.status();

        match status {
            StatusCode::CREATED => Ok(RpCreateDir::default()),
            _ => {
                // we cannot just check status code because 409 Conflict has two meaning:
                // 1. If a directory by the same name is being deleted when Create Directory is called, the server returns status code 409 (Conflict)
                // 2. If a directory or file with the same name already exists, the operation fails with status code 409 (Conflict).
                // but we just need case 2 (already exists)
                // ref: https://learn.microsoft.com/en-us/rest/api/storageservices/create-directory
                if resp
                    .headers()
                    .get("x-ms-error-code")
                    .map(|value| value.to_str().unwrap_or(""))
                    .unwrap_or_else(|| "")
                    == "ResourceAlreadyExists"
                {
                    Ok(RpCreateDir::default())
                } else {
                    Err(parse_error(resp))
                }
            }
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let resp = if path.ends_with('/') {
            self.core.azfile_get_directory_properties(path).await?
        } else {
            self.core.azfile_get_file_properties(path).await?
        };

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?;
                let user_meta = parse_prefixed_headers(headers, X_MS_META_PREFIX);
                if !user_meta.is_empty() {
                    meta = meta.with_user_metadata(user_meta);
                }
                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.azfile_read(path, args.range()).await?;

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
        self.core.ensure_parent_dir_exists(path).await?;
        let w = AzfileWriter::new(self.core.clone(), args.clone(), path.to_string());
        let w = if args.append() {
            AzfileWriters::Two(oio::AppendWriter::new(w))
        } else {
            AzfileWriters::One(oio::OneShotWriter::new(w))
        };
        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(AzfileDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = AzfileLister::new(self.core.clone(), path.to_string(), args.limit());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        self.core.ensure_parent_dir_exists(to).await?;
        let resp = self.core.azfile_rename(from, to).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => Err(parse_error(resp)),
        }
    }
}
