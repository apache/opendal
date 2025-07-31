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
use std::sync::Arc;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use http::Response;
use http::StatusCode;
use log::debug;
use reqsign::AzureStorageConfig;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;
use sha2::Digest;
use sha2::Sha256;

use super::core::constants::X_MS_META_PREFIX;
use super::core::constants::X_MS_VERSION_ID;
use super::core::AzblobCore;
use super::delete::AzblobDeleter;
use super::error::parse_error;
use super::lister::AzblobLister;
use super::writer::AzblobWriter;
use super::writer::AzblobWriters;
use super::DEFAULT_SCHEME;
use crate::raw::*;
use crate::services::AzblobConfig;
use crate::*;
const AZBLOB_BATCH_LIMIT: usize = 256;

impl From<AzureStorageConfig> for AzblobConfig {
    fn from(value: AzureStorageConfig) -> Self {
        Self {
            endpoint: value.endpoint,
            account_name: value.account_name,
            account_key: value.account_key,
            sas_token: value.sas_token,
            ..Default::default()
        }
    }
}

impl Configurator for AzblobConfig {
    type Builder = AzblobBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        AzblobBuilder {
            config: self,

            http_client: None,
        }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct AzblobBuilder {
    config: AzblobConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for AzblobBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzblobBuilder");

        ds.field("config", &self.config);

        ds.finish()
    }
}

impl AzblobBuilder {
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

    /// Set container name of this backend.
    pub fn container(mut self, container: &str) -> Self {
        self.config.container = container.to_string();

        self
    }

    /// Set endpoint of this backend
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

    /// Set encryption_key of this backend.
    ///
    /// # Args
    ///
    /// `v`: Base64-encoded key that matches algorithm specified in `encryption_algorithm`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn encryption_key(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.encryption_key = Some(v.to_string());
        }

        self
    }

    /// Set encryption_key_sha256 of this backend.
    ///
    /// # Args
    ///
    /// `v`: Base64-encoded SHA256 digest of the key specified in encryption_key.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn encryption_key_sha256(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.encryption_key_sha256 = Some(v.to_string());
        }

        self
    }

    /// Set encryption_algorithm of this backend.
    ///
    /// # Args
    ///
    /// `v`: server-side encryption algorithm. (Available values: `AES256`)
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn encryption_algorithm(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.encryption_algorithm = Some(v.to_string());
        }

        self
    }

    /// Enable server side encryption with customer key.
    ///
    /// As known as: CPK
    ///
    /// # Args
    ///
    /// `key`: Base64-encoded SHA256 digest of the key specified in encryption_key.
    ///
    /// # Note
    ///
    /// Function that helps the user to set the server-side customer-provided encryption key, the key's SHA256, and the algorithm.
    /// See [Server-side encryption with customer-provided keys (CPK)](https://learn.microsoft.com/en-us/azure/storage/blobs/encryption-customer-provided-keys)
    /// for more info.
    pub fn server_side_encryption_with_customer_key(mut self, key: &[u8]) -> Self {
        // Only AES256 is supported for now
        self.config.encryption_algorithm = Some("AES256".to_string());
        self.config.encryption_key = Some(BASE64_STANDARD.encode(key));
        self.config.encryption_key_sha256 =
            Some(BASE64_STANDARD.encode(Sha256::digest(key).as_slice()));
        self
    }

    /// Set sas_token of this backend.
    ///
    /// - If sas_token is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    ///
    /// See [Grant limited access to Azure Storage resources using shared access signatures (SAS)](https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview)
    /// for more info.
    pub fn sas_token(mut self, sas_token: &str) -> Self {
        if !sas_token.is_empty() {
            self.config.sas_token = Some(sas_token.to_string());
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

    /// Set maximum batch operations of this backend.
    pub fn batch_max_operations(mut self, batch_max_operations: usize) -> Self {
        self.config.batch_max_operations = Some(batch_max_operations);

        self
    }

    /// from_connection_string will make a builder from connection string
    ///
    /// connection string looks like:
    ///
    /// ```txt
    /// DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;
    /// AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
    /// BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
    /// QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;
    /// TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
    /// ```
    ///
    /// Or
    ///
    /// ```txt
    /// DefaultEndpointsProtocol=https;
    /// AccountName=storagesample;
    /// AccountKey=<account-key>;
    /// EndpointSuffix=core.chinacloudapi.cn;
    /// ```
    ///
    /// For reference: [Configure Azure Storage connection strings](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string)
    ///
    /// # Note
    ///
    /// Connection strings can only configure the endpoint, account name and
    /// authentication information. Users still need to configure container name.
    pub fn from_connection_string(conn: &str) -> Result<Self> {
        let config =
            raw::azure_config_from_connection_string(conn, raw::AzureStorageService::Blob)?;

        Ok(AzblobConfig::from(config).into_builder())
    }
}

impl Builder for AzblobBuilder {
    type Config = AzblobConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        // Handle endpoint, region and container name.
        let container = match self.config.container.is_empty() {
            false => Ok(&self.config.container),
            true => Err(Error::new(ErrorKind::ConfigInvalid, "container is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azblob)),
        }?;
        debug!("backend use container {}", &container);

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azblob)),
        }?;
        debug!("backend use endpoint {}", &container);

        let mut config_loader = AzureStorageConfig::default().from_env();

        if let Some(v) = self
            .config
            .account_name
            .clone()
            .or_else(|| raw::azure_account_name_from_endpoint(endpoint.as_str()))
        {
            config_loader.account_name = Some(v);
        }

        if let Some(v) = self.config.account_key.clone() {
            config_loader.account_key = Some(v);
        }

        if let Some(v) = self.config.sas_token.clone() {
            config_loader.sas_token = Some(v);
        }

        let encryption_key =
            match &self.config.encryption_key {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_key")
                })?),
            };

        let encryption_key_sha256 = match &self.config.encryption_key_sha256 {
            None => None,
            Some(v) => Some(build_header_value(v).map_err(|err| {
                err.with_context("key", "server_side_encryption_customer_key_sha256")
            })?),
        };

        let encryption_algorithm = match &self.config.encryption_algorithm {
            None => None,
            Some(v) => {
                if v == "AES256" {
                    Some(build_header_value(v).map_err(|err| {
                        err.with_context("key", "server_side_encryption_customer_algorithm")
                    })?)
                } else {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        "encryption_algorithm value must be AES256",
                    ));
                }
            }
        };

        let cred_loader = AzureStorageLoader::new(config_loader);

        let signer = AzureStorageSigner::new();

        Ok(AzblobBackend {
            core: Arc::new(AzblobCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(DEFAULT_SCHEME)
                        .set_root(&root)
                        .set_name(container)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_with_if_match: true,
                            stat_with_if_none_match: true,

                            read: true,

                            read_with_if_match: true,
                            read_with_if_none_match: true,
                            read_with_override_content_disposition: true,
                            read_with_if_modified_since: true,
                            read_with_if_unmodified_since: true,

                            write: true,
                            write_can_append: true,
                            write_can_empty: true,
                            write_can_multi: true,
                            write_with_cache_control: true,
                            write_with_content_type: true,
                            write_with_if_not_exists: true,
                            write_with_if_none_match: true,
                            write_with_user_metadata: true,

                            delete: true,
                            delete_max_size: Some(AZBLOB_BATCH_LIMIT),

                            copy: true,
                            copy_with_if_not_exists: true,

                            list: true,
                            list_with_recursive: true,

                            presign: self.config.sas_token.is_some(),
                            presign_stat: self.config.sas_token.is_some(),
                            presign_read: self.config.sas_token.is_some(),
                            presign_write: self.config.sas_token.is_some(),

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
                encryption_key,
                encryption_key_sha256,
                encryption_algorithm,
                container: self.config.container.clone(),

                loader: cred_loader,
                signer,
            }),
        })
    }
}

/// Backend for azblob services.
#[derive(Debug, Clone)]
pub struct AzblobBackend {
    core: Arc<AzblobCore>,
}

impl Access for AzblobBackend {
    type Reader = HttpBody;
    type Writer = AzblobWriters;
    type Lister = oio::PageLister<AzblobLister>;
    type Deleter = oio::BatchDeleter<AzblobDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.azblob_get_blob_properties(path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?;
                if let Some(version_id) = parse_header_to_str(headers, X_MS_VERSION_ID)? {
                    meta.set_version(version_id);
                }

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
        let resp = self.core.azblob_get_blob(path, args.range(), &args).await?;

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
        let w = AzblobWriter::new(self.core.clone(), args.clone(), path.to_string());
        let w = if args.append() {
            AzblobWriters::Two(oio::AppendWriter::new(w))
        } else {
            AzblobWriters::One(oio::BlockWriter::new(
                self.core.info.clone(),
                w,
                args.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(AzblobDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = AzblobLister::new(
            self.core.clone(),
            path.to_string(),
            args.recursive(),
            args.limit(),
        );

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.azblob_copy_blob(from, to, args).await?;

        let status = resp.status();

        match status {
            StatusCode::ACCEPTED => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.azblob_head_blob_request(path, v),
            PresignOperation::Read(v) => {
                self.core
                    .azblob_get_blob_request(path, BytesRange::default(), v)
            }
            PresignOperation::Write(_) => {
                self.core
                    .azblob_put_blob_request(path, None, &OpWrite::default(), Buffer::new())
            }
            PresignOperation::Delete(_) => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        };

        let mut req = req?;

        self.core.sign_query(&mut req).await?;

        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}
