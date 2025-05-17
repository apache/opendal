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

use http::Response;
use http::StatusCode;
use log::debug;
use reqsign::AzureStorageConfig;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use super::core::AzfileCore;
use super::delete::AzfileDeleter;
use super::error::parse_error;
use super::lister::AzfileLister;
use super::writer::AzfileWriter;
use super::writer::AzfileWriters;
use crate::raw::*;
use crate::services::AzfileConfig;
use crate::*;

/// Default endpoint of Azure File services.
const DEFAULT_AZFILE_ENDPOINT_SUFFIX: &str = "file.core.windows.net";

impl Configurator for AzfileConfig {
    type Builder = AzfileBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        AzfileBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// Azure File services support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct AzfileBuilder {
    config: AzfileConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for AzfileBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzfileBuilder");

        ds.field("config", &self.config);

        ds.finish()
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
}

impl Builder for AzfileBuilder {
    const SCHEME: Scheme = Scheme::Azfile;
    type Config = AzfileConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azfile)),
        }?;
        debug!("backend use endpoint {}", &endpoint);

        let account_name_option = self
            .config
            .account_name
            .clone()
            .or_else(|| infer_account_name_from_endpoint(endpoint.as_str()));

        let account_name = match account_name_option {
            Some(account_name) => Ok(account_name),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "account_name is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Azfile),
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
                    am.set_scheme(Scheme::Azfile)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_cache_control: true,
                            stat_has_content_length: true,
                            stat_has_content_type: true,
                            stat_has_content_encoding: true,
                            stat_has_content_range: true,
                            stat_has_etag: true,
                            stat_has_content_md5: true,
                            stat_has_last_modified: true,
                            stat_has_content_disposition: true,

                            read: true,

                            write: true,
                            create_dir: true,
                            delete: true,
                            rename: true,

                            list: true,
                            list_has_etag: true,
                            list_has_last_modified: true,
                            list_has_content_length: true,

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

fn infer_account_name_from_endpoint(endpoint: &str) -> Option<String> {
    let endpoint: &str = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);

    let mut parts = endpoint.splitn(2, '.');
    let account_name = parts.next();
    let endpoint_suffix = parts
        .next()
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_lowercase();

    if endpoint_suffix == DEFAULT_AZFILE_ENDPOINT_SUFFIX {
        account_name.map(|s| s.to_string())
    } else {
        None
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
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

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
                let meta = parse_into_metadata(path, resp.headers())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_storage_name_from_endpoint() {
        let cases = vec![
            (
                "test infer account name from endpoint",
                "https://account.file.core.windows.net",
                "account",
            ),
            (
                "test infer account name from endpoint with trailing slash",
                "https://account.file.core.windows.net/",
                "account",
            ),
        ];
        for (desc, endpoint, expected) in cases {
            let account_name = infer_account_name_from_endpoint(endpoint);
            assert_eq!(account_name, Some(expected.to_string()), "{}", desc);
        }
    }
}
