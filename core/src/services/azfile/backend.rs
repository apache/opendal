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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use log::debug;
use reqsign::AzureStorageConfig;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use super::core::AzfileCore;
use super::error::parse_error;
use super::writer::AzfileWriter;
use super::writer::AzfileWriters;
use crate::raw::*;
use crate::services::azfile::lister::AzfileLister;
use crate::*;

/// Default endpoint of Azure File services.
const DEFAULT_AZFILE_ENDPOINT_SUFFIX: &str = "file.core.windows.net";

/// Azure File services support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct AzfileBuilder {
    root: Option<String>,
    endpoint: Option<String>,
    account_name: Option<String>,
    share_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for AzfileBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);
        ds.field("share_name", &self.share_name);
        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }

        ds.finish()
    }
}

impl AzfileBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

        self
    }

    /// Set endpoint of this backend.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set account_name of this backend.
    ///
    /// - If account_name is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn account_name(&mut self, account_name: &str) -> &mut Self {
        if !account_name.is_empty() {
            self.account_name = Some(account_name.to_string());
        }

        self
    }

    /// Set account_key of this backend.
    ///
    /// - If account_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn account_key(&mut self, account_key: &str) -> &mut Self {
        if !account_key.is_empty() {
            self.account_key = Some(account_key.to_string());
        }

        self
    }

    /// Set file share name of this backend.
    ///
    /// # Notes
    /// You can find more about from: <https://learn.microsoft.com/en-us/rest/api/storageservices/operations-on-shares--file-service>
    pub fn share_name(&mut self, share_name: &str) -> &mut Self {
        if !share_name.is_empty() {
            self.share_name = share_name.to_string();
        }

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for AzfileBuilder {
    const SCHEME: Scheme = Scheme::Azfile;
    type Accessor = AzfileBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = AzfileBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("account_name").map(|v| builder.account_name(v));
        map.get("account_key").map(|v| builder.account_key(v));
        map.get("share_name").map(|v| builder.share_name(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let endpoint = match &self.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azfile)),
        }?;
        debug!("backend use endpoint {}", &endpoint);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Azfile)
            })?
        };

        let account_name_option = self
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
            account_key: self.account_key.clone(),
            sas_token: self.sas_token.clone(),
            ..Default::default()
        };

        let cred_loader = AzureStorageLoader::new(config_loader);

        let signer = AzureStorageSigner::new();

        debug!("backend build finished: {:?}", &self);
        Ok(AzfileBackend {
            core: Arc::new(AzfileCore {
                root,
                endpoint,
                loader: cred_loader,
                client,
                signer,
                share_name: self.share_name.clone(),
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

#[async_trait]
impl Accessor for AzfileBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = AzfileWriters;
    type BlockingWriter = ();
    type Lister = oio::PageLister<AzfileLister>;
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Azfile)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_can_next: true,
                read_with_range: true,

                write: true,
                create_dir: true,
                delete: true,
                rename: true,

                list: true,

                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.ensure_parent_dir_exists(path).await?;
        let resp = self.core.azfile_create_dir(path).await?;
        let status = resp.status();

        match status {
            StatusCode::CREATED => {
                resp.into_body().consume().await?;
                Ok(RpCreateDir::default())
            }
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
                    Err(parse_error(resp).await?)
                }
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.azfile_read(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let size = parse_content_length(resp.headers())?;
                let range = parse_content_range(resp.headers())?;
                Ok((
                    RpRead::new().with_size(size).with_range(range),
                    resp.into_body(),
                ))
            }
            StatusCode::RANGE_NOT_SATISFIABLE => {
                resp.into_body().consume().await?;
                Ok((RpRead::new().with_size(Some(0)), IncomingAsyncBody::empty()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.core.ensure_parent_dir_exists(path).await?;
        let w = AzfileWriter::new(self.core.clone(), args.clone(), path.to_string());
        let w = if args.append() {
            AzfileWriters::Two(oio::AppendObjectWriter::new(w))
        } else {
            AzfileWriters::One(oio::OneShotWriter::new(w))
        };
        return Ok((RpWrite::default(), w));
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
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        self.core.ensure_parent_dir_exists(to).await?;
        let resp = self.core.azfile_rename(from, to).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpRename::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = if path.ends_with('/') {
            self.core.azfile_delete_dir(path).await?
        } else {
            self.core.azfile_delete_file(path).await?
        };

        let status = resp.status();
        match status {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => {
                resp.into_body().consume().await?;
                Ok(RpDelete::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = AzfileLister::new(self.core.clone(), path.to_string(), args.limit());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Builder;

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

    #[test]
    fn test_builder_from_endpoint_and_key_infer_account_name() {
        let mut azfile_builder = AzfileBuilder::default();
        azfile_builder.endpoint("https://account.file.core.windows.net/");
        azfile_builder.account_key("account-key");
        let azfile = azfile_builder
            .build()
            .expect("build Azdls should be succeeded.");

        assert_eq!(
            azfile.core.endpoint,
            "https://account.file.core.windows.net"
        );

        assert_eq!(
            azfile_builder.account_key.unwrap(),
            "account-key".to_string()
        );
    }
}
