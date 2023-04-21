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

use super::core::AzdfsCore;
use super::error::parse_error;
use super::pager::AzdfsPager;
use super::writer::AzdfsWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Known endpoint suffix Azure Data Lake Storage Gen2 URI syntax.
/// Azure public cloud: https://accountname.dfs.core.windows.net
/// Azure US Government: https://accountname.dfs.core.usgovcloudapi.net
/// Azure China: https://accountname.dfs.core.chinacloudapi.cn
const KNOWN_AZDFS_ENDPOINT_SUFFIX: &[&str] = &[
    "dfs.core.windows.net",
    "dfs.core.usgovcloudapi.net",
    "dfs.core.chinacloudapi.cn",
];

/// Azure Data Lake Storage Gen2 Support.
///
/// As known as `abfs`, `azdfs` or `azdls`.
///
/// This service will visist the [ABFS](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver) URI supported by [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction).
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] rename
/// - [x] list
/// - [ ] ~~scan~~
/// - [ ] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work dir for backend.
/// - `filesystem`: Set the filesystem name for backend.
/// - `endpoint`: Set the endpoint for backend.
/// - `account_name`: Set the account_name for backend.
/// - `account_key`: Set the account_key for backend.
///
/// Refer to public API docs for more information.
///
/// # Example
///
/// ## Init OpenDAL Operator
///
/// ### Via Builder
///
/// ```no_run
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::services::Azdfs;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create azblob backend builder.
///     let mut builder = Azdfs::default();
///     // Set the root for azblob, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/path/to/dir");
///     // Set the filesystem name, this is required.
///     builder.filesystem("test");
///     // Set the endpoint, this is required.
///     //
///     // For examples:
///     // - "https://accountname.dfs.core.windows.net"
///     builder.endpoint("https://accountname.dfs.core.windows.net");
///     // Set the account_name and account_key.
///     //
///     // OpenDAL will try load credential from the env.
///     // If credential not set and no valid credential in env, OpenDAL will
///     // send request without signing like anonymous user.
///     builder.account_name("account_name");
///     builder.account_key("account_key");
///
///     // `Accessor` provides the low level APIs, we will use `Operator` normally.
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
#[derive(Default, Clone)]
pub struct AzdfsBuilder {
    root: Option<String>,
    filesystem: String,
    endpoint: Option<String>,
    account_name: Option<String>,
    account_key: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for AzdfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("root", &self.root);
        ds.field("filesystem", &self.filesystem);
        ds.field("endpoint", &self.endpoint);

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }

        ds.finish()
    }
}

impl AzdfsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

        self
    }

    /// Set filesystem name of this backend.
    pub fn filesystem(&mut self, filesystem: &str) -> &mut Self {
        self.filesystem = filesystem.to_string();

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azblob: `https://accountname.blob.core.windows.net`
    /// - Azurite: `http://127.0.0.1:10000/devstoreaccount1`
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

impl Builder for AzdfsBuilder {
    type Accessor = AzdfsBackend;
    const SCHEME: Scheme = Scheme::Azdfs;

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle endpoint, region and container name.
        let filesystem = match self.filesystem.is_empty() {
            false => Ok(&self.filesystem),
            true => Err(Error::new(ErrorKind::ConfigInvalid, "filesystem is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azdfs)),
        }?;
        debug!("backend use filesystem {}", &filesystem);

        let endpoint = match &self.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azdfs)),
        }?;
        debug!("backend use endpoint {}", &filesystem);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Azdfs)
            })?
        };

        let config_loader = AzureStorageConfig {
            account_name: self
                .account_name
                .clone()
                .or_else(|| infer_storage_name_from_endpoint(endpoint.as_str())),
            account_key: self.account_key.clone(),
            sas_token: None,
        };

        let cred_loader = AzureStorageLoader::new(config_loader);
        let signer = AzureStorageSigner::new();

        debug!("backend build finished: {:?}", &self);
        Ok(AzdfsBackend {
            core: Arc::new(AzdfsCore {
                filesystem: self.filesystem.clone(),
                root,
                endpoint,
                client,
                loader: cred_loader,
                signer,
            }),
        })
    }

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = AzdfsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("filesystem").map(|v| builder.filesystem(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("account_name").map(|v| builder.account_name(v));
        map.get("account_key").map(|v| builder.account_key(v));

        builder
    }
}

/// Backend for azblob services.
#[derive(Debug, Clone)]
pub struct AzdfsBackend {
    core: Arc<AzdfsCore>,
}

#[async_trait]
impl Accessor for AzdfsBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = AzdfsWriter;
    type BlockingWriter = ();
    type Pager = AzdfsPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Azdfs)
            .set_root(&self.core.root)
            .set_name(&self.core.filesystem)
            .set_capability(Capability {
                read: true,
                read_can_next: true,
                write: true,
                rename: true,
                list: true,
                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut req =
            self.core
                .azdfs_create_request(path, "directory", None, None, AsyncBody::Empty)?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCreate::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.azdfs_read(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        Ok((
            RpWrite::default(),
            AzdfsWriter::new(self.core.clone(), args, path.to_string()),
        ))
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        if let Some(resp) = self.core.azdfs_ensure_parent_path(to).await? {
            let status = resp.status();
            match status {
                StatusCode::CREATED | StatusCode::CONFLICT => {
                    resp.into_body().consume().await?;
                }
                _ => return Err(parse_error(resp).await?),
            }
        }

        let resp = self.core.azdfs_rename(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED => {
                resp.into_body().consume().await?;
                Ok(RpRename::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.core.azdfs_get_properties(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.azdfs_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let op = AzdfsPager::new(self.core.clone(), path.to_string(), args.limit());

        Ok((RpList::default(), op))
    }
}

fn infer_storage_name_from_endpoint(endpoint: &str) -> Option<String> {
    let endpoint: &str = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);

    let mut parts = endpoint.splitn(2, '.');
    let storage_name = parts.next();
    let endpoint_suffix = parts
        .next()
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_lowercase();

    if KNOWN_AZDFS_ENDPOINT_SUFFIX
        .iter()
        .any(|s| *s == endpoint_suffix.as_str())
    {
        storage_name.map(|s| s.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::AzdfsBuilder;
    use crate::services::azdfs::backend::infer_storage_name_from_endpoint;
    use crate::Builder;

    #[test]
    fn test_infer_storage_name_from_endpoint() {
        let endpoint = "https://account.dfs.core.windows.net";
        let storage_name = infer_storage_name_from_endpoint(endpoint);
        assert_eq!(storage_name, Some("account".to_string()));
    }

    #[test]
    fn test_infer_storage_name_from_endpoint_with_trailing_slash() {
        let endpoint = "https://account.dfs.core.windows.net/";
        let storage_name = infer_storage_name_from_endpoint(endpoint);
        assert_eq!(storage_name, Some("account".to_string()));
    }

    #[test]
    fn test_builder_from_endpoint_and_key_infer_account_name() {
        let mut azdfs_builder = AzdfsBuilder::default();
        azdfs_builder.endpoint("https://storagesample.dfs.core.chinacloudapi.cn");
        azdfs_builder.account_key("account-key");
        azdfs_builder.filesystem("filesystem");
        let azdfs = azdfs_builder
            .build()
            .expect("build azdfs should be succeeded.");

        assert_eq!(
            azdfs.core.endpoint,
            "https://storagesample.dfs.core.chinacloudapi.cn"
        );

        assert_eq!(azdfs.core.filesystem, "filesystem".to_string());

        assert_eq!(
            azdfs_builder.account_key.unwrap(),
            "account-key".to_string()
        );
    }

    #[test]
    fn test_no_key_wont_infer_account_name() {
        let mut azdfs_builder = AzdfsBuilder::default();
        azdfs_builder.endpoint("https://storagesample.dfs.core.windows.net");
        azdfs_builder.filesystem("filesystem");
        let azdfs = azdfs_builder
            .build()
            .expect("build azdfs should be succeeded.");

        assert_eq!(
            azdfs.core.endpoint,
            "https://storagesample.dfs.core.windows.net"
        );

        assert_eq!(azdfs.core.filesystem, "filesystem".to_string());

        assert_eq!(azdfs_builder.account_key, None);
    }
}
