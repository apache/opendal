// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::mem;
use std::sync::Arc;

use async_trait::async_trait;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use reqsign::AzureStorageSigner;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::object::ObjectMetadata;
use crate::ops::*;
use crate::raw::*;
use crate::*;

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
/// - [x] list
/// - [ ] ~~scan~~
/// - [ ] presign
/// - [ ] multipart
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
/// use opendal::Object;
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
///     let op: Operator = Operator::create(builder)?.finish();
///
///     // Create an object handle to start operation on object.
///     let _: Object = op.object("test_file");
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
            true => Err(
                Error::new(ErrorKind::BackendConfigInvalid, "filesystem is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Azdfs),
            ),
        }?;
        debug!("backend use filesystem {}", &filesystem);

        let endpoint = match &self.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(
                Error::new(ErrorKind::BackendConfigInvalid, "endpoint is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Azdfs),
            ),
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

        let mut signer_builder = AzureStorageSigner::builder();
        if let (Some(name), Some(key)) = (&self.account_name, &self.account_key) {
            signer_builder.account_name(name).account_key(key);
        }

        let signer = signer_builder.build().map_err(|e| {
            Error::new(ErrorKind::BackendConfigInvalid, "build AzureStorageSigner")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azdfs)
                .with_context("endpoint", &endpoint)
                .with_context("container", filesystem.as_str())
                .set_source(e)
        })?;

        debug!("backend build finished: {:?}", &self);
        Ok(AzdfsBackend {
            root,
            endpoint,
            signer: Arc::new(signer),
            filesystem: self.filesystem.clone(),
            client,
            _account_name: mem::take(&mut self.account_name).unwrap_or_default(),
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
    filesystem: String,
    client: HttpClient,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    signer: Arc<AzureStorageSigner>,
    _account_name: String,
}

#[async_trait]
impl Accessor for AzdfsBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Pager = DirStream;
    type BlockingPager = ();

    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Azdfs)
            .set_root(&self.root)
            .set_name(&self.filesystem)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            )
            .set_hints(AccessorHint::ReadStreamable);

        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let resource = match args.mode() {
            ObjectMode::FILE => "file",
            ObjectMode::DIR => "directory",
            _ => unimplemented!("not supported object mode"),
        };

        let mut req = self.azdfs_create_request(path, resource, None, None, AsyncBody::Empty)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

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
        let resp = self.azdfs_read(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_object_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let mut req = self.azdfs_create_request(
            path,
            "file",
            args.content_type(),
            args.content_disposition(),
            AsyncBody::Empty,
        )?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
            }
            _ => {
                return Err(parse_error(resp)
                    .await?
                    .with_operation("Backend::azdfs_create_request"));
            }
        }

        let mut req = self.azdfs_update_request(path, Some(args.size()), AsyncBody::Reader(r))?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::ACCEPTED => {
                resp.into_body().consume().await?;
                Ok(RpWrite::new(args.size()))
            }
            _ => Err(parse_error(resp)
                .await?
                .with_operation("Backend::azdfs_update_request")),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)));
        }

        let resp = self.azdfs_get_properties(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_object_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.azdfs_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let op = DirStream::new(
            Arc::new(self.clone()),
            self.root.clone(),
            path.to_string(),
            args.limit(),
        );

        Ok((RpList::default(), op))
    }
}

impl AzdfsBackend {
    async fn azdfs_read(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            // azblob doesn't support read with suffix range.
            //
            // ref: https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-blob-service-operations
            if range.offset().is_none() && range.size().is_some() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "azblob doesn't support read with suffix range",
                ));
            }

            req = req.header(http::header::RANGE, range.to_header());
        }

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    /// resource should be one of `file` or `directory`
    ///
    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
    fn azdfs_create_request(
        &self,
        path: &str,
        resource: &str,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?resource={resource}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // Content length must be 0 for create request.
        req = req.header(CONTENT_LENGTH, 0);

        if let Some(ty) = content_type {
            req = req.header(CONTENT_TYPE, ty)
        }

        if let Some(pos) = content_disposition {
            req = req.header(CONTENT_DISPOSITION, pos)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    fn azdfs_update_request(
        &self,
        path: &str,
        size: Option<u64>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        // - close: Make this is the final action to this file.
        // - flush: Flush the file directly.
        let url = format!(
            "{}/{}/{}?action=append&close=true&flush=true&position=0",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::patch(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn azdfs_get_properties(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?action=getStatus",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn azdfs_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub(crate) async fn azdfs_list(
        &self,
        path: &str,
        continuation: &str,
        limit: Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = format!(
            "{}/{}?resource=filesystem&recursive=false",
            self.endpoint, self.filesystem
        );
        if !p.is_empty() {
            write!(url, "&directory={}", percent_encode_path(&p))
                .expect("write into string must succeed");
        }
        if let Some(limit) = limit {
            write!(url, "&maxresults={limit}").expect("write into string must succeed");
        }
        if !continuation.is_empty() {
            write!(url, "&continuation={continuation}").expect("write into string must succeed");
        }

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }
}
