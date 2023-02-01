// Copyright 2022 Datafuse Labs.
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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::mem;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
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
use crate::raw::*;
use crate::*;

/// Builder for azblob services
#[derive(Default, Clone)]
pub struct Builder {
    root: Option<String>,
    filesystem: String,
    endpoint: Option<String>,
    account_name: Option<String>,
    account_key: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for Builder {
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

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "filesystem" => builder.filesystem(v),
                "endpoint" => builder.endpoint(v),
                "account_name" => builder.account_name(v),
                "account_key" => builder.account_key(v),
                _ => continue,
            };
        }

        builder
    }

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

    /// Consume builder to build an azblob backend.
    pub fn build(&mut self) -> Result<impl Accessor> {
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
        Ok(apply_wrapper(Backend {
            root,
            endpoint,
            signer: Arc::new(signer),
            filesystem: self.filesystem.clone(),
            client,
            _account_name: mem::take(&mut self.account_name).unwrap_or_default(),
        }))
    }
}

/// Backend for azblob services.
#[derive(Debug, Clone)]
pub struct Backend {
    filesystem: String,
    client: HttpClient,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    signer: Arc<AzureStorageSigner>,
    _account_name: String,
}

#[async_trait]
impl Accessor for Backend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();

    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Azdfs)
            .set_root(&self.root)
            .set_name(&self.filesystem)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            )
            .set_hints(AccessorHint::ReadIsStreamable);

        am
    }

    fn create(&self, path: &str, args: OpCreate) -> FutureResult<RpCreate> {
        let fut = async {
            let resource = match args.mode() {
                ObjectMode::FILE => "file",
                ObjectMode::DIR => "directory",
                _ => unimplemented!("not supported object mode"),
            };

            let mut req = self.azdfs_create_request(path, resource, None, AsyncBody::Empty)?;

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
        };

        Box::pin(fut)
    }

    fn read(&self, path: &str, args: OpRead) -> FutureResult<(RpRead, Self::Reader)> {
        let fut = async {
            let resp = self.azdfs_read(path, args.range()).await?;

            let status = resp.status();

            match status {
                StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                    let meta = parse_into_object_metadata(path, resp.headers())?;
                    Ok((RpRead::with_metadata(meta), resp.into_body()))
                }
                _ => Err(parse_error(resp).await?),
            }
        };

        Box::pin(fut)
    }

    fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> FutureResult<RpWrite> {
        let fut = async {
            let mut req =
                self.azdfs_create_request(path, "file", args.content_type(), AsyncBody::Empty)?;

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

            let mut req =
                self.azdfs_update_request(path, Some(args.size()), AsyncBody::Reader(r))?;

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
        };

        Box::pin(fut)
    }

    fn stat(&self, path: &str, _: OpStat) -> FutureResult<RpStat> {
        let fut = async {
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
        };

        Box::pin(fut)
    }

    fn delete(&self, path: &str, _: OpDelete) -> FutureResult<RpDelete> {
        let fut = async {
            let resp = self.azdfs_delete(path).await?;

            let status = resp.status();

            match status {
                StatusCode::OK | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
                _ => Err(parse_error(resp).await?),
            }
        };

        Box::pin(fut)
    }

    fn list(&self, path: &str, _: OpList) -> FutureResult<(RpList, ObjectPager)> {
        let op = Box::new(DirStream::new(
            Arc::new(self.clone()),
            self.root.clone(),
            path.to_string(),
        ));

        Box::pin(future::ok((RpList::default(), op as ObjectPager)))
    }
}

impl Backend {
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
