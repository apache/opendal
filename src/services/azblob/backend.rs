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
use http::header::HeaderName;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use reqsign::AzureStorageSigner;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::accessor::AccessorCapability;
use crate::accessor::AccessorMetadata;
use crate::http_util::*;
use crate::object::ObjectMetadata;
use crate::object::ObjectPageStreamer;
use crate::ops::*;
use crate::path::build_abs_path;
use crate::path::normalize_root;
use crate::wrappers::wrapper;
use crate::*;

const X_MS_BLOB_TYPE: &str = "x-ms-blob-type";

/// Builder for azblob services
#[derive(Default, Clone)]
pub struct Builder {
    root: Option<String>,
    container: String,
    endpoint: Option<String>,
    account_name: Option<String>,
    account_key: Option<String>,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("root", &self.root);
        ds.field("container", &self.container);
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
                "container" => builder.container(v),
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

    /// Set container name of this backend.
    pub fn container(&mut self, container: &str) -> &mut Self {
        self.container = container.to_string();

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

    /// Consume builder to build an azblob backend.
    pub fn build(&mut self) -> Result<impl Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle endpoint, region and container name.
        let container = match self.container.is_empty() {
            false => Ok(&self.container),
            true => Err(
                Error::new(ErrorKind::BackendConfigInvalid, "container is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Azblob),
            ),
        }?;
        debug!("backend use container {}", &container);

        let endpoint = match &self.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(
                Error::new(ErrorKind::BackendConfigInvalid, "endpoint is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Azblob),
            ),
        }?;
        debug!("backend use endpoint {}", &container);

        let client = HttpClient::new();

        let mut signer_builder = AzureStorageSigner::builder();
        if let (Some(name), Some(key)) = (&self.account_name, &self.account_key) {
            signer_builder.account_name(name).account_key(key);
        }

        let signer = signer_builder.build().map_err(|e| {
            Error::new(ErrorKind::BackendConfigInvalid, "build AzureStorageSigner")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azblob)
                .with_context("endpoint", &endpoint)
                .with_context("container", container.as_str())
                .set_source(e)
        })?;

        debug!("backend build finished: {:?}", &self);
        Ok(wrapper(Backend {
            root,
            endpoint,
            signer: Arc::new(signer),
            container: self.container.clone(),
            client,
            _account_name: mem::take(&mut self.account_name).unwrap_or_default(),
        }))
    }
}

/// Backend for azblob services.
#[derive(Debug, Clone)]
pub struct Backend {
    container: String,
    client: HttpClient,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    signer: Arc<AzureStorageSigner>,
    _account_name: String,
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Azblob)
            .set_root(&self.root)
            .set_name(&self.container)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            );

        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut req = self.azblob_put_blob_request(path, Some(0), None, AsyncBody::Empty)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCreate::default())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(er);
                Err(err)
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let resp = self.azblob_get_blob(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_object_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body().reader()))
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(er);
                Err(err)
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        let mut req = self.azblob_put_blob_request(
            path,
            Some(args.size()),
            args.content_type(),
            AsyncBody::Reader(r),
        )?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpWrite::new(args.size()))
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(er);
                Err(err)
            }
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(ObjectMetadata::new(ObjectMode::DIR));
        }

        let resp = self.azblob_get_blob_properties(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_object_metadata(path, resp.headers()),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(ObjectMetadata::new(ObjectMode::DIR))
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(er);
                Err(err)
            }
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<()> {
        let resp = self.azblob_delete_blob(path).await?;

        let status = resp.status();

        match status {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => Ok(()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(er);
                Err(err)
            }
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<ObjectStreamer> {
        Ok(Box::new(ObjectPageStreamer::new(DirStream::new(
            Arc::new(self.clone()),
            self.root.clone(),
            path.to_string(),
        ))))
    }
}

impl Backend {
    async fn azblob_get_blob(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
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

    fn azblob_put_blob_request(
        &self,
        path: &str,
        size: Option<u64>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        if let Some(ty) = content_type {
            req = req.header(CONTENT_TYPE, ty)
        }

        req = req.header(HeaderName::from_static(X_MS_BLOB_TYPE), "BlockBlob");

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn azblob_get_blob_properties(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn azblob_delete_blob(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub(crate) async fn azblob_list_blobs(
        &self,
        path: &str,
        next_marker: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}?restype=container&comp=list&delimiter=/",
            self.endpoint, self.container
        );
        if !p.is_empty() {
            write!(url, "&prefix={}", percent_encode_path(&p))
                .expect("write into string must succeed");
        }
        if !next_marker.is_empty() {
            write!(url, "&marker={next_marker}").expect("write into string must succeed");
        }

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }
}
