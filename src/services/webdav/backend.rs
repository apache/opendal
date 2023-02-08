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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use async_trait::async_trait;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;

use super::error::parse_error;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) backend support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [ ] list
/// - [ ] ~~presign~~
/// - [ ] ~~multipart~~
/// - [ ] blocking
///
/// # Notes
///
/// Bazel Remote Caching and Ccache HTTP Storage is also part of this service.
/// Users can use `webdav` to connect those services.
///
/// # Status
///
/// - `list` is not supported so far.
///
/// # Configuration
///
/// - `endpoint`: set the endpoint for webdav
/// - `root`: Set the work directory for backend
///
/// You can refer to [`WebdavBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Webdav;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Webdav::default();
///
///     builder.endpoint("127.0.0.1");
///
///     let op: Operator = Operator::create(builder)?.finish();
///     let _obj: Object = op.object("test_file");
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct WebdavBuilder {
    endpoint: Option<String>,
    root: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for WebdavBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Builder");
        de.field("endpoint", &self.endpoint);
        de.field("root", &self.root);

        de.finish()
    }
}

impl WebdavBuilder {
    /// Set endpoint for http backend.
    ///
    /// For example: `https://example.com`
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// Set root path of http backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

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

impl Builder for WebdavBuilder {
    const SCHEME: Scheme = Scheme::Webdav;
    type Accessor = WebdavBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = WebdavBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let endpoint = match &self.endpoint {
            Some(v) => v,
            None => {
                return Err(
                    Error::new(ErrorKind::BackendConfigInvalid, "endpoint is empty")
                        .with_context("service", Scheme::Webdav),
                )
            }
        };

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Webdav)
            })?
        };

        debug!("backend build finished: {:?}", &self);
        Ok(WebdavBackend {
            endpoint: endpoint.to_string(),
            root,
            client,
        })
    }
}
/// Backend is used to serve `Accessor` support for http.
#[derive(Clone)]
pub struct WebdavBackend {
    endpoint: String,
    root: String,
    client: HttpClient,
}

impl Debug for WebdavBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("client", &self.client)
            .finish()
    }
}

#[async_trait]
impl Accessor for WebdavBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();

    fn metadata(&self) -> AccessorMetadata {
        let mut ma = AccessorMetadata::default();
        ma.set_scheme(Scheme::Webdav)
            .set_root(&self.root)
            .set_capabilities(AccessorCapability::Read | AccessorCapability::Write)
            .set_hints(AccessorHint::ReadIsStreamable);

        ma
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let resp = self
            .webdav_put(path, Some(0), None, AsyncBody::Empty)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED
            | StatusCode::OK
            // create existing dir will retrun conflict
            | StatusCode::CONFLICT
            // create existing file will return no_content
            | StatusCode::NO_CONTENT => {
                resp.into_body().consume().await?;
                Ok(RpCreate::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.webdav_get(path, args.range()).await?;

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
        let resp = self
            .webdav_put(
                path,
                Some(args.size()),
                args.content_type(),
                AsyncBody::Reader(r),
            )
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpWrite::new(args.size()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)));
        }

        let resp = self.webdav_head(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_object_metadata(path, resp.headers()).map(RpStat::new),
            // HTTP Server like nginx could return FORBIDDEN if auto-index
            // is not enabled, we should ignore them.
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN if path.ends_with('/') => {
                Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.webdav_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }
}

impl WebdavBackend {
    async fn webdav_get(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn webdav_put(
        &self,
        path: &str,
        size: Option<u64>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(CONTENT_TYPE, mime)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn webdav_head(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let req = Request::head(&url);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn webdav_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let req = Request::delete(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }
}
