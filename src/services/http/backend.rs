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
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;

use super::error::parse_error;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// HTTP Read-only service support like Nginx and Caddy.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [ ] ~~write~~
/// - [ ] ~~list~~
/// - [ ] ~~presign~~
/// - [ ] ~~multipart~~
/// - [ ] blocking
///
/// # Notes
///
/// Only `read` ans `stat` are supported. We can use this service to visit any
/// HTTP Server like nginx, caddy.
///
/// # Configuration
///
/// - `endpoint`: set the endpoint for http
/// - `root`: Set the work directory for backend
///
/// You can refer to [`HttpBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Http;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Http::default();
///
///     builder.endpoint("127.0.0.1");
///
///     let op: Operator = Operator::create(builder)?.finish();
///     let _obj: Object = op.object("test_file");
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct HttpBuilder {
    endpoint: Option<String>,
    root: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for HttpBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Builder");
        de.field("endpoint", &self.endpoint);
        de.field("root", &self.root);

        de.finish()
    }
}

impl HttpBuilder {
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

impl Builder for HttpBuilder {
    const SCHEME: Scheme = Scheme::Http;
    type Accessor = HttpBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = HttpBuilder::default();

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
                        .with_context("service", Scheme::Http),
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
                    .with_context("service", Scheme::Http)
            })?
        };

        debug!("backend build finished: {:?}", &self);
        Ok(HttpBackend {
            endpoint: endpoint.to_string(),
            root,
            client,
        })
    }
}

/// Backend is used to serve `Accessor` support for http.
#[derive(Clone)]
pub struct HttpBackend {
    endpoint: String,
    root: String,
    client: HttpClient,
}

impl Debug for HttpBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("client", &self.client)
            .finish()
    }
}

#[async_trait]
impl Accessor for HttpBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();

    fn metadata(&self) -> AccessorMetadata {
        let mut ma = AccessorMetadata::default();
        ma.set_scheme(Scheme::Http)
            .set_root(&self.root)
            .set_capabilities(AccessorCapability::Read)
            .set_hints(AccessorHint::ReadIsStreamable);

        ma
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.http_get(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_object_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)));
        }

        let resp = self.http_head(path).await?;

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
}

impl HttpBackend {
    async fn http_get(&self, path: &str, range: BytesRange) -> Result<Response<IncomingAsyncBody>> {
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

    async fn http_head(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let req = Request::head(&url);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use wiremock::matchers::method;
    use wiremock::matchers::path;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;

    use super::*;
    use crate::Operator;

    #[tokio::test]
    async fn test_read() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/hello"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "13")
                    .set_body_string("Hello, World!"),
            )
            .mount(&mock_server)
            .await;

        let mut builder = HttpBuilder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        let op = Operator::create(builder)?.finish();

        let bs = op.object("hello").read().await?;

        assert_eq!(bs, b"Hello, World!");
        Ok(())
    }

    #[tokio::test]
    async fn test_stat() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let mock_server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/hello"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "128"))
            .mount(&mock_server)
            .await;

        let mut builder = HttpBuilder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        let op = Operator::create(builder)?.finish();

        let bs = op.object("hello").metadata().await?;

        assert_eq!(bs.mode(), ObjectMode::FILE);
        assert_eq!(bs.content_length(), 128);
        Ok(())
    }
}
