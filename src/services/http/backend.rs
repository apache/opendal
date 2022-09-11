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
use std::io::Result;

use anyhow::anyhow;
use async_trait::async_trait;
use http::Request;
use http::Response;
use http::StatusCode;
use log::info;

use super::error::parse_error;
use crate::accessor::AccessorCapability;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::parse_content_length;
use crate::http_util::parse_content_md5;
use crate::http_util::parse_error_response;
use crate::http_util::parse_etag;
use crate::http_util::parse_last_modified;
use crate::http_util::percent_encode_path;
use crate::http_util::AsyncBody;
use crate::http_util::HttpClient;
use crate::ops::BytesRange;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::Operation;
use crate::path::build_rooted_abs_path;
use crate::path::normalize_root;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Scheme;

/// Builder for http backend.
#[derive(Default)]
pub struct Builder {
    endpoint: Option<String>,
    root: Option<String>,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Builder");
        de.field("endpoint", &self.endpoint);
        de.field("root", &self.root);

        de.finish()
    }
}

impl Builder {
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

    /// Build a HTTP backend.
    pub fn build(&mut self) -> Result<Backend> {
        info!("backend build started: {:?}", &self);

        let endpoint = match &self.endpoint {
            None => {
                return Err(other(BackendError::new(
                    HashMap::new(),
                    anyhow!("endpoint must be specified"),
                )))
            }
            Some(v) => v,
        };

        let root = normalize_root(&self.root.take().unwrap_or_default());
        info!("backend use root {}", root);

        let client = HttpClient::new();

        info!("backend build finished: {:?}", &self);
        Ok(Backend {
            endpoint: endpoint.to_string(),
            root,
            client,
        })
    }
}

/// Backend is used to serve `Accessor` support for http.
#[derive(Clone)]
pub struct Backend {
    endpoint: String,
    root: String,
    client: HttpClient,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("client", &self.client)
            .finish()
    }
}

impl Backend {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "endpoint" => builder.endpoint(v),
                _ => continue,
            };
        }

        builder.build()
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut ma = AccessorMetadata::default();
        ma.set_scheme(Scheme::Http)
            .set_root(&self.root)
            .set_capabilities(AccessorCapability::Read);

        ma
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let p = build_rooted_abs_path(&self.root, args.path());

        let resp = self.http_get(&p, args.offset(), args.size()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(resp.into_body().reader()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Read, args.path(), er);
                Err(err)
            }
        }
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let p = build_rooted_abs_path(&self.root, args.path());

        // Stat root always returns a DIR.
        if p == self.root {
            let mut m = ObjectMetadata::default();
            m.set_mode(ObjectMode::DIR);

            return Ok(m);
        }

        let resp = self.http_head(&p).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut m = ObjectMetadata::default();

                if let Some(v) = parse_content_length(resp.headers())
                    .map_err(|e| other(ObjectError::new(Operation::Stat, &p, e)))?
                {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_content_md5(resp.headers())
                    .map_err(|e| other(ObjectError::new(Operation::Stat, &p, e)))?
                {
                    m.set_content_md5(v);
                }

                if let Some(v) = parse_etag(resp.headers())
                    .map_err(|e| other(ObjectError::new(Operation::Stat, &p, e)))?
                {
                    m.set_etag(v);
                }

                if let Some(v) = parse_last_modified(resp.headers())
                    .map_err(|e| other(ObjectError::new(Operation::Stat, &p, e)))?
                {
                    m.set_last_modified(v);
                }

                if p.ends_with('/') {
                    m.set_mode(ObjectMode::DIR);
                } else {
                    m.set_mode(ObjectMode::FILE);
                };

                Ok(m)
            }
            // HTTP Server like nginx could return FORBIDDEN if auto-index
            // is not enabled, we should ignore them.
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN if p.ends_with('/') => {
                let mut m = ObjectMetadata::default();
                m.set_mode(ObjectMode::DIR);

                Ok(m)
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Stat, args.path(), er);
                Err(err)
            }
        }
    }
}

impl Backend {
    async fn http_get(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Response<AsyncBody>> {
        let url = format!("{}{}", self.endpoint, percent_encode_path(path));

        let mut req = Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Read, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Read, path, e))
    }

    async fn http_head(&self, path: &str) -> Result<Response<AsyncBody>> {
        let url = format!("{}{}", self.endpoint, percent_encode_path(path));

        let req = Request::head(&url);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Stat, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Stat, path, e))
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
            .respond_with(ResponseTemplate::new(200).set_body_string("Hello, World!"))
            .mount(&mock_server)
            .await;

        let mut builder = Builder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        let op = Operator::new(builder.build()?);

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

        let mut builder = Builder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        let op = Operator::new(builder.build()?);

        let bs = op.object("hello").metadata().await?;

        assert_eq!(bs.mode(), ObjectMode::FILE);
        assert_eq!(bs.content_length(), 128);
        Ok(())
    }
}
