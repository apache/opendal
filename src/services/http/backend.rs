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

use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BufMut;
use futures::TryStreamExt;
use http::Response;
use http::StatusCode;
use hyper::body::HttpBody;
use hyper::Body;
use log::debug;
use log::error;
use log::info;
use radix_trie::Trie;
use radix_trie::TrieCommon;

use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
use crate::io_util::parse_content_length;
use crate::io_util::parse_content_md5;
use crate::io_util::parse_etag;
use crate::io_util::parse_last_modified;
use crate::io_util::HttpClient;
use crate::ops::BytesRange;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::BytesWriter;
use crate::DirEntry;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Scheme;

/// Builder for http backend.
#[derive(Default)]
pub struct Builder {
    endpoint: Option<String>,
    root: Option<String>,
    index: Trie<String, ()>,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Builder");
        de.field("endpoint", &self.endpoint);
        de.field("root", &self.root);
        de.field("index", &format!("length: {}", self.index.len()));

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

    /// Insert index into backend.
    pub fn insert_index(&mut self, key: &str) -> &mut Self {
        if key.is_empty() {
            return self;
        }

        let key = if key.starts_with('/') {
            key.to_string()
        } else {
            format!("/{key}")
        };

        self.index.insert(key, ());
        self
    }

    /// Extend index from an iterator.
    pub fn extend_index<'a>(&mut self, it: impl Iterator<Item = &'a str>) -> &mut Self {
        for k in it.filter(|v| !v.is_empty()) {
            let k = if k.starts_with('/') {
                k.to_string()
            } else {
                format!("/{k}")
            };

            self.index.insert(k, ());
        }
        self
    }

    /// Build a HTTP backend.
    pub async fn build(&mut self) -> Result<Arc<dyn Accessor>> {
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

        // Make `/` as the default of root.
        let root = match &self.root {
            None => "/".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty());

                let mut v = v.clone();
                if !v.starts_with('/') {
                    return Err(other(BackendError::new(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("root must start with /"),
                    )));
                }
                if !v.ends_with('/') {
                    v.push('/');
                }

                v
            }
        };

        let client = HttpClient::new();

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend {
            endpoint: endpoint.to_string(),
            root,
            client,
            index: Arc::new(mem::take(&mut self.index)),
        }))
    }
}

/// Backend is used to serve `Accessor` support for http.
#[derive(Debug, Clone)]
pub struct Backend {
    endpoint: String,
    root: String,
    client: HttpClient,
    index: Arc<Trie<String, ()>>,
}

impl Backend {
    /// Create a new builder for s3.
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.to_string();
        }

        // root must be normalized like `/abc/`
        format!("{}{}", self.root, path)
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut ma = AccessorMetadata::default();
        ma.set_scheme(Scheme::Http).set_root(&self.root);

        ma
    }

    async fn create(&self, _: &OpCreate) -> Result<()> {
        Err(ErrorKind::Unsupported.into())
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let p = self.get_abs_path(args.path());
        debug!(
            "object {} read start: offset {:?}, size {:?}",
            &p,
            args.offset(),
            args.size()
        );

        let resp = self
            .http_get(&p, args.offset(), args.size())
            .await
            .map_err(|e| {
                error!("object {} http_get: {:?}", p, e);
                e
            })?;

        match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                debug!(
                    "object {} reader created: offset {:?}, size {:?}",
                    &p,
                    args.offset(),
                    args.size()
                );

                Ok(Box::new(
                    resp.into_body()
                        .into_stream()
                        .map_err(move |e| other(ObjectError::new("read", &p, e)))
                        .into_async_read(),
                ))
            }
            _ => Err(parse_error_response_with_body(resp, "read", &p).await),
        }
    }

    async fn write(&self, _: &OpWrite) -> Result<BytesWriter> {
        Err(ErrorKind::Unsupported.into())
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let p = self.get_abs_path(args.path());
        debug!("object {} stat start", &p);

        // Stat root always returns a DIR.
        if p == self.root {
            let mut m = ObjectMetadata::default();
            m.set_mode(ObjectMode::DIR);

            debug!("backed root object stat finished");
            return Ok(m);
        }

        let resp = self.http_head(&p).await?;

        match resp.status() {
            StatusCode::OK => {
                let mut m = ObjectMetadata::default();

                if let Some(v) = parse_content_length(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_content_md5(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_content_md5(v);
                }

                if let Some(v) = parse_etag(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_etag(v);
                }

                if let Some(v) = parse_last_modified(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_last_modified(v);
                }

                if p.ends_with('/') {
                    m.set_mode(ObjectMode::DIR);
                } else {
                    m.set_mode(ObjectMode::FILE);
                };

                debug!("object {} stat finished: {:?}", &p, m);
                Ok(m)
            }
            StatusCode::NOT_FOUND if p.ends_with('/') => {
                let mut m = ObjectMetadata::default();
                m.set_mode(ObjectMode::DIR);

                debug!("object {} stat finished", &p);
                Ok(m)
            }
            _ => Err(parse_error_response_with_body(resp, "stat", &p).await),
        }
    }

    async fn delete(&self, _: &OpDelete) -> Result<()> {
        Err(ErrorKind::Unsupported.into())
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = args.path().to_string();

        let paths = match self.index.subtrie(&path) {
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    ObjectError::new("list", &path, anyhow!("no such dir")),
                ))
            }
            Some(trie) => trie
                .keys()
                // Make sure k is at the same level with input path.
                .filter(|k| match k[path.len()..].find('/') {
                    None => true,
                    Some(idx) => idx + 1 + path.len() == k.len(),
                })
                .map(|v| v.to_string())
                .collect::<Vec<_>>(),
        };

        debug!("dir object {} listed keys: {paths:?}", path);
        Ok(Box::new(DirStream {
            backend: Arc::new(self.clone()),
            path,
            paths,
            idx: 0,
        }))
    }
}

impl Backend {
    pub(crate) async fn http_get(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<hyper::Response<hyper::Body>> {
        let url = format!("{}{}", self.endpoint, path);

        let mut req = hyper::Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        let req = req.body(hyper::Body::empty()).map_err(|e| {
            error!("object {path} http_get: {url} {e:?}");
            other(ObjectError::new(
                "read",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        self.client.request(req).await.map_err(|e| {
            error!("object {path} http_get: {url} {e:?}");
            other(ObjectError::new(
                "read",
                path,
                anyhow!("send request: {url}: {e:?}"),
            ))
        })
    }

    pub(crate) async fn http_head(&self, path: &str) -> Result<hyper::Response<hyper::Body>> {
        let url = format!("{}{}", self.endpoint, path);

        let req = hyper::Request::head(&url);

        let req = req.body(hyper::Body::empty()).map_err(|e| {
            error!("object {path} http_head: {url} {e:?}");
            other(ObjectError::new(
                "stat",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        self.client.request(req).await.map_err(|e| {
            error!("object {path} http_head: {url} {e:?}");
            other(ObjectError::new(
                "stat",
                path,
                anyhow!("send request {url}: {e:?}"),
            ))
        })
    }
}

struct DirStream {
    backend: Arc<Backend>,
    path: String,
    paths: Vec<String>,
    idx: usize,
}

impl futures::Stream for DirStream {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.idx >= self.paths.len() {
            return Poll::Ready(None);
        }

        let idx = self.idx;
        self.idx += 1;

        let path = self.paths.get(idx).expect("path must valid");
        let path = path
            .strip_prefix(&self.path)
            .expect("must start will list base");

        let de = if path.ends_with('/') {
            DirEntry::new(self.backend.clone(), ObjectMode::DIR, path)
        } else {
            DirEntry::new(self.backend.clone(), ObjectMode::FILE, path)
        };

        debug!(
            "dir object {} got entry, mode: {}, path: {}",
            &self.path,
            de.mode(),
            de.path()
        );
        Poll::Ready(Some(Ok(de)))
    }
}

// Read and decode whole error response.
async fn parse_error_response_with_body(
    resp: Response<Body>,
    op: &'static str,
    path: &str,
) -> Error {
    let (part, mut body) = resp.into_parts();
    let kind = match part.status {
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::FORBIDDEN => ErrorKind::PermissionDenied,
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => ErrorKind::Interrupted,
        _ => ErrorKind::Other,
    };

    // Only read 4KiB from the response to avoid broken services.
    let mut bs = Vec::new();
    let mut limit = 4 * 1024;

    while let Some(b) = body.data().await {
        match b {
            Ok(b) => {
                bs.put_slice(&b[..min(b.len(), limit)]);
                limit -= b.len();
                if limit == 0 {
                    break;
                }
            }
            Err(e) => {
                return other(ObjectError::new(
                    op,
                    path,
                    anyhow!("parse error response parse: {:?}", e),
                ))
            }
        }
    }

    Error::new(
        kind,
        ObjectError::new(
            op,
            path,
            anyhow!(
                "response part: {:?}, body: {:?}",
                part,
                String::from_utf8_lossy(&bs)
            ),
        ),
    )
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

        let mut builder = Backend::build();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        builder.insert_index("/hello");
        let op = Operator::new(builder.build().await?);

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

        let mut builder = Backend::build();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        builder.insert_index("/hello");
        let op = Operator::new(builder.build().await?);

        let bs = op.object("hello").metadata().await?;

        assert_eq!(bs.mode(), ObjectMode::FILE);
        assert_eq!(bs.content_length(), 128);
        Ok(())
    }

    #[tokio::test]
    async fn test_list() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let mock_server = MockServer::start().await;

        let mut builder = Backend::build();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        builder.insert_index("/hello");
        builder.insert_index("/world");
        builder.insert_index("/another/");
        let op = Operator::new(builder.build().await?);

        let bs = op.object("/").list().await?;
        let paths = bs.try_collect::<Vec<_>>().await?;
        let paths = paths
            .into_iter()
            .map(|v| v.path().to_string())
            .collect::<Vec<_>>();

        assert_eq!(paths, vec!["another/", "hello", "world"]);
        Ok(())
    }
}
