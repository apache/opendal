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
use std::io;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BufMut;
use futures::TryStreamExt;
use http::header::HeaderName;
use http::Response;
use http::StatusCode;
use hyper::body::HttpBody;
use hyper::Body;
use log::debug;
use log::error;
use log::info;
use metrics::increment_counter;
use minitrace::trace;
use reqsign::services::azure::storage::Signer;
use time::format_description::well_known::Rfc2822;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use crate::accessor::AccessorMetadata;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
use crate::io_util::new_http_channel;
use crate::io_util::HttpBodyWriter;
use crate::object::ObjectMetadata;
use crate::ops::BytesRange;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::BytesReader;
use crate::BytesWriter;
use crate::ObjectMode;
use crate::Scheme;
use crate::{Accessor, DirStreamer};

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
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        info!("backend build started: {:?}", &self);

        let root = match &self.root {
            // Use "/" as root if user not specified.
            None => "/".to_string(),
            Some(v) => {
                let mut v = v
                    .split('/')
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<&str>>()
                    .join("/");
                if !v.starts_with('/') {
                    v.insert(0, '/');
                }
                if !v.ends_with('/') {
                    v.push('/')
                }
                v
            }
        };

        info!("backend use root {}", root);

        // Handle endpoint, region and container name.
        let container = match self.container.is_empty() {
            false => Ok(&self.container),
            true => Err(other(BackendError::new(
                HashMap::from([("container".to_string(), "".to_string())]),
                anyhow!("container is empty"),
            ))),
        }?;
        debug!("backend use container {}", &container);

        let endpoint = match &self.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(other(BackendError::new(
                HashMap::from([("endpoint".to_string(), "".to_string())]),
                anyhow!("endpoint is empty"),
            ))),
        }?;
        debug!("backend use endpoint {}", &container);

        let context = HashMap::from([
            ("container".to_string(), container.to_string()),
            ("endpoint".to_string(), endpoint.to_string()),
        ]);

        let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());

        let mut signer_builder = Signer::builder();
        if let (Some(name), Some(key)) = (&self.account_name, &self.account_key) {
            signer_builder.account_name(name).account_key(key);
        }

        let signer = signer_builder
            .build()
            .map_err(|e| other(BackendError::new(context, e)))?;

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend {
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
    client: hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    signer: Arc<Signer>,
    _account_name: String,
}

impl Backend {
    /// Create a builder for azblob.
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.trim_start_matches('/').to_string();
        }
        // root must be normalized like `/abc/`
        format!("{}{}", self.root, path)
            .trim_start_matches('/')
            .to_string()
    }

    pub(crate) fn get_rel_path(&self, path: &str) -> String {
        let path = format!("/{}", path);

        match path.strip_prefix(&self.root) {
            Some(v) => v.to_string(),
            None => unreachable!(
                "invalid path {} that not start with backend root {}",
                &path, &self.root
            ),
        }
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Azblob)
            .set_root(&self.root)
            .set_name(&self.container);

        am
    }

    #[trace("create")]
    async fn create(&self, args: &OpCreate) -> Result<()> {
        increment_counter!("opendal_azblob_create_requests");
        let p = self.get_abs_path(args.path());

        let req = self.put_blob(&p, 0, Body::empty()).await?;
        let resp = self.client.request(req).await.map_err(|e| {
            error!("object {} put_object: {:?}", args.path(), e);
            other(ObjectError::new("read", args.path(), e))
        })?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                debug!("object {} create finished", args.path());
                Ok(())
            }
            _ => Err(parse_error_response_without_body(
                resp,
                "write",
                args.path(),
            )),
        }
    }

    #[trace("read")]
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        increment_counter!("opendal_azblob_read_requests");

        let p = self.get_abs_path(args.path());
        debug!(
            "object {} read start: offset {:?}, size {:?}",
            &p,
            args.offset(),
            args.size()
        );

        let resp = self.get_blob(&p, args.offset(), args.size()).await?;
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

    #[trace("write")]
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        let p = self.get_abs_path(args.path());
        debug!("object {} write start: size {}", &p, args.size());

        let (tx, body) = new_http_channel();

        let req = self.put_blob(&p, args.size(), body).await?;

        let bs = HttpBodyWriter::new(args, tx, self.client.request(req), |op, resp| {
            match resp.status() {
                http::StatusCode::CREATED | http::StatusCode::OK => {
                    debug!("object {} write finished: size {:?}", op.path(), op.size());
                    Ok(())
                }
                _ => Err(parse_error_response_without_body(resp, "write", op.path())),
            }
        });

        Ok(Box::new(bs))
    }

    #[trace("stat")]
    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        increment_counter!("opendal_azure_stat_requests");

        let p = self.get_abs_path(args.path());
        debug!("object {} stat start", &p);

        // Stat root always returns a DIR.
        if self.get_rel_path(&p).is_empty() {
            let mut m = ObjectMetadata::default();
            m.set_mode(ObjectMode::DIR);

            debug!("backed root object stat finished");
            return Ok(m);
        }

        let resp = self.get_blob_properties(&p).await?;
        match resp.status() {
            http::StatusCode::OK => {
                let mut m = ObjectMetadata::default();

                // Parse content_length
                if let Some(v) = resp.headers().get(http::header::CONTENT_LENGTH) {
                    let v = v.to_str().map_err(|e| {
                        other(ObjectError::new(
                            "stat",
                            &p,
                            anyhow!("parse content-length header: {:?}", e),
                        ))
                    })?;
                    let v = u64::from_str(v).map_err(|e| {
                        other(ObjectError::new(
                            "stat",
                            &p,
                            anyhow!("parse content-length header: {:?}", e),
                        ))
                    })?;

                    m.set_content_length(v);
                }

                // Parse content_md5
                if let Some(v) = resp.headers().get(HeaderName::from_static("content-md5")) {
                    let v = v.to_str().map_err(|e| {
                        other(ObjectError::new(
                            "stat",
                            &p,
                            anyhow!("parse content-md5 header: {:?}", e),
                        ))
                    })?;

                    m.set_content_md5(v);
                }

                // Parse last_modified
                if let Some(v) = resp.headers().get(http::header::LAST_MODIFIED) {
                    let v = v.to_str().map_err(|e| {
                        other(ObjectError::new(
                            "stat",
                            &p,
                            anyhow!("parse last-modified header: {:?}", e),
                        ))
                    })?;
                    let t = OffsetDateTime::parse(v, &Rfc2822).map_err(|e| {
                        other(ObjectError::new(
                            "stat",
                            &p,
                            anyhow!("parse last-modified header: {:?}", e),
                        ))
                    })?;

                    m.set_last_modified(t);
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

    #[trace("delete")]
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        increment_counter!("opendal_azure_delete_requests");

        let p = self.get_abs_path(args.path());
        debug!("object {} delete start", &p);

        let resp = self.delete_blob(&p).await?;
        match resp.status() {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => {
                debug!("object {} delete finished", &p);
                Ok(())
            }
            _ => Err(parse_error_response_with_body(resp, "delete", &p).await),
        }
    }

    #[trace("list")]
    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        increment_counter!("opendal_azblob_list_requests");

        let path = self.get_abs_path(args.path());
        debug!("object {} list start", &path);

        Ok(Box::new(DirStream::new(Arc::new(self.clone()), &path)))
    }
}

impl Backend {
    #[trace("get_blob")]
    pub(crate) async fn get_blob(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<hyper::Response<hyper::Body>> {
        let url = format!("{}/{}/{}", self.endpoint, self.container, path);

        let mut req = hyper::Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        let mut req = req.body(hyper::Body::empty()).map_err(|e| {
            error!("object {path} get_blob: {url} {e:?}");
            other(ObjectError::new(
                "read",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} get_blob: {url} {e:?}");
            other(ObjectError::new(
                "read",
                path,
                anyhow!("sign request {url}: {e:?}"),
            ))
        })?;

        self.client.request(req).await.map_err(|e| {
            error!("object {path} get_blob: {url} {e:?}");
            other(ObjectError::new(
                "read",
                path,
                anyhow!("send request {url}: {e:?}"),
            ))
        })
    }

    #[trace("put_blob")]
    pub(crate) async fn put_blob(
        &self,
        path: &str,
        size: u64,
        body: Body,
    ) -> Result<hyper::Request<hyper::Body>> {
        let url = format!("{}/{}/{}", self.endpoint, self.container, path);

        let mut req = hyper::Request::put(&url);

        req = req.header(http::header::CONTENT_LENGTH, size.to_string());

        req = req.header(HeaderName::from_static(X_MS_BLOB_TYPE), "BlockBlob");

        // Set body
        let mut req = req.body(body).map_err(|e| {
            error!("object {path} put_blob: {url} {e:?}");
            other(ObjectError::new(
                "write",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} put_blob: {url} {e:?}");
            other(ObjectError::new(
                "write",
                path,
                anyhow!("sign request {url}: {e:?}"),
            ))
        })?;

        Ok(req)
    }

    #[trace("get_blob_properties")]
    pub(crate) async fn get_blob_properties(
        &self,
        path: &str,
    ) -> Result<hyper::Response<hyper::Body>> {
        let url = format!("{}/{}/{}", self.endpoint, self.container, path);

        let req = hyper::Request::head(&url);

        let mut req = req.body(hyper::Body::empty()).map_err(|e| {
            error!("object {path} get_blob_properties: {url} {e:?}");
            other(ObjectError::new(
                "stat",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} get_blob_properties: {url} {e:?}");
            other(ObjectError::new(
                "stat",
                path,
                anyhow!("sign request {url}: {e:?}"),
            ))
        })?;

        self.client.request(req).await.map_err(|e| {
            error!("object {path} get_blob_properties: {url} {e:?}");
            other(ObjectError::new(
                "stat",
                path,
                anyhow!("send request {url}: {e:?}"),
            ))
        })
    }

    #[trace("delete_blob")]
    pub(crate) async fn delete_blob(&self, path: &str) -> Result<hyper::Response<hyper::Body>> {
        let url = format!("{}/{}/{}", self.endpoint, self.container, path);

        let req = hyper::Request::delete(&url);

        let mut req = req.body(hyper::Body::empty()).map_err(|e| {
            error!("object {path} delete_blob: {url} {e:?}");
            other(ObjectError::new(
                "delete",
                path,
                anyhow!("build request {url}: {e:?}"),
            ))
        })?;

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {path} delete_blob: {url} {e:?}");
            other(ObjectError::new(
                "delete",
                path,
                anyhow!("sign request {url}: {e:?}"),
            ))
        })?;

        self.client.request(req).await.map_err(|e| {
            error!("object {path} delete_object: {url} {e:?}");
            other(ObjectError::new(
                "delete",
                path,
                anyhow!("send request {url}: {e:?}"),
            ))
        })
    }

    #[trace("list_blobs")]
    pub(crate) async fn list_blobs(
        &self,
        path: &str,
        next_marker: &str,
    ) -> Result<hyper::Response<hyper::Body>> {
        let mut url = format!(
            "{}/{}?restype=container&comp=list&delimiter=/",
            self.endpoint, self.container
        );
        if !path.is_empty() {
            url.push_str(&format!("&prefix={path}"))
        }
        if !next_marker.is_empty() {
            url.push_str(&format!("&marker={next_marker}"))
        }

        let mut req = hyper::Request::get(&url)
            .body(hyper::Body::empty())
            .map_err(|e| {
                error!("object {path} list_blobs: {url} {e:?}");
                other(ObjectError::new(
                    "list",
                    path,
                    anyhow!("build request {url}: {e:?}"),
                ))
            })?;

        self.signer.sign(&mut req).map_err(|e| {
            error!("object {} list_blobs: {url} {:?}", path, e);
            other(ObjectError::new(
                "list",
                path,
                anyhow!("sign request {url}: {:?}", e),
            ))
        })?;

        self.client.request(req).await.map_err(|e| {
            error!("object {path} list_blobs: {url} {e:?}");
            other(ObjectError::new(
                "list",
                path,
                anyhow!("send request {url}: {e:?}"),
            ))
        })
    }
}

fn parse_error_response_without_body(resp: Response<Body>, op: &'static str, path: &str) -> Error {
    let (part, _) = resp.into_parts();
    let kind = match part.status {
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::FORBIDDEN => ErrorKind::PermissionDenied,
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => ErrorKind::Interrupted,
        _ => ErrorKind::Other,
    };

    io::Error::new(
        kind,
        ObjectError::new(op, path, anyhow!("response part: {:?}", part)),
    )
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
            Err(e) => return other(anyhow!("parse error response parse: {:?}", e)),
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
