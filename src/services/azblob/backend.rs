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
use std::fmt::Write;
use std::io::Result;
use std::mem;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use http::header::HeaderName;
use http::StatusCode;
use isahc::AsyncBody;
use log::debug;
use log::info;
use reqsign::services::azure::storage::Signer;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::accessor::AccessorMetadata;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
use crate::http_util::new_http_channel;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::new_request_sign_error;
use crate::http_util::parse_content_length;
use crate::http_util::parse_error_response;
use crate::http_util::parse_etag;
use crate::http_util::parse_last_modified;
use crate::http_util::percent_encode_path;
use crate::http_util::HttpBodyWriter;
use crate::http_util::HttpClient;
use crate::object::ObjectMetadata;
use crate::ops::BytesRange;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BytesReader;
use crate::BytesWriter;
use crate::DirStreamer;
use crate::ObjectMode;
use crate::Scheme;

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
    pub fn build(&mut self) -> Result<Backend> {
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

        let client = HttpClient::new();

        let mut signer_builder = Signer::builder();
        if let (Some(name), Some(key)) = (&self.account_name, &self.account_key) {
            signer_builder.account_name(name).account_key(key);
        }

        let signer = signer_builder
            .build()
            .map_err(|e| other(BackendError::new(context, e)))?;

        info!("backend build finished: {:?}", &self);
        Ok(Backend {
            root,
            endpoint,
            signer: Arc::new(signer),
            container: self.container.clone(),
            client,
            _account_name: mem::take(&mut self.account_name).unwrap_or_default(),
        })
    }

    /// Consume builder to build an azblob backend.
    #[deprecated = "Use Builder::build() instead"]
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        Ok(Arc::new(self.build()?))
    }
}

/// Backend for azblob services.
#[derive(Debug, Clone)]
pub struct Backend {
    container: String,
    client: HttpClient,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    signer: Arc<Signer>,
    _account_name: String,
}

impl Backend {
    /// Create a builder for azblob.
    #[deprecated = "Use Builder::default() instead"]
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
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

        builder.build()
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
            .set_name(&self.container)
            .set_capabilities(None);

        am
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        let p = self.get_abs_path(args.path());

        let req = self
            .put_blob(&p, 0, AsyncBody::from_bytes_static(""))
            .await?;
        let resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("create", args.path(), e))?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("create", args.path(), er);
                Err(err)
            }
        }
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let p = self.get_abs_path(args.path());

        let resp = self.get_blob(&p, args.offset(), args.size()).await?;
        match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(Box::new(resp.into_body())),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("read", args.path(), er);
                Err(err)
            }
        }
    }

    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        let p = self.get_abs_path(args.path());
        let (tx, body) = new_http_channel(args.size());

        let req = self.put_blob(&p, args.size(), body).await?;

        let bs = HttpBodyWriter::new(
            args,
            tx,
            self.client.send_async(req),
            |v| v == StatusCode::CREATED || v == StatusCode::OK,
            parse_error,
        );

        Ok(Box::new(bs))
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let p = self.get_abs_path(args.path());

        // Stat root always returns a DIR.
        if self.get_rel_path(&p).is_empty() {
            let mut m = ObjectMetadata::default();
            m.set_mode(ObjectMode::DIR);
            return Ok(m);
        }

        let resp = self.get_blob_properties(&p).await?;
        match resp.status() {
            http::StatusCode::OK => {
                let mut m = ObjectMetadata::default();

                if let Some(v) = parse_content_length(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_etag(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_etag(v);
                    m.set_content_md5(v.trim_matches('"'));
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

                Ok(m)
            }
            StatusCode::NOT_FOUND if p.ends_with('/') => {
                let mut m = ObjectMetadata::default();
                m.set_mode(ObjectMode::DIR);

                Ok(m)
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("stat", args.path(), er);
                Err(err)
            }
        }
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let p = self.get_abs_path(args.path());

        let resp = self.delete_blob(&p).await?;
        match resp.status() {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => Ok(()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("delete", args.path(), er);
                Err(err)
            }
        }
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = self.get_abs_path(args.path());

        Ok(Box::new(DirStream::new(Arc::new(self.clone()), &path)))
    }
}

impl Backend {
    pub(crate) async fn get_blob(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<isahc::Response<isahc::AsyncBody>> {
        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(path)
        );

        let mut req = isahc::Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        let mut req = req
            .body(isahc::AsyncBody::empty())
            .map_err(|e| new_request_build_error("read", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("read", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("read", path, e))
    }

    pub(crate) async fn put_blob(
        &self,
        path: &str,
        size: u64,
        body: AsyncBody,
    ) -> Result<isahc::Request<isahc::AsyncBody>> {
        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(path)
        );

        let mut req = isahc::Request::put(&url);

        req = req.header(http::header::CONTENT_LENGTH, size.to_string());

        req = req.header(HeaderName::from_static(X_MS_BLOB_TYPE), "BlockBlob");

        // Set body
        let mut req = req
            .body(body)
            .map_err(|e| new_request_build_error("write", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("write", path, e))?;

        Ok(req)
    }

    pub(crate) async fn get_blob_properties(
        &self,
        path: &str,
    ) -> Result<isahc::Response<isahc::AsyncBody>> {
        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(path)
        );

        let req = isahc::Request::head(&url);

        let mut req = req
            .body(isahc::AsyncBody::empty())
            .map_err(|e| new_request_build_error("stat", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("stat", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("stat", path, e))
    }

    pub(crate) async fn delete_blob(
        &self,
        path: &str,
    ) -> Result<isahc::Response<isahc::AsyncBody>> {
        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(path)
        );

        let req = isahc::Request::delete(&url);

        let mut req = req
            .body(isahc::AsyncBody::empty())
            .map_err(|e| new_request_build_error("delete", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("delete", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("delete", path, e))
    }

    pub(crate) async fn list_blobs(
        &self,
        path: &str,
        next_marker: &str,
    ) -> Result<isahc::Response<isahc::AsyncBody>> {
        let mut url = format!(
            "{}/{}?restype=container&comp=list&delimiter=/",
            self.endpoint, self.container
        );
        if !path.is_empty() {
            write!(url, "&prefix={}", percent_encode_path(path))
                .expect("write into string must succeed");
        }
        if !next_marker.is_empty() {
            write!(url, "&marker={next_marker}").expect("write into string must succeed");
        }

        let mut req = isahc::Request::get(&url)
            .body(isahc::AsyncBody::empty())
            .map_err(|e| new_request_build_error("list", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("list", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("list", path, e))
    }
}
