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
use log::warn;
use metrics::increment_counter;
use minitrace::trace;
use reqsign::services::azure::storage::Signer;
use time::format_description::well_known::Rfc2822;
use time::OffsetDateTime;

use crate::credential::Credential;
use crate::error::Error;
use crate::error::Kind;
use crate::error::Result;
use crate::io::BytesStream;
use crate::object::Metadata;
use crate::ops::HeaderRange;
use crate::ops::OpDelete;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::readers::ReaderStream;
use crate::Accessor;
use crate::BoxedAsyncReader;
use crate::ObjectMode;

pub const DELETE_SNAPSHOTS: &str = "x-ms-delete-snapshots";
pub const BLOB_TYPE: &str = "x-ms-blob-type";

#[derive(Default, Debug, Clone)]
pub struct Builder {
    root: Option<String>,
    container: String,
    credential: Option<Credential>,
    endpoint: Option<String>,
}

impl Builder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
    pub fn container(&mut self, container: &str) -> &mut Self {
        self.container = container.to_string();

        self
    }
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = Some(endpoint.to_string());

        self
    }
    pub fn credential(&mut self, credential: Credential) -> &mut Self {
        self.credential = Some(credential);

        self
    }
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        info!("backend build started: {:?}", &self);

        let root = match &self.root {
            // Use "/" as root if user not specified.
            None => "/".to_string(),
            Some(v) => {
                let mut v = Backend::normalize_path(v);
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
            true => Err(Error::Backend {
                kind: Kind::BackendConfigurationInvalid,
                context: HashMap::from([("container".to_string(), "".to_string())]),
                source: anyhow!("container is empty"),
            }),
        }?;
        debug!("backend use container {}", &container);

        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => "blob.core.windows.net".to_string(),
        };

        let mut context: HashMap<String, String> = HashMap::from([
            ("endpoint".to_string(), endpoint.to_string()),
            ("container".to_string(), container.to_string()),
        ]);

        let mut account_name = String::new();
        let mut account_key = String::new();
        if let Some(cred) = &self.credential {
            context.insert("credential".to_string(), "*".to_string());
            match cred {
                Credential::HMAC {
                    access_key_id,
                    secret_access_key,
                } => {
                    account_name = access_key_id.to_string();
                    account_key = secret_access_key.to_string();
                }
                // We don't need to do anything if user tries to read credential from env.
                Credential::Plain => {
                    warn!("backend got empty credential, fallback to read from env.")
                }
                _ => {
                    return Err(Error::Backend {
                        kind: Kind::BackendConfigurationInvalid,
                        context: context.clone(),
                        source: anyhow!("credential is invalid"),
                    });
                }
            }
        }
        let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());

        let mut signer_builder = Signer::builder();
        signer_builder
            .account_name(&account_name)
            .account_key(&account_key);

        let signer = signer_builder.build().await?;

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend {
            root,
            endpoint,
            signer: Arc::new(signer),
            container: self.container.clone(),
            client,
            account_name,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct Backend {
    container: String,
    client: hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    signer: Arc<Signer>,
    account_name: String,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn normalize_path(path: &str) -> String {
        let has_trailing = path.ends_with('/');

        let mut p = path
            .split('/')
            .filter(|v| !v.is_empty())
            .collect::<Vec<&str>>()
            .join("/");

        if has_trailing && !p.eq("/") {
            p.push('/')
        }

        p
    }
    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        let path = Backend::normalize_path(path);
        // root must be normalized like `/abc/`
        format!("{}{}", self.root, path)
            .trim_start_matches('/')
            .to_string()
    }
    #[allow(dead_code)]
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
    #[trace("read")]
    async fn read2(&self, args: &OpRead) -> Result<BytesStream> {
        increment_counter!("opendal_azure_read_requests");

        let p = self.get_abs_path(&args.path);
        debug!(
            "object {} read start: offset {:?}, size {:?}",
            &p, args.offset, args.size
        );

        let resp = self.get_blob(&p, args.offset, args.size).await?;
        match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                debug!(
                    "object {} reader created: offset {:?}, size {:?}",
                    &p, args.offset, args.size
                );

                Ok(Box::new(resp.into_body().into_stream().map_err(move |e| {
                    Error::Object {
                        kind: Kind::Unexpected,
                        op: "read",
                        path: p.to_string(),
                        source: anyhow::Error::from(e),
                    }
                })))
            }
            _ => Err(parse_error_response(resp, "read", &p).await),
        }
    }
    #[trace("write")]
    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let p = self.get_abs_path(&args.path);
        debug!("object {} write start: size {}", &p, args.size);

        let resp = self.put_blob(&p, r, args.size).await?;

        match resp.status() {
            http::StatusCode::CREATED | http::StatusCode::OK => {
                debug!("object {} write finished: size {:?}", &p, args.size);
                Ok(args.size as usize)
            }
            _ => Err(parse_error_response(resp, "write", &p).await),
        }
    }
    #[trace("stat")]
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        increment_counter!("opendal_azure_stat_requests");

        let p = self.get_abs_path(&args.path);
        debug!("object {} stat start", &p);

        // Stat root always returns a DIR.
        if self.get_rel_path(&p).is_empty() {
            let mut m = Metadata::default();
            m.set_path(&args.path);
            m.set_content_length(0);
            m.set_mode(ObjectMode::DIR);
            m.set_complete();

            debug!("backed root object stat finished");
            return Ok(m);
        }

        let resp = self.get_blob_properties(&p).await?;
        match resp.status() {
            http::StatusCode::OK => {
                let mut m = Metadata::default();
                m.set_path(&args.path);

                // Parse content_length
                if let Some(v) = resp.headers().get(http::header::CONTENT_LENGTH) {
                    let v =
                        u64::from_str(v.to_str().expect("header must not contain non-ascii value"))
                            .expect("content length header must contain valid length");

                    m.set_content_length(v);
                }

                // Parse content_md5
                if let Some(v) = resp.headers().get(HeaderName::from_static("content-md5")) {
                    let v = v.to_str().expect("header must not contain non-ascii value");
                    m.set_content_md5(v);
                }

                // Parse last_modified
                if let Some(v) = resp.headers().get(http::header::LAST_MODIFIED) {
                    let v = v.to_str().expect("header must not contain non-ascii value");
                    let t =
                        OffsetDateTime::parse(v, &Rfc2822).expect("must contain valid time format");
                    m.set_last_modified(t.into());
                }

                if p.ends_with('/') {
                    m.set_mode(ObjectMode::DIR);
                } else {
                    m.set_mode(ObjectMode::FILE);
                };

                m.set_complete();

                debug!("object {} stat finished: {:?}", &p, m);
                Ok(m)
            }
            StatusCode::NOT_FOUND if p.ends_with('/') => {
                let mut m = Metadata::default();
                m.set_path(&args.path);
                m.set_content_length(0);
                m.set_mode(ObjectMode::DIR);
                m.set_complete();

                debug!("object {} stat finished", &p);
                Ok(m)
            }
            _ => Err(parse_error_response(resp, "stat", &p).await),
        }
    }
    #[trace("delete")]
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        increment_counter!("opendal_azure_delete_requests");

        let p = self.get_abs_path(&args.path);
        debug!("object {} delete start", &p);

        let resp = self.delete_blob(&p).await?;
        match resp.status() {
            StatusCode::NO_CONTENT => {
                debug!("object {} delete finished", &p);
                Ok(())
            }
            _ => Err(parse_error_response(resp, "delete", &p).await),
        }
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
        let mut req = hyper::Request::get(&format!(
            "https://{}.{}/{}/{}",
            self.account_name, self.endpoint, self.container, path
        ));

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                HeaderRange::new(offset, size).to_string(),
            );
        }

        let mut req = req
            .body(hyper::Body::empty())
            .expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.request(req).await.map_err(|e| {
            error!("object {} get_object: {:?}", path, e);
            Error::Object {
                kind: Kind::Unexpected,
                op: "read",
                path: path.to_string(),
                source: anyhow::Error::from(e),
            }
        })
    }
    #[trace("put_blob")]
    pub(crate) async fn put_blob(
        &self,
        path: &str,
        r: BoxedAsyncReader,
        size: u64,
    ) -> Result<hyper::Response<hyper::Body>> {
        let mut req = hyper::Request::put(&format!(
            "https://{}.{}/{}/{}",
            self.account_name, self.endpoint, self.container, path
        ));

        req = req.header(http::header::CONTENT_LENGTH, size.to_string());

        req = req.header(HeaderName::from_static(BLOB_TYPE), "BlockBlob");

        // Set body
        let mut req = req
            .body(hyper::body::Body::wrap_stream(ReaderStream::new(r)))
            .expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.request(req).await.map_err(|e| {
            error!("object {} put_object: {:?}", path, e);
            Error::Object {
                kind: Kind::Unexpected,
                op: "write",
                path: path.to_string(),
                source: anyhow::Error::from(e),
            }
        })
    }

    #[trace("get_blob_properties")]
    pub(crate) async fn get_blob_properties(
        &self,
        path: &str,
    ) -> Result<hyper::Response<hyper::Body>> {
        let req = hyper::Request::head(&format!(
            "https://{}.{}/{}/{}",
            self.account_name, self.endpoint, self.container, path
        ));
        let mut req = req
            .body(hyper::Body::empty())
            .expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.request(req).await.map_err(|e| {
            error!("object {} head_object: {:?}", path, e);
            Error::Object {
                kind: Kind::Unexpected,
                op: "stat",
                path: path.to_string(),
                source: anyhow::Error::from(e),
            }
        })
    }

    #[trace("delete_blob")]
    pub(crate) async fn delete_blob(&self, path: &str) -> Result<hyper::Response<hyper::Body>> {
        let req = hyper::Request::delete(&format!(
            "https://{}.{}/{}/{}",
            self.account_name, self.endpoint, self.container, path
        ));

        let mut req = req
            .body(hyper::Body::empty())
            .expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.request(req).await.map_err(|e| {
            error!("object {} delete_object: {:?}", path, e);
            Error::Object {
                kind: Kind::Unexpected,
                op: "delete",
                path: path.to_string(),
                source: anyhow::Error::from(e),
            }
        })
    }
}

// Read and decode whole error response.
async fn parse_error_response(resp: Response<Body>, op: &'static str, path: &str) -> Error {
    let (part, mut body) = resp.into_parts();
    let kind = match part.status {
        StatusCode::NOT_FOUND => Kind::ObjectNotExist,
        StatusCode::FORBIDDEN => Kind::ObjectPermissionDenied,
        _ => Kind::Unexpected,
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
            Err(e) => return Error::Unexpected(anyhow!("parse error response parse: {:?}", e)),
        }
    }

    Error::Object {
        kind,
        op,
        path: path.to_string(),
        source: anyhow!(
            "response part: {:?}, body: {:?}",
            part,
            String::from_utf8_lossy(&bs)
        ),
    }
}
