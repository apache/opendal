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
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::TryStreamExt;
use http::HeaderValue;
use http::StatusCode;
use log::debug;
use log::error;
use log::info;
use log::warn;
use metrics::increment_counter;
use once_cell::sync::Lazy;
use reqsign::services::aws::v4::Signer;
use reqwest::{Body, Response, Url};

use super::object_stream::S3ObjectStream;
use crate::credential::Credential;
use crate::error::Error;
use crate::error::Kind;
use crate::error::Result;
use crate::object::BoxedObjectStream;
use crate::object::Metadata;
use crate::ops::HeaderRange;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::readers::ReaderStream;
use crate::Accessor;
use crate::BoxedAsyncReader;
use crate::ObjectMode;

static ENDPOINT_TEMPLATES: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    // AWS S3 Service.
    m.insert(
        "https://s3.amazonaws.com",
        "https://s3.{region}.amazonaws.com",
    );
    m
});

/// Builder for s3 services
///
/// # TODO
///
/// enable_path_style and enable_signature_v2 need sdk support.
///
/// ref: <https://github.com/awslabs/aws-sdk-rust/issues/390>
#[derive(Default, Debug, Clone)]
pub struct Builder {
    root: Option<String>,

    bucket: String,
    credential: Option<Credential>,
    /// endpoint must be full uri, e.g.
    /// - <https://s3.amazonaws.com>
    /// - <http://127.0.0.1:3000>
    ///
    /// If user inputs endpoint like "s3.amazonaws.com", we will prepend
    /// "https://" before it.
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

    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();

        self
    }

    pub fn credential(&mut self, credential: Credential) -> &mut Self {
        self.credential = Some(credential);

        self
    }

    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

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
        info!("backend use root {}", &root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(Error::Backend {
                kind: Kind::BackendConfigurationInvalid,
                context: HashMap::from([("bucket".to_string(), "".to_string())]),
                source: anyhow!("bucket is empty"),
            }),
        }?;
        debug!("backend use bucket {}", &bucket);

        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint,
            None => "https://s3.amazonaws.com",
        };
        debug!("backend use endpoint {} to detect region", &endpoint);

        // Setup error context so that we don't need to construct many times.
        let mut context: HashMap<String, String> = HashMap::from([
            ("endpoint".to_string(), endpoint.to_string()),
            ("bucket".to_string(), bucket.to_string()),
        ]);

        let client = reqwest::Client::new();
        let res = client
            .head(format!("{endpoint}/{bucket}"))
            .send()
            .await
            .map_err(|e| Error::Backend {
                kind: Kind::BackendConfigurationInvalid,
                context: context.clone(),
                source: anyhow::Error::new(e),
            })?;
        // Read RFC-0057: Auto Region for detailed behavior.
        let (endpoint, region) = match res.status() {
            // The endpoint works, return with not changed endpoint and
            // default region.
            StatusCode::OK | StatusCode::FORBIDDEN => {
                let region = res
                    .headers()
                    .get("x-amz-bucket-region")
                    .unwrap_or(&HeaderValue::from_static("us-east-1"))
                    .to_str()
                    .map_err(|e| Error::Backend {
                        kind: Kind::BackendConfigurationInvalid,
                        context: context.clone(),
                        source: anyhow::Error::new(e),
                    })?
                    .to_string();
                (endpoint.to_string(), region)
            }
            // The endpoint should move, return with constructed endpoint
            StatusCode::MOVED_PERMANENTLY => {
                let region = res
                    .headers()
                    .get("x-amz-bucket-region")
                    .ok_or(Error::Backend {
                        kind: Kind::BackendConfigurationInvalid,
                        context: context.clone(),
                        source: anyhow!("can't detect region automatically, region is empty"),
                    })?
                    .to_str()
                    .map_err(|e| Error::Backend {
                        kind: Kind::BackendConfigurationInvalid,
                        context: context.clone(),
                        source: anyhow::Error::new(e),
                    })?
                    .to_string();
                let template = ENDPOINT_TEMPLATES.get(endpoint).ok_or(Error::Backend {
                    kind: Kind::BackendConfigurationInvalid,
                    context: context.clone(),
                    source: anyhow!(
                        "can't detect region automatically, no valid endpoint template for {}",
                        &endpoint
                    ),
                })?;

                let endpoint = template.replace("{region}", &region);

                (endpoint, region)
            }
            // Unexpected status code
            code => {
                return Err(Error::Backend {
                    kind: Kind::BackendConfigurationInvalid,
                    context: context.clone(),
                    source: anyhow!(
                        "can't detect region automatically, unexpected response: status code {}",
                        code
                    ),
                });
            }
        };
        debug!("backend use endpoint: {}, region: {}", &endpoint, &region);

        let mut signer_builder = reqsign::services::aws::v4::Signer::builder();
        signer_builder.service("s3");
        signer_builder.region(&region);
        signer_builder.allow_anonymous();

        if let Some(cred) = &self.credential {
            context.insert("credential".to_string(), "*".to_string());
            match cred {
                Credential::HMAC {
                    access_key_id,
                    secret_access_key,
                } => {
                    signer_builder.access_key(access_key_id);
                    signer_builder.secret_key(secret_access_key);
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

        let signer = signer_builder.build().await?;

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend {
            root,
            endpoint,
            signer: Arc::new(signer),
            bucket: self.bucket.clone(),
            client,
        }))
    }
}

/// Backend for s3 services.
#[derive(Debug, Clone)]
pub struct Backend {
    bucket: String,
    endpoint: String,
    signer: Arc<Signer>,
    client: reqwest::Client,
    // root will be "/" or "/abc/"
    root: String,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }

    // normalize_path removes all internal `//` inside path.
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

    /// get_abs_path will return the absolute path of the given path in the s3 format.
    ///
    /// Read [RFC-112](https://github.com/datafuselabs/opendal/pull/112) for more details.
    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        let path = Backend::normalize_path(path);
        // root must be normalized like `/abc/`
        format!("{}{}", self.root, path)
            .trim_start_matches('/')
            .to_string()
    }

    /// get_rel_path will return the relative path of the given path in the s3 format.
    pub(crate) fn get_rel_path(&self, path: &str) -> String {
        let path = Backend::normalize_path(path);
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
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        increment_counter!("opendal_s3_read_requests");

        let p = self.get_abs_path(&args.path);
        info!(
            "object {} read start: offset {:?}, size {:?}",
            &p, args.offset, args.size
        );

        let resp = self.get_object(&p, args.offset, args.size).await?;

        info!(
            "object {} reader created: offset {:?}, size {:?}",
            &p, args.offset, args.size
        );
        Ok(Box::new(
            resp.bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .into_async_read(),
        ))
    }

    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let p = self.get_abs_path(&args.path);
        info!("object {} write start: size {}", &p, args.size);

        let resp = self.put_object(&p, r, args.size).await?;
        match resp.status() {
            http::StatusCode::CREATED | http::StatusCode::OK => {
                info!("object {} write finished: size {:?}", &p, args.size);
                Ok(args.size as usize)
            }
            _ => Err(Error::Object {
                kind: Kind::Unexpected,
                op: "write",
                path: p.to_string(),
                source: anyhow!("{:?}", resp),
            }),
        }
    }

    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        increment_counter!("opendal_s3_stat_requests");

        let p = self.get_abs_path(&args.path);
        info!("object {} stat start", &p);

        // Stat root always returns a DIR.
        if self.get_rel_path(&p).is_empty() {
            let mut m = Metadata::default();
            m.set_path(&args.path);
            m.set_content_length(0);
            m.set_mode(ObjectMode::DIR);
            m.set_complete();

            info!("backed root object stat finished");
            return Ok(m);
        }

        let resp = self.head_object(&p).await?;

        match resp.status() {
            http::StatusCode::OK => {
                let mut m = Metadata::default();
                m.set_path(&args.path);

                // Parse content_length
                if let Some(v) = resp.headers().get(http::header::CONTENT_LENGTH) {
                    m.set_content_length(
                        u64::from_str(v.to_str().expect("header must not contain non-ascii value"))
                            .expect("content length header must contain valid length"),
                    );
                }

                if p.ends_with('/') {
                    m.set_mode(ObjectMode::DIR);
                } else {
                    m.set_mode(ObjectMode::FILE);
                };

                m.set_complete();

                info!("object {} stat finished", &p);
                Ok(m)
            }
            http::StatusCode::NOT_FOUND => {
                // Always returns empty dir object if path is endswith "/"
                if p.ends_with('/') {
                    let mut m = Metadata::default();
                    m.set_path(&args.path);
                    m.set_content_length(0);
                    m.set_mode(ObjectMode::DIR);
                    m.set_complete();

                    info!("object {} stat finished", &p);
                    Ok(m)
                } else {
                    Err(Error::Object {
                        kind: Kind::ObjectNotExist,
                        op: "stat",
                        path: p.to_string(),
                        source: anyhow!("{:?}", resp),
                    })
                }
            }
            _ => {
                error!("object {} head_object: {:?}", &p, resp);
                Err(Error::Object {
                    kind: Kind::Unexpected,
                    op: "stat",
                    path: p.to_string(),
                    source: anyhow!("{:?}", resp),
                })
            }
        }
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        increment_counter!("opendal_s3_delete_requests");

        let p = self.get_abs_path(&args.path);
        info!("object {} delete start", &p);

        let _ = self.delete_object(&p).await?;

        info!("object {} delete finished", &p);
        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<BoxedObjectStream> {
        increment_counter!("opendal_s3_list_requests");

        let mut path = self.get_abs_path(&args.path);
        // Make sure list path is endswith '/'
        if !path.ends_with('/') && !path.is_empty() {
            path.push('/')
        }
        info!("object {} list start", &path);

        Ok(Box::new(S3ObjectStream::new(self.clone(), path)))
    }
}

impl Backend {
    pub(crate) async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Response> {
        let mut req = reqwest::Request::new(
            http::Method::GET,
            Url::from_str(&format!("{}/{}/{}", self.endpoint, self.bucket, path))
                .expect("url must be valid"),
        );

        if offset.is_some() || size.is_some() {
            req.headers_mut().insert(
                http::header::RANGE,
                HeaderRange::new(offset, size)
                    .to_string()
                    .parse()
                    .expect("header must be valid"),
            );
        }

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.execute(req).await.map_err(|e| {
            error!("object {} get_object: {:?}", path, e);
            Error::Unexpected(anyhow::Error::from(e))
        })
    }

    pub(crate) async fn put_object(
        &self,
        path: &str,
        r: BoxedAsyncReader,
        size: u64,
    ) -> Result<Response> {
        let mut req = reqwest::Request::new(
            http::Method::PUT,
            Url::from_str(&format!("{}/{}/{}", self.endpoint, self.bucket, path))
                .expect("url must be valid"),
        );

        // Set content length.
        req.headers_mut().insert(
            http::header::CONTENT_LENGTH,
            size.to_string()
                .parse()
                .expect("content length must be valid"),
        );
        req.headers_mut()
            .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("test"));
        *req.body_mut() = Some(Body::from(hyper::body::Body::wrap_stream(
            ReaderStream::new(r),
        )));

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.execute(req).await.map_err(|e| {
            error!("object {} put_object: {:?}", path, e);
            Error::Unexpected(anyhow::Error::from(e))
        })
    }

    pub(crate) async fn head_object(&self, path: &str) -> Result<Response> {
        let mut req = reqwest::Request::new(
            http::Method::HEAD,
            Url::from_str(&format!("{}/{}/{}", self.endpoint, self.bucket, path))
                .expect("url must be valid"),
        );

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.execute(req).await.map_err(|e| {
            error!("object {} head_object: {:?}", path, e);
            Error::Unexpected(anyhow::Error::from(e))
        })
    }

    pub(crate) async fn delete_object(&self, path: &str) -> Result<Response> {
        let mut req = reqwest::Request::new(
            http::Method::DELETE,
            Url::from_str(&format!("{}/{}/{}", self.endpoint, self.bucket, path))
                .expect("url must be valid"),
        );

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.execute(req).await.map_err(|e| {
            error!("object {} delete_object: {:?}", path, e);
            Error::Unexpected(anyhow::Error::from(e))
        })
    }

    pub(crate) async fn list_object(
        &self,
        path: &str,
        continuation_token: &str,
    ) -> Result<Response> {
        let mut req = reqwest::Request::new(
            http::Method::GET,
            Url::from_str(&format!("{}/{}", self.endpoint, self.bucket))
                .expect("url must be valid"),
        );

        {
            let mut query_pairs = req.url_mut().query_pairs_mut();

            query_pairs
                .append_pair("list-type", "2")
                .append_pair("delimiter", "/")
                .append_pair("prefix", path);
            if !continuation_token.is_empty() {
                query_pairs.append_pair("continuation-token", continuation_token);
            }
        }

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.execute(req).await.map_err(|e| {
            error!("object {} list_object: {:?}", path, e);
            Error::Unexpected(anyhow::Error::from(e))
        })
    }
}
