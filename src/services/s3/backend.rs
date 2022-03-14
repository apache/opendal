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

use std::borrow::Cow;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3;
use aws_sdk_s3::Client;
use aws_smithy_http::body::SdkBody;
use aws_smithy_http::byte_stream::ByteStream;
use futures::TryStreamExt;
use http::HeaderValue;
use http::StatusCode;
use log::debug;
use log::error;
use log::info;
use log::warn;
use metrics::increment_counter;
use once_cell::sync::Lazy;

use super::error::parse_get_object_error;
use super::error::parse_head_object_error;
use super::error::parse_unexpect_error;
use super::middleware::DefaultMiddleware;
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

        let hc = reqwest::Client::new();
        let res = hc
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

        // Config Loader will load config from environment.
        //
        // We will take user's input first if any. If there is no user input, we
        // will fallback to the aws default load chain like the following:
        //
        // - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION
        // - The default credentials files located in ~/.aws/config and ~/.aws/credentials (location can vary per platform)
        // - Web Identity Token credentials from the environment or container (including EKS)
        // - ECS Container Credentials (IAM roles for tasks)
        // - EC2 Instance Metadata Service (IAM Roles attached to instance)
        //
        // Please keep in mind that the config loader only detect region and credentials.
        let cfg_loader = aws_config::ConfigLoader::default();
        let mut cfg = aws_sdk_s3::config::Builder::from(&cfg_loader.load().await);

        {
            // Set region.
            cfg = cfg.region(aws_sdk_s3::Region::new(Cow::from(region.clone())));
        }

        {
            // Set endpoint
            let uri = http::Uri::from_str(&endpoint).map_err(|e| Error::Backend {
                kind: Kind::BackendConfigurationInvalid,
                context: context.clone(),
                source: anyhow::Error::from(e),
            })?;

            cfg = cfg.endpoint_resolver(aws_sdk_s3::Endpoint::immutable(uri));
        }

        if let Some(cred) = &self.credential {
            context.insert("credential".to_string(), "*".to_string());
            match cred {
                Credential::HMAC {
                    access_key_id,
                    secret_access_key,
                } => {
                    cfg = cfg.credentials_provider(aws_sdk_s3::Credentials::from_keys(
                        access_key_id,
                        secret_access_key,
                        None,
                    ));
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

        let hyper_connector = aws_smithy_client::hyper_ext::Adapter::builder()
            .build(aws_smithy_client::conns::https());

        let aws_client = aws_smithy_client::Builder::new()
            .connector(hyper_connector)
            .middleware(aws_smithy_client::erase::DynMiddleware::new(
                DefaultMiddleware::new(),
            ))
            .default_async_sleep()
            .build();

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend {
            root,
            bucket: self.bucket.clone(),
            client: aws_sdk_s3::Client::with_config(aws_client.into_dyn(), cfg.build()),
        }))
    }
}

/// Backend for s3 services.
#[derive(Debug, Clone)]
pub struct Backend {
    bucket: String,

    client: aws_sdk_s3::Client,
    // root will be "/" or "/abc/"
    root: String,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn inner(&self) -> Client {
        self.client.clone()
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

        let mut req = self
            .client
            .get_object()
            .bucket(&self.bucket.clone())
            .key(&p);

        if args.offset.is_some() || args.size.is_some() {
            req = req.range(HeaderRange::new(args.offset, args.size).to_string());
        }

        let resp = req.send().await.map_err(|e| {
            let e = parse_get_object_error(e, "read", &p);
            error!("object {} get_object: {:?}", &p, e);
            e
        })?;

        info!(
            "object {} reader created: offset {:?}, size {:?}",
            &p, args.offset, args.size
        );
        Ok(Box::new(S3ByteStream(resp.body).into_async_read()))
    }

    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let p = self.get_abs_path(&args.path);
        info!("object {} write start: size {}", &p, args.size);

        let _ = self
            .client
            .put_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .content_length(args.size as i64)
            .body(ByteStream::from(SdkBody::from(
                hyper::body::Body::wrap_stream(ReaderStream::new(r)),
            )))
            .send()
            .await
            .map_err(|e| {
                let e = parse_unexpect_error(e, "write", &p);
                error!("object {} put_object: {:?}", &p, e);
                e
            })?;

        info!("object {} write finished: size {:?}", &p, args.size);
        Ok(args.size as usize)
    }

    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        increment_counter!("opendal_s3_stat_requests");

        let p = self.get_abs_path(&args.path);
        info!("object {} stat start", &p);

        // Stat root always returns a DIR.
        if p == self.get_abs_path(&self.root) {
            let mut m = Metadata::default();
            m.set_path(&args.path);
            m.set_content_length(0);
            m.set_mode(ObjectMode::DIR);
            m.set_complete();

            info!("backed root object stat finished");
            return Ok(m);
        }

        let meta = self
            .client
            .head_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .send()
            .await
            .map_err(|e| parse_head_object_error(e, "stat", &p));

        match meta {
            Ok(meta) => {
                let mut m = Metadata::default();
                m.set_path(&args.path);
                m.set_content_length(meta.content_length as u64);

                if p.ends_with('/') {
                    m.set_mode(ObjectMode::DIR);
                } else {
                    m.set_mode(ObjectMode::FILE);
                };

                m.set_complete();

                info!("object {} stat finished", &p);
                Ok(m)
            }
            // Always returns empty dir object if path is endswith "/" and we got an
            // ObjectNotExist error.
            Err(e) if (e.kind() == Kind::ObjectNotExist && p.ends_with('/')) => {
                let mut m = Metadata::default();
                m.set_path(&args.path);
                m.set_content_length(0);
                m.set_mode(ObjectMode::DIR);
                m.set_complete();

                info!("object {} stat finished", &p);
                Ok(m)
            }
            Err(e) => {
                error!("object {} head_object: {:?}", &p, e);
                Err(e)
            }
        }
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        increment_counter!("opendal_s3_delete_requests");

        let p = self.get_abs_path(&args.path);
        info!("object {} delete start", &p);

        let _ = self
            .client
            .delete_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .send()
            .await
            .map_err(|e| parse_unexpect_error(e, "delete", &p))?;

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

        Ok(Box::new(S3ObjectStream::new(
            self.clone(),
            self.bucket.clone(),
            path,
        )))
    }
}

struct S3ByteStream(aws_smithy_http::byte_stream::ByteStream);

impl futures::Stream for S3ByteStream {
    type Item = std::result::Result<bytes::Bytes, std::io::Error>;

    /// ## TODO
    ///
    /// This hack is ugly, we should find a better way to do this.
    ///
    /// The problem is `into_async_read` requires the stream returning
    /// `std::io::Error`, the the `ByteStream` returns
    /// `aws_smithy_http::byte_stream::Error` instead.
    ///
    /// I don't know why aws sdk should wrap the error into their own type...
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_next(cx)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
