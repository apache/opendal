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
use once_cell::sync::Lazy;

use super::error::parse_get_object_error;
use super::error::parse_head_object_error;
use super::error::parse_unexpect_error;
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

/// # TODO
///
/// enable_path_style and enable_signature_v2 need sdk support.
///
/// ref: https://github.com/awslabs/aws-sdk-rust/issues/390
#[derive(Default, Debug, Clone)]
pub struct Builder {
    root: Option<String>,

    bucket: String,
    credential: Option<Credential>,
    /// endpoint must be full uri, e.g.
    /// - https://s3.amazonaws.com
    /// - http://127.0.0.1:3000
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
        let root = match &self.root {
            // Use "/" as root if user not specified.
            None => "/".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty(), "root must not be empty");

                // If root not startswith or endswith "/", we should append it.
                let prepend = if v.starts_with('/') { "" } else { "/" };
                let append = if v.ends_with('/') { "" } else { "/" };

                format!("{prepend}{v}{append}")
            }
        };

        // Handle endpoint, region and bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(Error::Backend {
                kind: Kind::BackendConfigurationInvalid,
                context: HashMap::from([("bucket".to_string(), "".to_string())]),
                source: anyhow!("bucket is empty"),
            }),
        }?;

        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint,
            None => "https://s3.amazonaws.com",
        };

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
                Credential::Plain => {}
                _ => {
                    return Err(Error::Backend {
                        kind: Kind::BackendConfigurationInvalid,
                        context: context.clone(),
                        source: anyhow!("credential is invalid"),
                    });
                }
            }
        }

        Ok(Arc::new(Backend {
            root,
            bucket: self.bucket.clone(),
            client: aws_sdk_s3::Client::from_conf(cfg.build()),
        }))
    }
}

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

    /// get_abs_path will return the absolute path of the given path in the s3 format.
    /// If user input an absolute path, we will return it as it is with the prefix `/` striped.
    /// If user input a relative path, we will calculate the absolute path with the root.
    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path.starts_with('/') {
            return path.strip_prefix('/').unwrap().to_string();
        }

        let abs_path = format!("{}{}", self.root, path);
        return abs_path.strip_prefix('/').unwrap().to_string();
    }

    /// get_rel_path will return the relative path of the given path in the s3 format.
    pub(crate) fn get_rel_path(&self, path: &str) -> String {
        let path = format!("/{}", path);

        match path.strip_prefix(&self.root) {
            None => unreachable!("invalid input that not start with backend root"),
            Some(v) => v.to_string(),
        }
    }
}

#[async_trait]
impl Accessor for Backend {
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        let p = self.get_abs_path(&args.path);

        let mut req = self
            .client
            .get_object()
            .bucket(&self.bucket.clone())
            .key(&p);

        if args.offset.is_some() || args.size.is_some() {
            req = req.range(HeaderRange::new(args.offset, args.size).to_string());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| parse_get_object_error(e, "read", &args.path))?;

        Ok(Box::new(S3ByteStream(resp.body).into_async_read()))
    }

    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let p = self.get_abs_path(&args.path);

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
            .map_err(|e| parse_unexpect_error(e, "write", &args.path))?;

        Ok(args.size as usize)
    }

    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let p = self.get_abs_path(&args.path);

        let meta = self
            .client
            .head_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .send()
            .await
            .map_err(|e| parse_head_object_error(e, "stat", &args.path))?;

        let mut m = Metadata::default();
        m.set_path(&args.path)
            .set_content_length(meta.content_length as u64)
            .set_mode(ObjectMode::FILE)
            .set_complete();

        Ok(m)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let p = self.get_abs_path(&args.path);

        let _ = self
            .client
            .delete_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .send()
            .await
            .map_err(|e| parse_unexpect_error(e, "delete", &args.path))?;

        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<BoxedObjectStream> {
        let path = self.get_abs_path(&args.path);

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
