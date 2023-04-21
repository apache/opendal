// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Buf;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::AliyunConfig;
use reqsign::AliyunLoader;
use reqsign::AliyunOssSigner;

use super::core::*;
use super::error::parse_error;
use super::pager::OssPager;
use super::writer::OssWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Aliyun Object Storage Service (OSS) support
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] copy
/// - [x] list
/// - [x] scan
/// - [ ] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work dir for backend.
/// - `bucket`: Set the container name for backend.
/// - `endpoint`: Set the endpoint for backend.
/// - `presign_endpoint`: Set the endpoint for presign.
/// - `access_key_id`: Set the access_key_id for backend.
/// - `access_key_secret`: Set the access_key_secret for backend.
/// - `role_arn`: Set the role of backend.
/// - `oidc_token`: Set the oidc_token for backend.
/// - `allow_anonymous`: Set the backend access OSS in anonymous way.
///
/// Refer to [`OssBuilder`]'s public API docs for more information.
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::services::Oss;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create OSS backend builder.
///     let mut builder = Oss::default();
///     // Set the root for oss, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/path/to/dir");
///     // Set the bucket name, this is required.
///     builder.bucket("test");
///     // Set the endpoint.
///     //
///     // For example:
///     // - "https://oss-ap-northeast-1.aliyuncs.com"
///     // - "https://oss-hangzhou.aliyuncs.com"
///     builder.endpoint("https://oss-cn-beijing.aliyuncs.com");
///     // Set the access_key_id and access_key_secret.
///     //
///     // OpenDAL will try load credential from the env.
///     // If credential not set and no valid credential in env, OpenDAL will
///     // send request without signing like anonymous user.
///     builder.access_key_id("access_key_id");
///     builder.access_key_secret("access_key_secret");
///
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct OssBuilder {
    root: Option<String>,

    endpoint: Option<String>,
    presign_endpoint: Option<String>,
    bucket: String,

    // authenticate options
    access_key_id: Option<String>,
    access_key_secret: Option<String>,

    http_client: Option<HttpClient>,
}

impl Debug for OssBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");
        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint);

        d.finish_non_exhaustive()
    }
}

impl OssBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set bucket name of this backend.
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();

        self
    }

    /// Set endpoint of this backend.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Set a endpoint for generating presigned urls.
    ///
    /// You can offer a public endpoint like <https://oss-cn-beijing.aliyuncs.com> to return a presinged url for
    /// public accessors, along with an internal endpoint like <https://oss-cn-beijing-internal.aliyuncs.com>
    /// to access objects in a faster path.
    ///
    /// - If presign_endpoint is set, we will use presign_endpoint on generating presigned urls.
    /// - if not, we will use endpoint as default.
    pub fn presign_endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.presign_endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Set access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.access_key_id = Some(v.to_string())
        }

        self
    }

    /// Set access_key_secret of this backend.
    ///
    /// - If access_key_secret is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_secret(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.access_key_secret = Some(v.to_string())
        }

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

    /// preprocess the endpoint option
    fn parse_endpoint(&self, endpoint: &Option<String>, bucket: &str) -> Result<(String, String)> {
        let (endpoint, host) = match endpoint.clone() {
            Some(ep) => {
                let uri = ep.parse::<Uri>().map_err(|err| {
                    Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                        .with_context("service", Scheme::Oss)
                        .with_context("endpoint", &ep)
                        .set_source(err)
                })?;
                let host = uri.host().ok_or_else(|| {
                    Error::new(ErrorKind::ConfigInvalid, "endpoint host is empty")
                        .with_context("service", Scheme::Oss)
                        .with_context("endpoint", &ep)
                })?;
                let full_host = format!("{bucket}.{host}");
                let endpoint = match uri.scheme_str() {
                    Some(scheme_str) => match scheme_str {
                        "http" | "https" => format!("{scheme_str}://{full_host}"),
                        _ => {
                            return Err(Error::new(
                                ErrorKind::ConfigInvalid,
                                "endpoint protocol is invalid",
                            )
                            .with_context("service", Scheme::Oss));
                        }
                    },
                    None => format!("https://{full_host}"),
                };
                (endpoint, full_host)
            }
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", Scheme::Oss));
            }
        };
        Ok((endpoint, host))
    }
}

impl Builder for OssBuilder {
    const SCHEME: Scheme = Scheme::Oss;
    type Accessor = OssBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = OssBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("bucket").map(|v| builder.bucket(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("presign_endpoint")
            .map(|v| builder.presign_endpoint(v));
        map.get("access_key_id").map(|v| builder.access_key_id(v));
        map.get("access_key_secret")
            .map(|v| builder.access_key_secret(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::Oss),
            ),
        }?;

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Oss)
            })?
        };

        // Retrieve endpoint and host by parsing the endpoint option and bucket. If presign_endpoint is not
        // set, take endpoint as default presign_endpoint.
        let (endpoint, host) = self.parse_endpoint(&self.endpoint, bucket)?;
        debug!("backend use bucket {}, endpoint: {}", &bucket, &endpoint);

        let presign_endpoint = if self.presign_endpoint.is_some() {
            self.parse_endpoint(&self.presign_endpoint, bucket)?.0
        } else {
            endpoint.clone()
        };
        debug!("backend use presign_endpoint: {}", &presign_endpoint);

        let mut cfg = AliyunConfig::default();
        // Load cfg from env first.
        cfg = cfg.from_env();

        if let Some(v) = self.access_key_id.take() {
            cfg.access_key_id = Some(v);
        }

        if let Some(v) = self.access_key_secret.take() {
            cfg.access_key_secret = Some(v);
        }

        let loader = AliyunLoader::new(client.client(), cfg);

        let signer = AliyunOssSigner::new(bucket);

        debug!("Backend build finished");

        Ok(OssBackend {
            core: Arc::new(OssCore {
                root,
                bucket: bucket.to_owned(),
                endpoint,
                host,
                presign_endpoint,
                signer,
                loader,
                client,
            }),
        })
    }
}

#[derive(Debug, Clone)]
/// Aliyun Object Storage Service backend
pub struct OssBackend {
    core: Arc<OssCore>,
}

#[async_trait]
impl Accessor for OssBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = OssWriter;
    type BlockingWriter = ();
    type Pager = OssPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Oss)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_capability(Capability {
                read: true,
                read_can_next: true,
                write: true,
                list: true,
                scan: true,
                copy: true,
                presign: true,
                batch: true,
                batch_max_operations: Some(1000),
                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let resp = self
            .core
            .oss_put_object(path, None, None, None, None, AsyncBody::Empty)
            .await?;
        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCreate::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self
            .core
            .oss_get_object(
                path,
                args.range(),
                args.if_match(),
                args.if_none_match(),
                args.override_content_disposition(),
            )
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            OssWriter::new(self.core.clone(), path, args),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.oss_copy_object(from, to).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCopy::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        if path == "/" {
            let m = Metadata::new(EntryMode::DIR);
            return Ok(RpStat::new(m));
        }

        let resp = self
            .core
            .oss_head_object(path, args.if_match(), args.if_none_match())
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                let m = Metadata::new(EntryMode::DIR);
                Ok(RpStat::new(m))
            }

            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.oss_delete_object(path).await?;
        let status = resp.status();
        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => {
                resp.into_body().consume().await?;
                Ok(RpDelete::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        Ok((
            RpList::default(),
            OssPager::new(self.core.clone(), path, "/", args.limit()),
        ))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        Ok((
            RpScan::default(),
            OssPager::new(self.core.clone(), path, "", args.limit()),
        ))
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let mut req = match args.operation() {
            PresignOperation::Stat(v) => {
                self.core
                    .oss_head_object_request(path, true, v.if_match(), v.if_none_match())?
            }
            PresignOperation::Read(v) => self.core.oss_get_object_request(
                path,
                v.range(),
                true,
                v.if_match(),
                v.if_none_match(),
                v.override_content_disposition(),
            )?,
            PresignOperation::Write(v) => self.core.oss_put_object_request(
                path,
                None,
                v.content_type(),
                v.content_disposition(),
                v.cache_control(),
                AsyncBody::Empty,
                true,
            )?,
        };

        self.core.sign_query(&mut req, args.expire()).await?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let ops = args.into_operation();
        // Sadly, OSS will not return failed keys, so we will build
        // a set to calculate the failed keys.
        let mut keys = HashSet::new();

        let ops_len = ops.len();
        if ops_len > 1000 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "oss services only allow delete up to 1000 keys at once",
            )
            .with_context("length", ops_len.to_string()));
        }

        let paths = ops
            .into_iter()
            .map(|(p, _)| {
                keys.insert(p.clone());
                p
            })
            .collect();

        let resp = self.core.oss_delete_objects(paths).await?;

        let status = resp.status();

        if let StatusCode::OK = status {
            let bs = resp.into_body().bytes().await?;

            let result: DeleteObjectsResult =
                quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

            let mut batched_result = Vec::with_capacity(ops_len);
            for i in result.deleted {
                let path = build_rel_path(&self.core.root, &i.key);
                keys.remove(&path);
                batched_result.push((path, Ok(RpDelete::default().into())));
            }
            // TODO: we should handle those errors with code.
            for i in keys {
                batched_result.push((
                    i,
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "oss delete this key failed for reason we don't know",
                    )),
                ));
            }

            Ok(RpBatch::new(batched_result))
        } else {
            Err(parse_error(resp).await?)
        }
    }
}
