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

use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use http::Response;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::AliyunConfig;
use reqsign::AliyunLoader;
use reqsign::AliyunOssSigner;
use serde::Deserialize;
use serde::Serialize;

use super::core::*;
use super::error::parse_error;
use super::lister::OssLister;
use super::writer::OssWriter;
use crate::raw::*;
use crate::services::oss::writer::OssWriters;
use crate::*;

const DEFAULT_BATCH_MAX_OPERATIONS: usize = 1000;

/// Config for Aliyun Object Storage Service (OSS) support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct OssConfig {
    /// Root for oss.
    pub root: Option<String>,

    /// Endpoint for oss.
    pub endpoint: Option<String>,
    /// Presign endpoint for oss.
    pub presign_endpoint: Option<String>,
    /// Bucket for oss.
    pub bucket: String,

    // OSS features
    /// Server side encryption for oss.
    pub server_side_encryption: Option<String>,
    /// Server side encryption key id for oss.
    pub server_side_encryption_key_id: Option<String>,
    /// Allow anonymous for oss.
    pub allow_anonymous: bool,

    // authenticate options
    /// Access key id for oss.
    pub access_key_id: Option<String>,
    /// Access key secret for oss.
    pub access_key_secret: Option<String>,
    /// batch_max_operations
    pub batch_max_operations: Option<usize>,
}

impl Debug for OssConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");
        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("allow_anonymous", &self.allow_anonymous);

        d.finish_non_exhaustive()
    }
}

impl Configurator for OssConfig {
    type Builder = OssBuilder;
    fn into_builder(self) -> Self::Builder {
        OssBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// Aliyun Object Storage Service (OSS) support
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct OssBuilder {
    config: OssConfig,
    http_client: Option<HttpClient>,
}

impl Debug for OssBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("OssBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl OssBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set bucket name of this backend.
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();

        self
    }

    /// Set endpoint of this backend.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Set an endpoint for generating presigned urls.
    ///
    /// You can offer a public endpoint like <https://oss-cn-beijing.aliyuncs.com> to return a presinged url for
    /// public accessors, along with an internal endpoint like <https://oss-cn-beijing-internal.aliyuncs.com>
    /// to access objects in a faster path.
    ///
    /// - If presign_endpoint is set, we will use presign_endpoint on generating presigned urls.
    /// - if not, we will use endpoint as default.
    pub fn presign_endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.presign_endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Set access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.access_key_id = Some(v.to_string())
        }

        self
    }

    /// Set access_key_secret of this backend.
    ///
    /// - If access_key_secret is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_secret(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.access_key_secret = Some(v.to_string())
        }

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(mut self, client: HttpClient) -> Self {
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
                let full_host = if let Some(port) = uri.port_u16() {
                    format!("{bucket}.{host}:{port}")
                } else {
                    format!("{bucket}.{host}")
                };
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

    /// Set server_side_encryption for this backend.
    ///
    /// Available values: `AES256`, `KMS`.
    ///
    /// Reference: <https://www.alibabacloud.com/help/en/object-storage-service/latest/server-side-encryption-5>
    /// Brief explanation:
    /// There are two server-side encryption methods available:
    /// SSE-AES256:
    ///     1. Configure the bucket encryption mode as OSS-managed and specify the encryption algorithm as AES256.
    ///     2. Include the `x-oss-server-side-encryption` parameter in the request and set its value to AES256.
    /// SSE-KMS:
    ///     1. To use this service, you need to first enable KMS.
    ///     2. Configure the bucket encryption mode as KMS, and specify the specific CMK ID for BYOK (Bring Your Own Key)
    ///        or not specify the specific CMK ID for OSS-managed KMS key.
    ///     3. Include the `x-oss-server-side-encryption` parameter in the request and set its value to KMS.
    ///     4. If a specific CMK ID is specified, include the `x-oss-server-side-encryption-key-id` parameter in the request, and set its value to the specified CMK ID.
    pub fn server_side_encryption(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption = Some(v.to_string())
        }
        self
    }

    /// Set server_side_encryption_key_id for this backend.
    ///
    /// # Notes
    ///
    /// This option only takes effect when server_side_encryption equals to KMS.
    pub fn server_side_encryption_key_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_key_id = Some(v.to_string())
        }
        self
    }

    /// Set maximum batch operations of this backend.
    pub fn batch_max_operations(mut self, batch_max_operations: usize) -> Self {
        self.config.batch_max_operations = Some(batch_max_operations);

        self
    }

    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    pub fn allow_anonymous(mut self) -> Self {
        self.config.allow_anonymous = true;
        self
    }
}

impl Builder for OssBuilder {
    const SCHEME: Scheme = Scheme::Oss;
    type Config = OssConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.config.bucket.is_empty() {
            false => Ok(&self.config.bucket),
            true => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::Oss),
            ),
        }?;

        // Retrieve endpoint and host by parsing the endpoint option and bucket. If presign_endpoint is not
        // set, take endpoint as default presign_endpoint.
        let (endpoint, host) = self.parse_endpoint(&self.config.endpoint, bucket)?;
        debug!("backend use bucket {}, endpoint: {}", &bucket, &endpoint);

        let presign_endpoint = if self.config.presign_endpoint.is_some() {
            self.parse_endpoint(&self.config.presign_endpoint, bucket)?
                .0
        } else {
            endpoint.clone()
        };
        debug!("backend use presign_endpoint: {}", &presign_endpoint);

        let server_side_encryption = match &self.config.server_side_encryption {
            None => None,
            Some(v) => Some(
                build_header_value(v)
                    .map_err(|err| err.with_context("key", "server_side_encryption"))?,
            ),
        };

        let server_side_encryption_key_id = match &self.config.server_side_encryption_key_id {
            None => None,
            Some(v) => Some(
                build_header_value(v)
                    .map_err(|err| err.with_context("key", "server_side_encryption_key_id"))?,
            ),
        };

        let mut cfg = AliyunConfig::default();
        // Load cfg from env first.
        cfg = cfg.from_env();

        if let Some(v) = self.config.access_key_id {
            cfg.access_key_id = Some(v);
        }

        if let Some(v) = self.config.access_key_secret {
            cfg.access_key_secret = Some(v);
        }

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Oss)
            })?
        };

        let loader = AliyunLoader::new(client.client(), cfg);

        let signer = AliyunOssSigner::new(bucket);

        let batch_max_operations = self
            .config
            .batch_max_operations
            .unwrap_or(DEFAULT_BATCH_MAX_OPERATIONS);

        Ok(OssBackend {
            core: Arc::new(OssCore {
                root,
                bucket: bucket.to_owned(),
                endpoint,
                host,
                presign_endpoint,
                allow_anonymous: self.config.allow_anonymous,
                signer,
                loader,
                client,
                server_side_encryption,
                server_side_encryption_key_id,
                batch_max_operations,
            }),
        })
    }
}

#[derive(Debug, Clone)]
/// Aliyun Object Storage Service backend
pub struct OssBackend {
    core: Arc<OssCore>,
}

impl Access for OssBackend {
    type Reader = HttpBody;
    type Writer = OssWriters;
    type Lister = oio::PageLister<OssLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Oss)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_native_capability(Capability {
                stat: true,
                stat_with_if_match: true,
                stat_with_if_none_match: true,

                read: true,

                read_with_if_match: true,
                read_with_if_none_match: true,

                write: true,
                write_can_empty: true,
                write_can_append: true,
                write_can_multi: true,
                write_with_cache_control: true,
                write_with_content_type: true,
                write_with_content_disposition: true,
                // The min multipart size of OSS is 100 KiB.
                //
                // ref: <https://www.alibabacloud.com/help/en/oss/user-guide/multipart-upload-12>
                write_multi_min_size: Some(100 * 1024),
                // The max multipart size of OSS is 5 GiB.
                //
                // ref: <https://www.alibabacloud.com/help/en/oss/user-guide/multipart-upload-12>
                write_multi_max_size: if cfg!(target_pointer_width = "64") {
                    Some(5 * 1024 * 1024 * 1024)
                } else {
                    Some(usize::MAX)
                },
                write_with_user_metadata: true,

                delete: true,
                copy: true,

                list: true,
                list_with_limit: true,
                list_with_start_after: true,
                list_with_recursive: true,

                presign: true,
                presign_stat: true,
                presign_read: true,
                presign_write: true,

                batch: true,
                batch_max_operations: Some(self.core.batch_max_operations),

                ..Default::default()
            });

        am.into()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.oss_head_object(path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta =
                    self.core
                        .parse_metadata(path, constants::X_OSS_META_PREFIX, resp.headers())?;

                if let Some(v) = parse_header_to_str(headers, "x-oss-version-id")? {
                    meta.set_version(v);
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.oss_get_object(path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)).await?)
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = OssWriter::new(self.core.clone(), path, args.clone());

        let w = if args.append() {
            OssWriters::Two(oio::AppendWriter::new(writer))
        } else {
            OssWriters::One(oio::MultipartWriter::new(
                writer,
                args.executor().cloned(),
                args.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let resp = self.core.oss_delete_object(path, &args).await?;
        let status = resp.status();
        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = OssLister::new(
            self.core.clone(),
            path,
            args.recursive(),
            args.limit(),
            args.start_after(),
        );
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.oss_copy_object(from, to).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let mut req = match args.operation() {
            PresignOperation::Stat(v) => self.core.oss_head_object_request(path, true, v)?,
            PresignOperation::Read(v) => self.core.oss_get_object_request(path, true, v)?,
            PresignOperation::Write(v) => {
                self.core
                    .oss_put_object_request(path, None, v, Buffer::new(), true)?
            }
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
            let bs = resp.into_body();

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
