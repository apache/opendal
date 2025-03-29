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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use http::Response;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::AliyunConfig;
use reqsign::AliyunLoader;
use reqsign::AliyunOssSigner;

use super::core::*;
use super::delete::OssDeleter;
use super::error::parse_error;
use super::lister::{OssLister, OssListers, OssObjectVersionsLister};
use super::writer::OssWriter;
use super::writer::OssWriters;
use crate::raw::*;
use crate::services::OssConfig;
use crate::*;

const DEFAULT_BATCH_MAX_OPERATIONS: usize = 1000;

impl Configurator for OssConfig {
    type Builder = OssBuilder;

    #[allow(deprecated)]
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

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
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

    /// Set bucket versioning status for this backend
    pub fn enable_versioning(mut self, enabled: bool) -> Self {
        self.config.enable_versioning = enabled;

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
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
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
    #[deprecated(
        since = "0.52.0",
        note = "Please use `delete_max_size` instead of `batch_max_operations`"
    )]
    pub fn batch_max_operations(mut self, delete_max_size: usize) -> Self {
        self.config.delete_max_size = Some(delete_max_size);

        self
    }

    /// Set maximum delete operations of this backend.
    pub fn delete_max_size(mut self, delete_max_size: usize) -> Self {
        self.config.delete_max_size = Some(delete_max_size);

        self
    }

    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    pub fn allow_anonymous(mut self) -> Self {
        self.config.allow_anonymous = true;
        self
    }

    /// Set role_arn for this backend.
    ///
    /// If `role_arn` is set, we will use already known config as source
    /// credential to assume role with `role_arn`.
    pub fn role_arn(mut self, role_arn: &str) -> Self {
        if !role_arn.is_empty() {
            self.config.role_arn = Some(role_arn.to_string())
        }

        self
    }

    /// Set role_session_name for this backend.
    pub fn role_session_name(mut self, role_session_name: &str) -> Self {
        if !role_session_name.is_empty() {
            self.config.role_session_name = Some(role_session_name.to_string())
        }

        self
    }

    /// Set oidc_provider_arn for this backend.
    pub fn oidc_provider_arn(mut self, oidc_provider_arn: &str) -> Self {
        if !oidc_provider_arn.is_empty() {
            self.config.oidc_provider_arn = Some(oidc_provider_arn.to_string())
        }

        self
    }

    /// Set oidc_token_file for this backend.
    pub fn oidc_token_file(mut self, oidc_token_file: &str) -> Self {
        if !oidc_token_file.is_empty() {
            self.config.oidc_token_file = Some(oidc_token_file.to_string())
        }

        self
    }

    /// Set sts_endpoint for this backend.
    pub fn sts_endpoint(mut self, sts_endpoint: &str) -> Self {
        if !sts_endpoint.is_empty() {
            self.config.sts_endpoint = Some(sts_endpoint.to_string())
        }

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

        if let Some(v) = self.config.role_arn {
            cfg.role_arn = Some(v);
        }

        // override default role_session_name if set
        if let Some(v) = self.config.role_session_name {
            cfg.role_session_name = v;
        }

        if let Some(v) = self.config.oidc_provider_arn {
            cfg.oidc_provider_arn = Some(v);
        }

        if let Some(v) = self.config.oidc_token_file {
            cfg.oidc_token_file = Some(v);
        }

        if let Some(v) = self.config.sts_endpoint {
            cfg.sts_endpoint = Some(v);
        }

        let loader = AliyunLoader::new(GLOBAL_REQWEST_CLIENT.clone(), cfg);

        let signer = AliyunOssSigner::new(bucket);

        let delete_max_size = self
            .config
            .delete_max_size
            .unwrap_or(DEFAULT_BATCH_MAX_OPERATIONS);

        Ok(OssBackend {
            core: Arc::new(OssCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Oss)
                        .set_root(&root)
                        .set_name(bucket)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_with_if_match: true,
                            stat_with_if_none_match: true,
                            stat_has_cache_control: true,
                            stat_has_content_length: true,
                            stat_has_content_type: true,
                            stat_has_content_encoding: true,
                            stat_has_content_range: true,
                            stat_with_version: self.config.enable_versioning,
                            stat_has_etag: true,
                            stat_has_content_md5: true,
                            stat_has_last_modified: true,
                            stat_has_content_disposition: true,
                            stat_has_user_metadata: true,
                            stat_has_version: true,

                            read: true,

                            read_with_if_match: true,
                            read_with_if_none_match: true,
                            read_with_version: self.config.enable_versioning,
                            read_with_if_modified_since: true,
                            read_with_if_unmodified_since: true,

                            write: true,
                            write_can_empty: true,
                            write_can_append: true,
                            write_can_multi: true,
                            write_with_cache_control: true,
                            write_with_content_type: true,
                            write_with_content_disposition: true,
                            // TODO: set this to false while version has been enabled.
                            write_with_if_not_exists: !self.config.enable_versioning,

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
                            delete_with_version: self.config.enable_versioning,
                            delete_max_size: Some(delete_max_size),

                            copy: true,

                            list: true,
                            list_with_limit: true,
                            list_with_start_after: true,
                            list_with_recursive: true,
                            list_has_etag: true,
                            list_has_content_md5: true,
                            list_with_versions: self.config.enable_versioning,
                            list_with_deleted: self.config.enable_versioning,
                            list_has_content_length: true,
                            list_has_last_modified: true,

                            presign: true,
                            presign_stat: true,
                            presign_read: true,
                            presign_write: true,

                            shared: true,

                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = self.http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
                },
                root,
                bucket: bucket.to_owned(),
                endpoint,
                host,
                presign_endpoint,
                allow_anonymous: self.config.allow_anonymous,
                signer,
                loader,
                server_side_encryption,
                server_side_encryption_key_id,
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
    type Lister = OssListers;
    type Deleter = oio::BatchDeleter<OssDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.oss_head_object(path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = self.core.parse_metadata(path, resp.headers())?;

                if let Some(v) = parse_header_to_str(headers, constants::X_OSS_VERSION_ID)? {
                    meta.set_version(v);
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
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
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = OssWriter::new(self.core.clone(), path, args.clone());

        let w = if args.append() {
            OssWriters::Two(oio::AppendWriter::new(writer))
        } else {
            OssWriters::One(oio::MultipartWriter::new(
                self.core.info.clone(),
                writer,
                args.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(OssDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = if args.versions() || args.deleted() {
            TwoWays::Two(oio::PageLister::new(OssObjectVersionsLister::new(
                self.core.clone(),
                path,
                args,
            )))
        } else {
            TwoWays::One(oio::PageLister::new(OssLister::new(
                self.core.clone(),
                path,
                args.recursive(),
                args.limit(),
                args.start_after(),
            )))
        };

        Ok((RpList::default(), l))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.oss_copy_object(from, to).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.oss_head_object_request(path, true, v),
            PresignOperation::Read(v) => self.core.oss_get_object_request(path, true, v),
            PresignOperation::Write(v) => {
                self.core
                    .oss_put_object_request(path, None, v, Buffer::new(), true)
            }
            PresignOperation::Delete(_) => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        };
        let mut req = req?;

        self.core.sign_query(&mut req, args.expire()).await?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}
