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
use std::sync::Arc;

use http::Response;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign_core::Context;
use reqsign_core::Env as _;
use reqsign_core::OsEnv;
use reqsign_core::Signer;
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_http_send_reqwest::ReqwestHttpSend;
use reqsign_tencent_cos::DefaultCredentialProvider;
use reqsign_tencent_cos::RequestSigner;
use reqsign_tencent_cos::StaticCredentialProvider;

use super::COS_SCHEME;
use super::config::CosConfig;
use super::core::*;
use super::deleter::CosDeleter;
use super::error::parse_error;
use super::lister::CosLister;
use super::lister::CosListers;
use super::lister::CosObjectVersionsLister;
use super::writer::CosWriter;
use super::writer::CosWriters;
use opendal_core::raw::*;
use std::sync::LazyLock;

static GLOBAL_REQWEST_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);
use opendal_core::*;

/// Tencent-Cloud COS services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct CosBuilder {
    pub(super) config: CosConfig,
}

impl Debug for CosBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CosBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl CosBuilder {
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

    /// Set endpoint of this backend.
    ///
    /// NOTE: no bucket or account id in endpoint, we will trim them if exists.
    ///
    /// # Examples
    ///
    /// - `https://cos.ap-singapore.myqcloud.com`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set secret_id of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_id(mut self, secret_id: &str) -> Self {
        if !secret_id.is_empty() {
            self.config.secret_id = Some(secret_id.to_string());
        }

        self
    }

    /// Set secret_key of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_key(mut self, secret_key: &str) -> Self {
        if !secret_key.is_empty() {
            self.config.secret_key = Some(secret_key.to_string());
        }

        self
    }

    /// Set bucket of this backend.
    /// The param is required.
    pub fn bucket(mut self, bucket: &str) -> Self {
        if !bucket.is_empty() {
            self.config.bucket = Some(bucket.to_string());
        }

        self
    }

    /// Set bucket versioning status for this backend
    pub fn enable_versioning(mut self, enabled: bool) -> Self {
        self.config.enable_versioning = enabled;

        self
    }

    /// Disable config load so that opendal will not load config from
    /// environment.
    ///
    /// For examples:
    ///
    /// - envs like `TENCENTCLOUD_SECRET_ID`
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
        self
    }
}

impl Builder for CosBuilder {
    type Config = CosConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let bucket = match &self.config.bucket {
            Some(bucket) => Ok(bucket.to_string()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", COS_SCHEME),
            ),
        }?;
        debug!("backend use bucket {}", &bucket);

        let uri = match &self.config.endpoint {
            Some(endpoint) => endpoint.parse::<Uri>().map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                    .with_context("service", COS_SCHEME)
                    .with_context("endpoint", endpoint)
                    .set_source(err)
            }),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_context("service", COS_SCHEME)),
        }?;

        let scheme = match uri.scheme_str() {
            Some(scheme) => scheme.to_string(),
            None => "https".to_string(),
        };

        // If endpoint contains bucket name, we should trim them.
        let endpoint = uri.host().unwrap().replace(&format!("//{bucket}."), "//");
        debug!("backend use endpoint {}", &endpoint);

        let os_env = OsEnv;
        let envs = os_env.vars();
        let ctx = Context::new()
            .with_file_read(TokioFileRead)
            .with_http_send(ReqwestHttpSend::new(GLOBAL_REQWEST_CLIENT.clone()))
            .with_env(os_env);

        let mut credential = if self.config.disable_config_load {
            DefaultCredentialProvider::builder()
                .disable_env(true)
                .disable_assume_role(true)
                .build()
        } else {
            DefaultCredentialProvider::new()
        };

        if let (Some(secret_id), Some(secret_key)) = (
            self.config.secret_id.as_deref(),
            self.config.secret_key.as_deref(),
        ) {
            let security_token = envs
                .get("TENCENTCLOUD_TOKEN")
                .or_else(|| envs.get("TENCENTCLOUD_SECURITY_TOKEN"))
                .or_else(|| envs.get("QCLOUD_SECRET_TOKEN"));

            let static_provider = if self.config.disable_config_load {
                StaticCredentialProvider::new(secret_id, secret_key)
            } else if let Some(token) = security_token {
                StaticCredentialProvider::with_security_token(secret_id, secret_key, token)
            } else {
                StaticCredentialProvider::new(secret_id, secret_key)
            };

            credential = credential.push_front(static_provider);
        }

        let signer = Signer::new(ctx, credential, RequestSigner::new());

        Ok(CosBackend {
            core: Arc::new(CosCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(COS_SCHEME)
                        .set_root(&root)
                        .set_name(&bucket)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_with_if_match: true,
                            stat_with_if_none_match: true,
                            stat_with_version: self.config.enable_versioning,

                            read: true,

                            read_with_if_match: true,
                            read_with_if_none_match: true,
                            read_with_if_modified_since: true,
                            read_with_if_unmodified_since: true,
                            read_with_version: self.config.enable_versioning,

                            write: true,
                            write_can_empty: true,
                            write_can_append: true,
                            write_can_multi: true,
                            write_with_content_type: true,
                            write_with_cache_control: true,
                            write_with_content_disposition: true,
                            // Cos doesn't support forbid overwrite while version has been enabled.
                            write_with_if_not_exists: !self.config.enable_versioning,
                            // The min multipart size of COS is 1 MiB.
                            //
                            // ref: <https://www.tencentcloud.com/document/product/436/14112>
                            write_multi_min_size: Some(1024 * 1024),
                            // The max multipart size of COS is 5 GiB.
                            //
                            // ref: <https://www.tencentcloud.com/document/product/436/14112>
                            write_multi_max_size: if cfg!(target_pointer_width = "64") {
                                Some(5 * 1024 * 1024 * 1024)
                            } else {
                                Some(usize::MAX)
                            },
                            write_with_user_metadata: true,

                            delete: true,
                            delete_with_version: self.config.enable_versioning,
                            copy: true,

                            list: true,
                            list_with_recursive: true,
                            list_with_versions: self.config.enable_versioning,
                            list_with_deleted: self.config.enable_versioning,

                            presign: true,
                            presign_stat: true,
                            presign_read: true,
                            presign_write: true,

                            shared: true,

                            ..Default::default()
                        });

                    am.into()
                },
                bucket: bucket.clone(),
                root,
                endpoint: format!("{}://{}.{}", &scheme, &bucket, &endpoint),
                signer,
            }),
        })
    }
}

/// Backend for Tencent-Cloud COS services.
#[derive(Debug, Clone)]
pub struct CosBackend {
    core: Arc<CosCore>,
}

impl Access for CosBackend {
    type Reader = HttpBody;
    type Writer = CosWriters;
    type Lister = CosListers;
    type Deleter = oio::OneShotDeleter<CosDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.cos_head_object(path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?;

                let user_meta = parse_prefixed_headers(headers, "x-cos-meta-");
                if !user_meta.is_empty() {
                    meta = meta.with_user_metadata(user_meta);
                }

                if let Some(v) = parse_header_to_str(headers, constants::X_COS_VERSION_ID)? {
                    if v != "null" {
                        meta.set_version(v);
                    }
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.cos_get_object(path, args.range(), &args).await?;

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
        let writer = CosWriter::new(self.core.clone(), path, args.clone());

        let w = if args.append() {
            CosWriters::Two(oio::AppendWriter::new(writer))
        } else {
            CosWriters::One(oio::MultipartWriter::new(
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
            oio::OneShotDeleter::new(CosDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = if args.versions() || args.deleted() {
            TwoWays::Two(oio::PageLister::new(CosObjectVersionsLister::new(
                self.core.clone(),
                path,
                args,
            )))
        } else {
            TwoWays::One(oio::PageLister::new(CosLister::new(
                self.core.clone(),
                path,
                args.recursive(),
                args.limit(),
            )))
        };

        Ok((RpList::default(), l))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.cos_copy_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.cos_head_object_request(path, v),
            PresignOperation::Read(v) => {
                self.core
                    .cos_get_object_request(path, BytesRange::default(), v)
            }
            PresignOperation::Write(v) => {
                self.core
                    .cos_put_object_request(path, None, v, Buffer::new())
            }
            PresignOperation::Delete(_) => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
            _ => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        };
        let req = req?;
        let req = self.core.sign_query(req, args.expire()).await?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}
