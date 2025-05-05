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
use reqsign::TencentCosConfig;
use reqsign::TencentCosCredentialLoader;
use reqsign::TencentCosSigner;

use super::core::*;
use super::delete::CosDeleter;
use super::error::parse_error;
use super::lister::{CosLister, CosListers, CosObjectVersionsLister};
use super::writer::CosWriter;
use super::writer::CosWriters;
use crate::raw::oio::PageLister;
use crate::raw::*;
use crate::services::CosConfig;
use crate::*;

impl Configurator for CosConfig {
    type Builder = CosBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        CosBuilder {
            config: self,

            http_client: None,
        }
    }
}

/// Tencent-Cloud COS services support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct CosBuilder {
    config: CosConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for CosBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CosBuilder")
            .field("config", &self.config)
            .finish()
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
}

impl Builder for CosBuilder {
    const SCHEME: Scheme = Scheme::Cos;
    type Config = CosConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let bucket = match &self.config.bucket {
            Some(bucket) => Ok(bucket.to_string()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::Cos),
            ),
        }?;
        debug!("backend use bucket {}", &bucket);

        let uri = match &self.config.endpoint {
            Some(endpoint) => endpoint.parse::<Uri>().map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                    .with_context("service", Scheme::Cos)
                    .with_context("endpoint", endpoint)
                    .set_source(err)
            }),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_context("service", Scheme::Cos)),
        }?;

        let scheme = match uri.scheme_str() {
            Some(scheme) => scheme.to_string(),
            None => "https".to_string(),
        };

        // If endpoint contains bucket name, we should trim them.
        let endpoint = uri.host().unwrap().replace(&format!("//{bucket}."), "//");
        debug!("backend use endpoint {}", &endpoint);

        let mut cfg = TencentCosConfig::default();
        if !self.config.disable_config_load {
            cfg = cfg.from_env();
        }

        if let Some(v) = self.config.secret_id {
            cfg.secret_id = Some(v);
        }
        if let Some(v) = self.config.secret_key {
            cfg.secret_key = Some(v);
        }

        let cred_loader = TencentCosCredentialLoader::new(GLOBAL_REQWEST_CLIENT.clone(), cfg);

        let signer = TencentCosSigner::new();

        Ok(CosBackend {
            core: Arc::new(CosCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Cos)
                        .set_root(&root)
                        .set_name(&bucket)
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
                            stat_has_version: true,
                            stat_has_user_metadata: true,

                            read: true,

                            read_with_if_match: true,
                            read_with_if_none_match: true,
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
                            list_has_content_length: true,

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
                bucket: bucket.clone(),
                root,
                endpoint: format!("{}://{}.{}", &scheme, &bucket, &endpoint),
                signer,
                loader: cred_loader,
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
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

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
                    meta.with_user_metadata(user_meta);
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
            TwoWays::Two(PageLister::new(CosObjectVersionsLister::new(
                self.core.clone(),
                path,
                args,
            )))
        } else {
            TwoWays::One(PageLister::new(CosLister::new(
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
