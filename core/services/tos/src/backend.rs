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

use crate::config::TosConfig;
use crate::copier::TosCopiers;
use crate::copier::new_tos_copier;
use crate::core::constants::X_TOS_VERSION_ID;
use crate::core::constants::{X_TOS_DIRECTORY, X_TOS_META_PREFIX};
use crate::core::*;
use crate::deleter::TosDeleter;
use crate::error::parse_error;
use crate::lister::{TosLister, TosListers, TosObjectVersionsLister};
use crate::utils::tos_parse_into_metadata;
use crate::writer::TosWriter;
use http::Response;
use http::StatusCode;
use opendal_core::BytesRange;
use opendal_core::raw::*;
use opendal_core::{Builder, Capability, EntryMode, Error, ErrorKind, Result};
use reqsign_core::{Context, OsEnv, ProvideCredentialChain, Signer};
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_volcengine_tos::{EnvCredentialProvider, RequestSigner, StaticCredentialProvider};

const TOS_SCHEME: &str = "tos";

/// Builder for Volcengine TOS service.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct TosBuilder {
    pub(super) config: TosConfig,
}

impl Debug for TosBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TosBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl TosBuilder {
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
    ///
    /// Endpoint must be full uri, e.g.
    /// - TOS: `https://tos-cn-beijing.volces.com`
    /// - TOS with region: `https://tos-{region}.volces.com`
    ///
    /// If user inputs endpoint without scheme like "tos-cn-beijing.volces.com", we
    /// will prepend "https://" before it.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }
        self
    }

    /// Set region of this backend.
    ///
    /// Region represent the signing region of this endpoint.
    ///
    /// If region is not set, we will try to load it from environment.
    /// If still not set, default to `cn-beijing`.
    pub fn region(mut self, region: &str) -> Self {
        if !region.is_empty() {
            self.config.region = Some(region.to_string());
        }
        self
    }

    /// Set access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(mut self, v: &str) -> Self {
        self.config.access_key_id = Some(v.to_string());
        self
    }

    /// Set secret_access_key of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(mut self, v: &str) -> Self {
        self.config.secret_access_key = Some(v.to_string());
        self
    }

    /// Set security_token of this backend.
    pub fn security_token(mut self, v: &str) -> Self {
        self.config.security_token = Some(v.to_string());
        self
    }

    /// Skip signature will skip loading credentials and signing requests.
    pub fn skip_signature(mut self) -> Self {
        self.config.skip_signature = true;
        self
    }
}

impl Builder for TosBuilder {
    type Config = TosConfig;

    fn build(self) -> Result<impl Service> {
        let mut config = self.config;
        let region = config
            .region
            .clone()
            .unwrap_or_else(|| "cn-beijing".to_string());

        if config.endpoint.is_none() {
            config.endpoint = Some(format!("https://tos-{}.volces.com", region));
        }

        let endpoint = config.endpoint.clone().unwrap();
        let bucket = config.bucket.clone();
        let root = normalize_root(&config.root.clone().unwrap_or_default());

        let ctx = Context::new().with_file_read(TokioFileRead).with_env(OsEnv);

        let mut provider = ProvideCredentialChain::new().push(EnvCredentialProvider::new());

        if let (Some(ak), Some(sk)) = (&config.access_key_id, &config.secret_access_key) {
            let static_provider = if let Some(token) = config.security_token.as_deref() {
                StaticCredentialProvider::new(ak, sk).with_security_token(token)
            } else {
                StaticCredentialProvider::new(ak, sk)
            };
            provider = provider.push_front(static_provider);
        }

        let request_signer = RequestSigner::new(&region);
        let signer = Signer::new(ctx, provider, request_signer);

        let info = ServiceInfo::new(TOS_SCHEME, &root, &bucket);
        let capability = Capability {
            stat_with_version: true,

            read: true,
            read_with_suffix: true,
            read_with_if_match: true,
            read_with_if_none_match: true,
            read_with_if_modified_since: true,
            read_with_if_unmodified_since: true,
            read_with_version: true,

            write: true,
            write_can_empty: true,
            write_can_multi: true,
            write_with_cache_control: true,
            write_with_content_type: true,
            write_with_content_encoding: true,
            write_with_if_match: true,
            write_with_if_not_exists: true,
            write_with_user_metadata: true,
            write_multi_min_size: Some(5 * 1024 * 1024),
            write_multi_max_size: if cfg!(target_pointer_width = "64") {
                Some(5 * 1024 * 1024 * 1024)
            } else {
                Some(usize::MAX)
            },

            delete: true,
            delete_max_size: Some(1000),
            delete_with_version: true,

            copy: true,
            copy_can_multi: true,
            copy_multi_min_size: Some(5 * 1024 * 1024),
            copy_multi_max_size: if cfg!(target_pointer_width = "64") {
                Some(5 * 1024 * 1024 * 1024)
            } else {
                Some(usize::MAX)
            },

            list: true,
            list_with_limit: true,
            list_with_start_after: true,
            list_with_recursive: true,
            list_with_versions: true,
            list_with_deleted: true,

            stat: true,
            stat_with_if_match: true,
            stat_with_if_none_match: true,
            stat_with_if_modified_since: true,
            stat_with_if_unmodified_since: true,

            shared: true,

            ..Default::default()
        };

        // Extract domain from endpoint, removing http:// or https:// prefix
        let endpoint_domain = if let Some(stripped) = endpoint.strip_prefix("http://") {
            stripped
        } else if let Some(stripped) = endpoint.strip_prefix("https://") {
            stripped
        } else {
            &endpoint
        };

        let core = TosCore {
            info,
            capability,
            bucket,
            endpoint: endpoint.clone(),
            endpoint_domain: endpoint_domain.to_string(),
            root,
            default_storage_class: None,
            skip_signature: config.skip_signature,
            signer,
        };

        Ok(TosBackend {
            core: Arc::new(core),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TosBackend {
    core: Arc<TosCore>,
}

/// Reader returned by this backend.
pub struct TosReader {
    backend: TosBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl TosReader {
    fn new(backend: TosBackend, ctx: OperationContext, path: &str, args: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for TosReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .tos_get_object(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();
        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Service for TosBackend {
    type Reader = oio::StreamReader<TosReader>;
    type Writer = oio::MultipartWriter<TosWriter>;
    type Lister = TosListers;
    type Deleter = oio::BatchDeleter<TosDeleter>;
    type Copier = TosCopiers;

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.tos_head_object(ctx, path, args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = tos_parse_into_metadata(path, headers)?;

                let user_meta = parse_prefixed_headers(headers, X_TOS_META_PREFIX);
                if !user_meta.is_empty() {
                    meta = meta.with_user_metadata(user_meta);
                }

                if let Some(v) = parse_header_to_str(headers, X_TOS_VERSION_ID)? {
                    meta.set_version(v);
                }

                if let Some(is_dir) = parse_header_to_str(headers, X_TOS_DIRECTORY)?
                    .map(|v| {
                        v.parse::<bool>().map_err(|e| {
                            Error::new(ErrorKind::Unexpected, "header value is not valid integer")
                                .set_source(e)
                        })
                    })
                    .transpose()?
                {
                    meta = meta.with_mode(if is_dir {
                        EntryMode::DIR
                    } else {
                        EntryMode::FILE
                    });
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<TosReader> = {
            Ok(oio::StreamReader::new(TosReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: oio::MultipartWriter<TosWriter> = {
            let writer = TosWriter::new(self.core.clone(), ctx.clone(), path, args.clone());

            let w = oio::MultipartWriter::new(ctx.executor().clone(), writer, args.concurrent());

            Ok(w)
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::BatchDeleter<TosDeleter> = {
            let deleter = TosDeleter::new(self.core.clone(), ctx.clone());
            Ok(oio::BatchDeleter::new(
                deleter,
                self.core.capability.delete_max_size,
            ))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: TosListers = {
            let lister = if args.versions() || args.deleted() {
                TwoWays::Two(oio::PageLister::new(TosObjectVersionsLister::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            } else {
                TwoWays::One(oio::PageLister::new(TosLister::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            };
            Ok(lister)
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        let output: TosCopiers = {
            let copier = new_tos_copier(self.core.clone(), ctx, from, to, args, opts)?;
            Ok(copier)
        }?;

        Ok(output)
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}
