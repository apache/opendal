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

use super::reader::*;
use crate::config::TosConfig;
use crate::copier::TosCopiers;
use crate::copier::new_tos_copier;
use crate::core::constants::X_TOS_DIRECTORY;
use crate::core::parse_error;
use crate::core::tos_parse_into_metadata;
use crate::core::*;
use crate::deleter::TosDeleter;
use crate::lister::{TosLister, TosListers, TosObjectVersionsLister};
use crate::writer::TosWriter;
use http::StatusCode;
use opendal_core::OperationContext;
use opendal_core::raw::*;
use opendal_core::{Buffer, Builder, Capability, Error, ErrorKind, Result};
use reqsign_core::{Context, OsEnv};
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_volcengine_tos::{DefaultCredentialProvider, RequestSigner, StaticCredentialProvider};

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
            let trimmed = if let Some(rest) = endpoint.strip_prefix("http://") {
                format!("http://{}", rest.trim_end_matches('/'))
            } else if let Some(rest) = endpoint.strip_prefix("https://") {
                format!("https://{}", rest.trim_end_matches('/'))
            } else {
                endpoint.trim_end_matches('/').to_string()
            };
            self.config.endpoint = Some(trimmed);
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

    /// Disable config load so that opendal will not load config from
    /// environment.
    ///
    /// For examples:
    ///
    /// - envs like `VOLCENGINE_ACCESS_KEY_ID`
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
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
        let config = self.config;
        let bucket = config.bucket.clone();
        if bucket.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "bucket is empty")
                .with_operation("Builder::build")
                .with_context("service", TOS_SCHEME));
        }

        let region = config
            .region
            .clone()
            .unwrap_or_else(|| "cn-beijing".to_string());

        let endpoint = match config.endpoint.as_deref() {
            Some(endpoint) if !endpoint.is_empty() => endpoint.to_string(),
            _ => format!("https://tos-{region}.volces.com"),
        };

        let (scheme, endpoint_domain) = if let Some(stripped) = endpoint.strip_prefix("http://") {
            ("http", stripped.trim_end_matches('/'))
        } else if let Some(stripped) = endpoint.strip_prefix("https://") {
            ("https", stripped.trim_end_matches('/'))
        } else {
            ("https", endpoint.trim_end_matches('/'))
        };

        if endpoint_domain.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", TOS_SCHEME));
        }

        let endpoint = format!("{scheme}://{endpoint_domain}");
        let bucket_endpoint = format!("{scheme}://{bucket}.{endpoint_domain}");
        let root = normalize_root(&config.root.clone().unwrap_or_default());

        let ctx = Context::new().with_file_read(TokioFileRead).with_env(OsEnv);

        let mut provider = DefaultCredentialProvider::builder();
        if config.disable_config_load {
            provider = provider.no_env();
        }
        let mut provider = provider.build();

        if let (Some(ak), Some(sk)) = (&config.access_key_id, &config.secret_access_key) {
            let static_provider = if let Some(token) = config.security_token.as_deref() {
                StaticCredentialProvider::new(ak, sk).with_security_token(token)
            } else {
                StaticCredentialProvider::new(ak, sk)
            };
            provider = provider.push_front(static_provider);
        }

        let credential_provider = Arc::new(provider);
        let request_signer = Arc::new(RequestSigner::new(&region));

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

            presign: true,
            presign_stat: true,
            presign_read: true,
            presign_write: true,
            presign_delete: true,

            stat: true,
            stat_with_if_match: true,
            stat_with_if_none_match: true,
            stat_with_if_modified_since: true,
            stat_with_if_unmodified_since: true,

            shared: true,

            ..Default::default()
        };

        let core = TosCore {
            info,
            capability,
            bucket,
            endpoint,
            bucket_endpoint,
            root,
            default_storage_class: None,
            skip_signature: config.skip_signature,
            sign_ctx: ctx,
            credential_provider,
            request_signer,
            signers: HttpTransportCache::new(),
        };

        Ok(TosBackend {
            core: Arc::new(core),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TosBackend {
    pub(crate) core: Arc<TosCore>,
}

impl Service for TosBackend {
    type Reader = oio::StreamReader<TosReader>;
    type Writer = oio::MultipartWriter<TosWriter>;
    type Lister = TosListers;
    type Deleter = oio::BatchDeleter<TosDeleter>;
    type Copier = TosCopiers;
    type Composer = ();

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
                let mut meta = tos_parse_into_metadata(path, headers)?.into_builder();

                if parse_header_to_str(headers, X_TOS_DIRECTORY)?
                    .map(|v| {
                        v.parse::<bool>().map_err(|e| {
                            Error::new(ErrorKind::Unexpected, "header value is not valid integer")
                                .set_source(e)
                        })
                    })
                    .transpose()?
                    == Some(true)
                {
                    meta.set_dir();
                }

                Ok(RpStat::new(meta.build()))
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("HeadObject")),
                resp,
            )),
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
    ) -> Result<Self::Copier> {
        let output: TosCopiers = {
            let copier = new_tos_copier(self.core.clone(), ctx, from, to, args)?;
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
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let (expire, op) = args.into_parts();
        let req = match op {
            PresignOperation::Stat(v) => self.core.tos_head_object_request(path, v),
            PresignOperation::Read(range, v) => self.core.tos_get_object_request(path, range, &v),
            PresignOperation::Write(v) => {
                self.core
                    .tos_put_object_request(path, None, &v, Buffer::new())
            }
            PresignOperation::Delete(v) => self.core.tos_delete_object_request(path, &v),
            _ => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }?;
        let req = self.core.sign_query(ctx, req, expire).await?;
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::task::Context as TaskContext;
    use std::task::Poll;
    use std::task::Waker;

    use opendal_core::Configurator;
    use reqsign_core::StaticEnv;

    use super::*;

    /// Poll a future once; these futures resolve without an external runtime.
    fn poll_once<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let mut cx = TaskContext::from_waker(Waker::noop());
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("future requires an external async runtime"),
        }
    }

    fn build_core(builder: TosBuilder) -> Arc<TosCore> {
        let backend = builder.build().expect("build must succeed");
        let backend = (&backend as &dyn std::any::Any)
            .downcast_ref::<TosBackend>()
            .expect("build must return TosBackend");
        backend.core.clone()
    }

    /// Sign a request with a static environment so tests never read the process env.
    fn sign_with_env(
        core: &TosCore,
        envs: HashMap<String, String>,
    ) -> reqsign_core::Result<http::request::Parts> {
        let signer = core.signer_with_context(Context::new().with_env(StaticEnv {
            home_dir: None,
            envs,
        }));

        let req = core
            .tos_put_object_request("test", None, &OpWrite::default(), Buffer::new())
            .expect("request must be valid");
        let (mut parts, _) = req.into_parts();
        poll_once(signer.sign(&mut parts, None))?;
        Ok(parts)
    }

    fn env_credentials() -> HashMap<String, String> {
        HashMap::from([
            ("VOLCENGINE_ACCESS_KEY_ID".to_string(), "env-ak".to_string()),
            (
                "VOLCENGINE_SECRET_ACCESS_KEY".to_string(),
                "env-sk".to_string(),
            ),
        ])
    }

    fn authorization(parts: &http::request::Parts) -> &str {
        parts.headers[http::header::AUTHORIZATION]
            .to_str()
            .expect("authorization must be valid header value")
    }

    #[test]
    fn test_disable_config_load_controls_env_credentials() {
        let core = build_core(
            TosBuilder::default()
                .bucket("test-bucket")
                .disable_config_load(),
        );
        assert!(
            sign_with_env(&core, env_credentials()).is_err(),
            "environment credentials must be ignored when config load is disabled"
        );

        let core = build_core(TosBuilder::default().bucket("test-bucket"));
        let parts = sign_with_env(&core, env_credentials())
            .expect("environment credentials must be loaded by default");
        assert!(authorization(&parts).contains("Credential=env-ak/"));
    }

    #[test]
    fn test_disable_config_load_keeps_static_credentials() {
        let core = build_core(
            TosBuilder::default()
                .bucket("test-bucket")
                .access_key_id("static-ak")
                .secret_access_key("static-sk")
                .security_token("static-token")
                .disable_config_load(),
        );

        let parts = sign_with_env(&core, env_credentials())
            .expect("explicit static credentials must still be used");
        assert!(authorization(&parts).contains("Credential=static-ak/"));
        assert_eq!(
            parts.headers["x-tos-security-token"], "static-token",
            "security token must be applied"
        );
    }

    #[test]
    fn test_build_rejects_empty_bucket() {
        let err = match TosBuilder::default().build() {
            Ok(_) => panic!("empty bucket must fail to build"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn test_build_rejects_empty_endpoint_domain() {
        for endpoint in ["http://", "https://", "http:///"] {
            let err = match TosBuilder::default()
                .bucket("test-bucket")
                .endpoint(endpoint)
                .build()
            {
                Ok(_) => panic!("endpoint {endpoint:?} without host must fail to build"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }
    }

    #[test]
    fn test_build_preserves_endpoint_scheme() {
        let http_core = build_core(
            TosBuilder::default()
                .bucket("test-bucket")
                .endpoint("http://127.0.0.1:9000/"),
        );
        assert_eq!(http_core.endpoint, "http://127.0.0.1:9000");
        assert_eq!(
            http_core.bucket_endpoint,
            "http://test-bucket.127.0.0.1:9000"
        );

        let https_core = build_core(
            TosBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://tos-cn-shanghai.volces.com/"),
        );
        assert_eq!(https_core.endpoint, "https://tos-cn-shanghai.volces.com");
        assert_eq!(
            https_core.bucket_endpoint,
            "https://test-bucket.tos-cn-shanghai.volces.com"
        );

        let schemeless_core = build_core(
            TosBuilder::default()
                .bucket("test-bucket")
                .endpoint("tos-cn-beijing.volces.com"),
        );
        assert_eq!(
            schemeless_core.endpoint,
            "https://tos-cn-beijing.volces.com"
        );
        assert_eq!(
            schemeless_core.bucket_endpoint,
            "https://test-bucket.tos-cn-beijing.volces.com"
        );

        let config_core = build_core(
            TosConfig {
                bucket: "test-bucket".to_string(),
                endpoint: Some("http://tos.local/".to_string()),
                ..Default::default()
            }
            .into_builder(),
        );
        assert_eq!(config_core.endpoint, "http://tos.local");
        assert_eq!(config_core.bucket_endpoint, "http://test-bucket.tos.local");
    }
}
