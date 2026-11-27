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
use crate::core::constants::X_TOS_VERSION_ID;
use crate::core::constants::{X_TOS_DIRECTORY, X_TOS_META_PREFIX};
use crate::core::parse_error;
use crate::core::tos_parse_into_metadata;
use crate::core::*;
use crate::deleter::TosDeleter;
use crate::lister::{TosLister, TosListers, TosObjectVersionsLister};
use crate::writer::TosWriter;
use bytes::Buf;
use http::StatusCode;
use opendal_core::OperationContext;
use opendal_core::raw::*;
use opendal_core::{Buffer, Builder, Capability, Error, ErrorKind, Result};
use reqsign_core::OsEnv;
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

        let ctx = opendal_core::raw::reqsign_context()
            .with_file_read(TokioFileRead)
            .with_env(OsEnv);

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
            write_with_checksum: !config.skip_signature,
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

            restore: true,
            restore_with_version: true,

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

                let user_meta = parse_prefixed_headers(headers, X_TOS_META_PREFIX);
                if !user_meta.is_empty() {
                    meta.user_metadata(user_meta);
                }

                if let Some(v) = parse_header_to_str(headers, X_TOS_VERSION_ID)? {
                    meta.version(v);
                }

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
        let concurrent = args.concurrent();
        let writer = TosWriter::new(self.core.clone(), ctx.clone(), path, args)?;
        Ok(oio::MultipartWriter::new(
            ctx.executor().clone(),
            writer,
            concurrent,
        ))
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

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        args.check(&self.capability())?;

        if let Some(version) = args.version() {
            if version.is_empty() {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "empty restore version ID",
                ));
            }
            let req = self
                .core
                .tos_copy_object_request(path, path, Some(version), false)?;
            let req = self.core.sign(ctx, req).await?;
            // Replaying after a lost response can create another version.
            let resp = ctx
                .http_transport()
                .send(req)
                .await
                .map_err(Error::set_permanent)?;
            if resp.status() != StatusCode::OK {
                return Err(
                    parse_error(ErrorContext::new(ServiceOperation("CopyObject")), resp)
                        .set_permanent(),
                );
            }
            let output: CopyObjectOutput = serde_json::from_reader(resp.into_body().reader())
                .map_err(new_json_deserialize_error)
                .map_err(Error::set_permanent)?;
            if output.etag.is_empty() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "ETag not present in restore copy response",
                )
                .set_permanent());
            }
            return Ok(RpRestore::new());
        }

        let key = build_absolute_path(&self.core.root, path);
        let mut key_marker = "";
        let mut version_id_marker = String::new();
        let mut current_marker = None;
        let marker = loop {
            let resp = self
                .core
                .tos_list_object_versions(ctx, path, "", Some(2), key_marker, &version_id_marker)
                .await?;
            if resp.status() != StatusCode::OK {
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("ListObjectVersions")),
                    resp,
                ));
            }
            let output: ListObjectVersionsOutput =
                serde_json::from_reader(resp.into_body().reader())
                    .map_err(new_json_deserialize_error)
                    .map_err(Error::set_temporary)?;

            if output
                .versions
                .iter()
                .any(|version| version.key == key && version.is_latest)
            {
                return Ok(RpRestore::new());
            }
            if current_marker.is_none() {
                current_marker = output
                    .delete_markers
                    .into_iter()
                    .find(|marker| marker.key == key && marker.is_latest)
                    .map(|marker| marker.version_id);
            }
            if current_marker.is_none() {
                break None;
            }
            // A delete marker can outlive all data versions of its key.
            if output.versions.iter().any(|version| version.key == key) {
                break current_marker;
            }
            if output.is_truncated != Some(true) || output.next_key_marker.as_deref() != Some(&key)
            {
                break None;
            }
            key_marker = &key;
            version_id_marker = output
                .next_version_id_marker
                .filter(|next| !next.is_empty() && *next != version_id_marker)
                .ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "version listing did not advance")
                })?;
        };
        let Some(marker) = marker else {
            return Err(Error::new(
                ErrorKind::NotFound,
                "no live or recoverable version exists",
            ));
        };
        if marker.is_empty() || marker == "null" {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "restore requires a persistent delete marker version ID",
            ));
        }

        let args = OpDelete::new().into_version(&marker);
        let req = self.core.tos_delete_object_request(path, &args)?;
        let req = self.core.sign(ctx, req).await?;
        // Replaying discovery after a lost response can remove the next marker.
        let resp = ctx
            .http_transport()
            .send(req)
            .await
            .map_err(Error::set_permanent)?;
        match resp.status() {
            StatusCode::NO_CONTENT => Ok(RpRestore::new()),
            _ => Err(
                parse_error(ErrorContext::new(ServiceOperation("DeleteObject")), resp)
                    .set_permanent(),
            ),
        }
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
                if v.checksum().is_some() {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "TOS cannot verify a presigned write checksum",
                    ));
                }
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
        let signer =
            core.signer_with_context(opendal_core::raw::reqsign_context().with_env(StaticEnv {
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
}
