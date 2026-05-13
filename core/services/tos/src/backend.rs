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
use crate::core::constants::X_TOS_VERSION_ID;
use crate::core::constants::{X_TOS_DIRECTORY, X_TOS_META_PREFIX};
use crate::core::*;
use crate::deleter::TosDeleter;
use crate::error::parse_error;
use crate::utils::tos_parse_into_metadata;
use crate::writer::TosWriter;
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::{Builder, Capability, EntryMode, Error, ErrorKind, Result};
use reqsign_core::{Context, OsEnv, ProvideCredentialChain, Signer};
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_volcengine_tos::{EnvCredentialProvider, RequestSigner, StaticCredentialProvider};

const TOS_SCHEME: &str = "tos";

/// Builder for Volcengine TOS service.
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

    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    pub fn allow_anonymous(mut self, allow: bool) -> Self {
        self.config.allow_anonymous = allow;
        self
    }

    /// Set bucket versioning status for this backend.
    ///
    /// If set to true, OpenDAL will support versioned operations like list with
    /// versions, read with version, etc.
    pub fn enable_versioning(mut self, enabled: bool) -> Self {
        self.config.enable_versioning = enabled;
        self
    }
}

impl Builder for TosBuilder {
    type Config = TosConfig;

    fn build(self) -> Result<impl Access> {
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
        let root = config.root.clone().unwrap_or_else(|| "/".to_string());

        let ctx = Context::new()
            .with_file_read(TokioFileRead)
            .with_http_send(HttpClient::with(GLOBAL_REQWEST_CLIENT.clone()))
            .with_env(OsEnv);

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

        let info = {
            let am = AccessorInfo::default();
            am.set_scheme(TOS_SCHEME)
                .set_root(&root)
                .set_name(&bucket)
                .set_native_capability(Capability {
                    read: true,
                    read_with_if_match: true,
                    read_with_if_none_match: true,
                    read_with_if_modified_since: true,
                    read_with_if_unmodified_since: true,
                    read_with_version: config.enable_versioning,

                    write: true,
                    write_can_empty: true,
                    write_can_multi: true,
                    write_with_cache_control: true,
                    write_with_content_type: true,
                    write_with_content_encoding: true,
                    write_with_if_match: true,
                    write_with_if_not_exists: !config.enable_versioning,
                    write_with_user_metadata: true,
                    write_multi_min_size: Some(5 * 1024 * 1024),
                    write_multi_max_size: Some(5 * 1024 * 1024 * 1024),

                    delete: true,
                    delete_max_size: Some(1000),
                    delete_with_version: config.enable_versioning,

                    list: false,

                    stat: false,

                    shared: false,

                    ..Default::default()
                });

            am.into()
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
            bucket,
            endpoint: endpoint.clone(),
            endpoint_domain: endpoint_domain.to_string(),
            root,
            default_storage_class: None,
            allow_anonymous: config.allow_anonymous,
            signer,
        };

        Ok(TosBackend {
            core: Arc::new(core),
        })
    }
}

#[derive(Debug)]
pub struct TosBackend {
    core: Arc<TosCore>,
}

impl Access for TosBackend {
    type Reader = HttpBody;
    type Writer = oio::MultipartWriter<TosWriter>;
    type Lister = ();
    type Deleter = oio::BatchDeleter<TosDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.tos_head_object(path, args).await?;

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

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.tos_get_object(path, args.range(), &args).await?;

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
        let writer = TosWriter::new(self.core.clone(), path, args.clone());

        let w = oio::MultipartWriter::new(self.core.info.clone(), writer, args.concurrent());

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let info = self.core.info.clone();
        let capability = info.full_capability();
        let deleter = TosDeleter::new(self.core.clone());
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(deleter, capability.delete_max_size),
        ))
    }
}
