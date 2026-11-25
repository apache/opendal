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

use bytes::Buf;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign_aliyun_oss::AssumeRoleCredentialProvider;
use reqsign_aliyun_oss::AssumeRoleWithOidcCredentialProvider;
use reqsign_aliyun_oss::Credential;
use reqsign_aliyun_oss::DefaultCredentialProvider;
use reqsign_aliyun_oss::EcsRamRoleCredentialProvider;
use reqsign_aliyun_oss::RequestSigner;
use reqsign_aliyun_oss::StaticCredentialProvider;
use reqsign_core::Context;
use reqsign_core::OsEnv;
use reqsign_core::ProvideCredentialChain;
use reqsign_file_read_tokio::TokioFileRead;

use super::OSS_SCHEME;
use super::config::OssConfig;
use super::core::parse_error;
use super::core::*;
use super::deleter::OssDeleter;
use super::lister::OssLister;
use super::lister::OssListers;
use super::lister::OssObjectVersionsLister;
use super::reader::*;
use super::writer::OssWriter;
use super::writer::OssWriters;
use opendal_core::raw::*;
use opendal_core::*;

const DEFAULT_BATCH_MAX_OPERATIONS: usize = 1000;

/// Aliyun Object Storage Service (OSS) support
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct OssBuilder {
    pub(super) config: OssConfig,
    pub(super) credential_providers: Option<ProvideCredentialChain<Credential>>,
}

impl Debug for OssBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OssBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
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

    /// Set addressing style for the endpoint.
    ///
    /// Available values: `virtual`, `cname`, `path`.
    ///
    /// - `virtual`: Use virtual addressing style, i.e. `http://bucket.oss-<region>.aliyuncs.com/object`
    /// - `cname`: Use cname addressing style, i.e. `http://mydomain.com/object` with mydomain.com bound to your bucket.
    /// - `path`: Use path addressing style. i.e. `http://oss-<region>.aliyuncs.com/bucket/object`
    ///
    /// - If not set, default value is `virtual`.
    pub fn addressing_style(mut self, addressing_style: &str) -> Self {
        self.config.addressing_style = Some(addressing_style.to_string());

        self
    }

    /// Deprecated: OSS versioning capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "OSS versioning capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_versioning(self, _enabled: bool) -> Self {
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

    /// Set addressing style for presign endpoint.
    ///
    /// Similar to setting addressing style for endpoint.
    ///
    /// - If both presign_endpoint and presign_addressing_style are not set, they are the same as endpoint's configurations.
    ///
    /// - If presign_endpoint is set, but presign_addressing_style is not set, default value is `virtual`.
    pub fn presign_addressing_style(mut self, addressing_style: &str) -> Self {
        self.config.presign_addressing_style = Some(addressing_style.to_string());

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

    /// Set security_token for this backend.
    ///
    /// - If security_token is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn security_token(mut self, security_token: &str) -> Self {
        if !security_token.is_empty() {
            self.config.security_token = Some(security_token.to_string())
        }

        self
    }

    /// preprocess the endpoint option
    fn parse_endpoint(
        &self,
        endpoint: &Option<String>,
        bucket: &str,
        addressing_style: AddressingStyle,
    ) -> Result<(String, String)> {
        let (endpoint, host) = match endpoint.clone() {
            Some(ep) => {
                let uri = ep.parse::<Uri>().map_err(|err| {
                    Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                        .with_context("service", OSS_SCHEME)
                        .with_context("endpoint", &ep)
                        .set_source(err)
                })?;
                let host = uri.host().ok_or_else(|| {
                    Error::new(ErrorKind::ConfigInvalid, "endpoint host is empty")
                        .with_context("service", OSS_SCHEME)
                        .with_context("endpoint", &ep)
                })?;
                let full_host = match addressing_style {
                    AddressingStyle::Virtual => {
                        if let Some(port) = uri.port_u16() {
                            format!("{bucket}.{host}:{port}")
                        } else {
                            format!("{bucket}.{host}")
                        }
                    }
                    AddressingStyle::Cname | AddressingStyle::Path => {
                        if let Some(port) = uri.port_u16() {
                            format!("{host}:{port}")
                        } else {
                            host.to_string()
                        }
                    }
                };
                if let Some(port) = uri.port_u16() {
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
                            .with_context("service", OSS_SCHEME));
                        }
                    },
                    None => format!("https://{full_host}"),
                };
                let endpoint = match addressing_style {
                    AddressingStyle::Path => format!("{}/{}", endpoint, bucket),
                    AddressingStyle::Cname | AddressingStyle::Virtual => endpoint,
                };
                (endpoint, full_host)
            }
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", OSS_SCHEME));
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

    /// Deprecated: OSS delete batch capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "OSS delete batch capability is enabled by default and this option is no longer needed."
    )]
    pub fn batch_max_operations(self, _delete_max_size: usize) -> Self {
        self
    }

    /// Deprecated: OSS delete batch capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "OSS delete batch capability is enabled by default and this option is no longer needed."
    )]
    pub fn delete_max_size(self, _delete_max_size: usize) -> Self {
        self
    }

    /// Skip signature will skip loading credentials and signing requests.
    pub fn skip_signature(mut self) -> Self {
        self.config.skip_signature = true;
        self
    }

    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    #[deprecated(
        since = "0.57.0",
        note = "Please use `skip_signature` instead of `allow_anonymous`"
    )]
    pub fn allow_anonymous(self) -> Self {
        self.skip_signature()
    }

    /// Disable loading credentials from ambient sources.
    ///
    /// See [`OssConfig::disable_config_load`] for the affected sources.
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
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

    /// Set external_id for this backend.
    pub fn external_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.external_id = Some(v.to_string())
        }

        self
    }

    /// Replace the credential providers with a custom chain.
    ///
    /// The custom chain replaces all default providers, including explicit and
    /// environment credentials, OIDC, ECS RAM Role, and AssumeRole providers.
    /// Other credential-related configuration remains available but does not
    /// override this chain.
    ///
    /// Build the chain with [`opendal_core::reqsign_core::ProvideCredentialChain`]
    /// and [`reqsign_aliyun_oss::Credential`].
    ///
    /// Set [`Credential::expires_in`] for temporary credentials. The reqsign
    /// signer caches credentials and calls this chain again when they approach
    /// expiration.
    pub fn credential_provider_chain(mut self, chain: ProvideCredentialChain<Credential>) -> Self {
        self.credential_providers = Some(chain);
        self
    }
}

enum AddressingStyle {
    Path,
    Cname,
    Virtual,
}

impl TryFrom<&Option<String>> for AddressingStyle {
    type Error = Error;

    fn try_from(value: &Option<String>) -> Result<Self> {
        match value.as_deref() {
            None | Some("virtual") => Ok(AddressingStyle::Virtual),
            Some("path") => Ok(AddressingStyle::Path),
            Some("cname") => Ok(AddressingStyle::Cname),
            Some(v) => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "Invalid addressing style, available: `virtual`, `path`, `cname`",
            )
            .with_context("service", OSS_SCHEME)
            .with_context("addressing_style", v)),
        }
    }
}

impl Builder for OssBuilder {
    type Config = OssConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        #[allow(deprecated)]
        let skip_signature = self.config.skip_signature || self.config.allow_anonymous;

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.config.bucket.is_empty() {
            false => Ok(&self.config.bucket),
            true => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", OSS_SCHEME),
            ),
        }?;

        // Use the operation endpoint for signing unless a separate endpoint is configured.
        let (endpoint, host) = self.parse_endpoint(
            &self.config.endpoint,
            bucket,
            (&self.config.addressing_style).try_into()?,
        )?;
        debug!("backend use bucket {}, endpoint: {}", bucket, endpoint);

        let presign_endpoint = if self.config.presign_endpoint.is_some() {
            self.parse_endpoint(
                &self.config.presign_endpoint,
                bucket,
                (&self.config.presign_addressing_style).try_into()?,
            )?
            .0
        } else {
            endpoint.clone()
        };
        debug!("backend use presign_endpoint: {}", presign_endpoint);

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

        // The signer reads credential files (OIDC tokens, OSS profile and
        // Alibaba Cloud files) and ambient environment variables through this
        // context. `OperationContext` supplies the HTTP sender for STS and
        // metadata requests when requests are signed.
        let ctx = Context::new().with_file_read(TokioFileRead).with_env(OsEnv);

        let static_provider = if let (Some(ak), Some(sk)) =
            (&self.config.access_key_id, &self.config.access_key_secret)
        {
            Some(if let Some(token) = self.config.security_token.as_deref() {
                StaticCredentialProvider::new(ak, sk).with_security_token(token)
            } else {
                StaticCredentialProvider::new(ak, sk)
            })
        } else {
            None
        };

        // Configure OIDC from builder options so that it never depends on the
        // environment. Options that are unset keep falling back to their
        // `ALIBABA_CLOUD_*` environment variables.
        let mut oidc = AssumeRoleWithOidcCredentialProvider::new();
        if let Some(v) = self.config.role_arn.as_deref() {
            oidc = oidc.with_role_arn(v);
        }
        if let Some(v) = self.config.oidc_provider_arn.as_deref() {
            oidc = oidc.with_oidc_provider_arn(v);
        }
        if let Some(v) = self.config.oidc_token_file.as_deref() {
            oidc = oidc.with_oidc_token_file(v);
        }
        if let Some(v) = self.config.sts_endpoint.as_deref() {
            oidc = oidc.with_sts_endpoint(v);
        }
        if let Some(v) = self.config.role_session_name.as_deref() {
            oidc = oidc.with_role_session_name(v);
        }

        // `role_arn` is taken from this builder only. Disabling the default
        // assume-role slot keeps an ambient `ALIBABA_CLOUD_ROLE_ARN` from
        // replacing explicit access keys with an assumed role.
        //
        // OIDC must stay ahead of ECS metadata so that ACK RRSA deployments use
        // the pod role instead of the node role (#8339). Both providers are
        // appended to the chain below instead of using their default slots,
        // because the default order tries ECS metadata before OIDC.
        let mut default_credential = DefaultCredentialProvider::builder()
            .no_assume_role()
            .no_oidc()
            .no_ecs_ram_role();

        if self.config.disable_config_load {
            default_credential = default_credential
                .no_env()
                .no_oss_profile()
                .no_credentials_file()
                .no_config_file()
                .no_credentials_uri();
        }

        // A partially configured OIDC provider still reads the missing options
        // from the environment, so it only runs when every required option is
        // set here or when loading ambient configuration is allowed.
        let oidc_configured_explicitly = self.config.role_arn.is_some()
            && self.config.oidc_provider_arn.is_some()
            && self.config.oidc_token_file.is_some();

        let mut provider = ProvideCredentialChain::new().push(default_credential.build());
        if !self.config.disable_config_load || oidc_configured_explicitly {
            provider = provider.push(oidc);
        }
        provider = provider.push(EcsRamRoleCredentialProvider::new());

        // Explicit access keys take precedence over every ambient source.
        if let Some(static_provider) = static_provider {
            provider = provider.push_front(static_provider);
        }

        // When `role_arn` is configured, the credentials above call STS and the
        // assumed role signs requests.
        let provider = if let Some(role_arn) = &self.config.role_arn {
            let mut assume_role = AssumeRoleCredentialProvider::new()
                .with_base_provider(provider)
                .with_role_arn(role_arn);

            if let Some(v) = self.config.role_session_name.as_deref() {
                assume_role = assume_role.with_role_session_name(v);
            }
            if let Some(v) = self.config.external_id.as_deref() {
                assume_role = assume_role.with_external_id(v);
            }
            if let Some(v) = self.config.sts_endpoint.as_deref() {
                assume_role = assume_role.with_sts_endpoint(v);
            }

            ProvideCredentialChain::new().push(assume_role)
        } else {
            provider
        };

        let provider = if let Some(credential_providers) = self.credential_providers {
            credential_providers
        } else {
            provider
        };

        let sign_ctx = ctx;
        let credential_provider = Arc::new(provider);
        let request_signer = Arc::new(RequestSigner::new(bucket));

        let info = ServiceInfo::new(OSS_SCHEME, &root, bucket);
        let capability = Capability {
            stat: true,
            stat_with_if_match: true,
            stat_with_if_none_match: true,
            stat_with_version: true,

            read: true,
            read_with_suffix: true,

            read_with_if_match: true,
            read_with_if_none_match: true,
            read_with_version: true,
            read_with_if_modified_since: true,
            read_with_if_unmodified_since: true,

            write: true,
            write_can_empty: true,
            write_can_append: true,
            write_can_multi: true,
            write_with_cache_control: true,
            write_with_content_type: true,
            write_with_content_disposition: true,
            write_with_content_encoding: true,
            write_with_if_not_exists: true,

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
            delete_with_version: true,
            delete_max_size: Some(DEFAULT_BATCH_MAX_OPERATIONS),

            copy: true,

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

            shared: true,

            ..Default::default()
        };

        Ok(OssBackend {
            core: Arc::new(OssCore {
                info,
                capability,
                root,
                bucket: bucket.to_owned(),
                endpoint,
                host,
                presign_endpoint,
                skip_signature,
                sign_ctx,
                credential_provider,
                request_signer,
                signers: HttpTransportCache::new(),
                server_side_encryption,
                server_side_encryption_key_id,
            }),
        })
    }
}

#[derive(Debug, Clone)]
/// Aliyun Object Storage Service backend
pub struct OssBackend {
    pub(crate) core: Arc<OssCore>,
}

impl Service for OssBackend {
    type Reader = oio::StreamReader<OssReader>;
    type Writer = OssWriters;
    type Lister = OssListers;
    type Deleter = oio::BatchDeleter<OssDeleter>;
    type Copier = oio::OneShotCopier;
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
        let resp = self.core.oss_head_object(ctx, path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpStat::new(self.core.parse_metadata(path, resp.headers())?)),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("HeadObject"))
                    .with_caller_condition(args.is_conditional()),
                resp,
            )),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<OssReader> = {
            Ok(oio::StreamReader::new(OssReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: OssWriters = {
            let writer = OssWriter::new(self.core.clone(), ctx.clone(), path, args.clone());

            let w = if args.append() {
                OssWriters::Two(oio::AppendWriter::new(writer))
            } else {
                OssWriters::One(oio::MultipartWriter::new(
                    ctx.executor().clone(),
                    writer,
                    args.concurrent(),
                ))
            };

            Ok(w)
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::BatchDeleter<OssDeleter> = {
            Ok(oio::BatchDeleter::new(
                OssDeleter::new(self.core.clone(), ctx.clone()),
                self.core.capability.delete_max_size,
            ))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: OssListers = {
            let l = if args.versions() || args.deleted() {
                TwoWays::Two(oio::PageLister::new(OssObjectVersionsLister::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            } else {
                TwoWays::One(oio::PageLister::new(OssLister::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args.recursive(),
                    args.limit(),
                    args.start_after(),
                )))
            };

            Ok(l)
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
        let core = self.core.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();
        Ok(oio::OneShotCopier::new(async move {
            let source_size = match args.source_content_length_hint() {
                Some(size) => size,
                None => {
                    let stat_args: OpStat = options::StatOptions {
                        version: args.source_version().map(str::to_owned),
                        ..Default::default()
                    }
                    .into();
                    let resp = core.oss_head_object(&ctx, &from, &stat_args).await?;
                    match resp.status() {
                        StatusCode::OK => {
                            parse_into_metadata(&from, resp.headers())?.content_length()
                        }
                        _ => {
                            return Err(parse_error(
                                ErrorContext::new(ServiceOperation("HeadObject")),
                                resp,
                            ));
                        }
                    }
                }
            };
            let resp = core.oss_copy_object(&ctx, &from, &to).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => Ok(MetadataBuilder::file(source_size).build()),
                _ => Err(parse_error(
                    ErrorContext::new(ServiceOperation("CopyObject")),
                    resp,
                )),
            }
        }))
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
            let req = self
                .core
                .oss_copy_object_request(path, path, Some(version))?;
            let req = self.core.sign(ctx, req).await?;
            // Replaying after a lost confirmation can create a second version.
            let resp = self
                .core
                .send(ctx, req)
                .await
                .map_err(Error::set_permanent)?;
            return match resp.status() {
                StatusCode::OK => Ok(RpRestore::new()),
                status => Err(
                    parse_error(ErrorContext::new(ServiceOperation("CopyObject")), resp)
                        .with_permanent(status.is_server_error()),
                ),
            };
        }

        let resp = self
            .core
            .oss_list_object_versions(ctx, path, "", Some(1), "", "")
            .await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListObjectVersions")),
                resp,
            ));
        }

        let output: ListObjectVersionsOutput =
            quick_xml::de::from_reader(resp.into_body().reader())
                .map_err(new_xml_deserialize_error)
                .map_err(Error::set_temporary)?;
        let key = build_absolute_path(&self.core.root, path);
        if output
            .version
            .iter()
            .any(|version| version.key == key && version.is_latest)
        {
            return Ok(RpRestore::new());
        }
        let Some(marker) = output
            .delete_marker
            .into_iter()
            .find(|marker| marker.key == key && marker.is_latest)
        else {
            return Err(Error::new(
                ErrorKind::NotFound,
                "no live object or current delete marker exists",
            ));
        };
        if marker.version_id.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "current delete marker has no version ID",
            ));
        }

        let args = OpDelete::new().into_version(&marker.version_id);
        let req = self.core.oss_delete_object_request(path, &args)?;
        let req = self.core.sign(ctx, req).await?;
        // An uncertain DELETE may already have removed this marker. Replaying
        // restore would look up the next marker and could remove it as well.
        let resp = self
            .core
            .send(ctx, req)
            .await
            .map_err(Error::set_permanent)?;
        match resp.status() {
            StatusCode::NO_CONTENT => Ok(RpRestore::new()),
            status => Err(
                parse_error(ErrorContext::new(ServiceOperation("DeleteObject")), resp)
                    .with_permanent(status.is_server_error()),
            ),
        }
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.oss_head_object_request(path, true, v),
            PresignOperation::Read(range, v) => {
                self.core.oss_get_object_request(path, true, *range, v)
            }
            PresignOperation::Write(v) => {
                self.core
                    .oss_put_object_request(path, None, v, Buffer::new(), true)
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
        let req = self.core.sign_query(ctx, req, args.expire()).await?;

        // We don't need this request anymore, consume it directly.
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
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use bytes::Bytes;
    use reqsign_core::time::Timestamp;
    use reqsign_core::{FileRead, HttpSend, ProvideCredential, Signer, StaticEnv};

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct TestFileRead {
        files: HashMap<String, String>,
        reads: Arc<Mutex<Vec<String>>>,
    }

    impl TestFileRead {
        fn new(files: impl IntoIterator<Item = (&'static str, &'static str)>) -> Self {
            Self {
                files: files
                    .into_iter()
                    .map(|(path, content)| (path.to_string(), content.to_string()))
                    .collect(),
                reads: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn reads(&self) -> Vec<String> {
            self.reads.lock().unwrap().clone()
        }
    }

    impl FileRead for TestFileRead {
        async fn file_read(&self, path: &str) -> reqsign_core::Result<Vec<u8>> {
            self.reads.lock().unwrap().push(path.to_string());
            self.files
                .get(path)
                .map(|content| content.as_bytes().to_vec())
                .ok_or_else(|| {
                    reqsign_core::Error::unexpected(format!("unexpected file path: {path}"))
                })
        }
    }

    #[derive(Debug)]
    struct TestCredentialProvider {
        calls: Arc<AtomicUsize>,
        credentials: Mutex<VecDeque<Credential>>,
    }

    impl TestCredentialProvider {
        fn new(calls: Arc<AtomicUsize>, credentials: Vec<Credential>) -> Self {
            Self {
                calls,
                credentials: Mutex::new(credentials.into_iter().collect()),
            }
        }
    }

    impl ProvideCredential for TestCredentialProvider {
        type Credential = Credential;

        async fn provide_credential(
            &self,
            _ctx: &Context,
        ) -> reqsign_core::Result<Option<Credential>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(
                self.credentials
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("test provider credential"),
            ))
        }
    }

    async fn sign_test_request(signer: &Signer<Credential>) -> http::request::Parts {
        let (mut parts, _) = http::Request::builder()
            .method(http::Method::PUT)
            .uri("https://test-bucket.oss-cn-hangzhou.aliyuncs.com/manifest.json")
            .body(())
            .unwrap()
            .into_parts();
        signer.sign(&mut parts, None).await.unwrap();
        parts
    }

    #[derive(Clone, Debug, Default)]
    struct CredentialHttpSend {
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl HttpSend for CredentialHttpSend {
        async fn http_send(
            &self,
            req: http::Request<Bytes>,
        ) -> reqsign_core::Result<http::Response<Bytes>> {
            let host = req.uri().host().unwrap();
            let path = req.uri().path();
            self.requests.lock().unwrap().push(format!("{host}{path}"));
            let body = match (host, path) {
                ("sts.aliyuncs.com", "/") => {
                    assert!(
                        req.uri()
                            .query()
                            .unwrap()
                            .contains("Action=AssumeRoleWithOIDC")
                    );
                    r#"{"Credentials":{"AccessKeyId":"oidc-ak","AccessKeySecret":"oidc-secret","SecurityToken":"oidc-token","Expiration":"2099-01-01T00:00:00Z"}}"#
                }
                ("sts.example.com", "/") => {
                    let query = req.uri().query().unwrap();
                    if query.contains("Action=AssumeRoleWithOIDC") {
                        assert!(query.contains("RoleSessionName=builder-session"));
                        assert!(query.contains("OIDCToken=test-oidc-token"));
                        r#"{"Credentials":{"AccessKeyId":"oidc-ak","AccessKeySecret":"oidc-secret","SecurityToken":"oidc-token","Expiration":"2099-01-01T00:00:00Z"}}"#
                    } else {
                        assert!(query.contains("Action=AssumeRole"));
                        assert!(query.contains("RoleSessionName=builder-session"));
                        r#"{"Credentials":{"AccessKeyId":"assumed-ak","AccessKeySecret":"assumed-secret","SecurityToken":"assumed-token","Expiration":"2099-01-01T00:00:00Z"}}"#
                    }
                }
                ("100.100.100.200", "/latest/api/token") => "metadata-token",
                ("100.100.100.200", "/latest/meta-data/ram/security-credentials/") => "node-role",
                ("100.100.100.200", "/latest/meta-data/ram/security-credentials/node-role") => {
                    r#"{"Code":"Success","AccessKeyId":"ecs-ak","AccessKeySecret":"ecs-secret","SecurityToken":"ecs-token","Expiration":"2099-01-01T00:00:00Z"}"#
                }
                _ => panic!("unexpected credential request: {}", req.uri()),
            };
            Ok(http::Response::new(Bytes::from_static(body.as_bytes())))
        }
    }

    #[tokio::test]
    async fn test_credential_precedence() {
        for (static_keys, env_keys, oidc, expected_key, expected_token) in [
            (true, true, true, "static-ak", "static-token"),
            (false, true, true, "env-ak", "env-token"),
            (false, false, true, "oidc-ak", "oidc-token"),
            (false, false, false, "ecs-ak", "ecs-token"),
        ] {
            let mut builder = OssBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com");
            if static_keys {
                builder = builder
                    .access_key_id("static-ak")
                    .access_key_secret("static-secret")
                    .security_token("static-token");
            }
            let backend = builder.build().unwrap();
            let backend = (&backend as &dyn std::any::Any)
                .downcast_ref::<OssBackend>()
                .unwrap();
            let mut envs = HashMap::new();
            if env_keys {
                envs.extend([
                    ("ALIBABA_CLOUD_ACCESS_KEY_ID".into(), "env-ak".into()),
                    (
                        "ALIBABA_CLOUD_ACCESS_KEY_SECRET".into(),
                        "env-secret".into(),
                    ),
                    ("ALIBABA_CLOUD_SECURITY_TOKEN".into(), "env-token".into()),
                ]);
            }
            if oidc {
                envs.extend([
                    (
                        "ALIBABA_CLOUD_ROLE_ARN".into(),
                        "acs:ram::123456789012:role/pod-role".into(),
                    ),
                    (
                        "ALIBABA_CLOUD_OIDC_PROVIDER_ARN".into(),
                        "acs:ram::123456789012:oidc-provider/test".into(),
                    ),
                    (
                        "ALIBABA_CLOUD_OIDC_TOKEN_FILE".into(),
                        "/test/oidc-token".into(),
                    ),
                ]);
            }
            let http = CredentialHttpSend::default();
            let ctx = Context::new()
                .with_env(StaticEnv {
                    home_dir: None,
                    envs,
                })
                .with_file_read(TestFileRead::new([("/test/oidc-token", "test-oidc-token")]))
                .with_http_send(http.clone());
            let signer = backend.core.signer_with_context(ctx);
            let (mut parts, _) = http::Request::builder()
                .method(http::Method::PUT)
                .uri("https://test-bucket.oss-cn-hangzhou.aliyuncs.com/manifest.json")
                .body(())
                .unwrap()
                .into_parts();
            signer.sign(&mut parts, None).await.unwrap();
            assert!(
                parts.headers[http::header::AUTHORIZATION]
                    .to_str()
                    .unwrap()
                    .starts_with(&format!("OSS {expected_key}:"))
            );
            assert_eq!(parts.headers["x-oss-security-token"], expected_token);
            let expected_requests = if static_keys || env_keys {
                vec![]
            } else if oidc {
                vec!["sts.aliyuncs.com/"]
            } else {
                vec![
                    "100.100.100.200/latest/api/token",
                    "100.100.100.200/latest/meta-data/ram/security-credentials/",
                    "100.100.100.200/latest/meta-data/ram/security-credentials/node-role",
                ]
            };
            assert_eq!(*http.requests.lock().unwrap(), expected_requests);
        }
    }

    #[tokio::test]
    async fn test_custom_credential_provider_replaces_default_chain() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = TestCredentialProvider::new(
            calls.clone(),
            vec![Credential {
                access_key_id: "custom-ak".to_string(),
                access_key_secret: "custom-secret".to_string(),
                security_token: Some("custom-token".to_string()),
                expires_in: Some(Timestamp::now() + std::time::Duration::from_secs(3600)),
            }],
        );
        let builder = OssBuilder::default()
            .bucket("test-bucket")
            .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
            .access_key_id("static-ak")
            .access_key_secret("static-secret")
            .security_token("static-token")
            .role_arn("acs:ram::123456789012:role/test-role")
            .oidc_provider_arn("acs:ram::123456789012:oidc-provider/test")
            .oidc_token_file("/test/oidc-token")
            .credential_provider_chain(ProvideCredentialChain::new().push(provider));

        let debug = format!("{builder:?}");
        for value in [
            "static-ak",
            "static-secret",
            "static-token",
            "custom-ak",
            "custom-secret",
            "custom-token",
        ] {
            assert!(!debug.contains(value));
        }

        let backend = builder.build().unwrap();
        let backend = (&backend as &dyn std::any::Any)
            .downcast_ref::<OssBackend>()
            .unwrap();
        let envs = HashMap::from([
            ("ALIBABA_CLOUD_ACCESS_KEY_ID".into(), "env-ak".into()),
            (
                "ALIBABA_CLOUD_ACCESS_KEY_SECRET".into(),
                "env-secret".into(),
            ),
            ("ALIBABA_CLOUD_SECURITY_TOKEN".into(), "env-token".into()),
            (
                "ALIBABA_CLOUD_ROLE_ARN".into(),
                "acs:ram::123456789012:role/env-role".into(),
            ),
            (
                "ALIBABA_CLOUD_OIDC_PROVIDER_ARN".into(),
                "acs:ram::123456789012:oidc-provider/env".into(),
            ),
            (
                "ALIBABA_CLOUD_OIDC_TOKEN_FILE".into(),
                "/test/oidc-token".into(),
            ),
        ]);
        let http = CredentialHttpSend::default();
        let ctx = Context::new()
            .with_env(StaticEnv {
                home_dir: None,
                envs,
            })
            .with_file_read(TestFileRead::new([("/test/oidc-token", "test-oidc-token")]))
            .with_http_send(http.clone());
        let signer = backend.core.signer_with_context(ctx);

        let parts = sign_test_request(&signer).await;

        assert!(
            parts.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS custom-ak:")
        );
        assert_eq!(parts.headers["x-oss-security-token"], "custom-token");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[derive(Clone, Default)]
    struct PanicTransport {
        dropped: Option<Arc<AtomicBool>>,
    }

    impl Drop for PanicTransport {
        fn drop(&mut self) {
            if let Some(dropped) = &self.dropped {
                dropped.store(true, Ordering::SeqCst);
            }
        }
    }

    impl HttpTransport for PanicTransport {
        async fn fetch(&self, req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
            panic!("unexpected HTTP request: {}", req.uri())
        }
    }

    #[tokio::test]
    async fn test_custom_credential_provider_refreshes_expiring_credentials() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = TestCredentialProvider::new(
            calls.clone(),
            vec![
                Credential {
                    access_key_id: "custom-ak-1".to_string(),
                    access_key_secret: "custom-secret-1".to_string(),
                    security_token: Some("custom-token-1".to_string()),
                    expires_in: Some(Timestamp::now() + std::time::Duration::from_secs(60)),
                },
                Credential {
                    access_key_id: "custom-ak-2".to_string(),
                    access_key_secret: "custom-secret-2".to_string(),
                    security_token: Some("custom-token-2".to_string()),
                    expires_in: Some(Timestamp::now() + std::time::Duration::from_secs(3600)),
                },
            ],
        );
        let backend = OssBuilder::default()
            .bucket("test-bucket")
            .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
            .credential_provider_chain(ProvideCredentialChain::new().push(provider))
            .build()
            .unwrap();
        let backend = (&backend as &dyn std::any::Any)
            .downcast_ref::<OssBackend>()
            .unwrap();
        let op_ctx = OperationContext::new()
            .with_http_transport(HttpTransporter::new(PanicTransport::default()));

        let first = sign_test_request(&backend.core.signer(&op_ctx)).await;
        assert!(
            first.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS custom-ak-1:")
        );
        assert_eq!(first.headers["x-oss-security-token"], "custom-token-1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let second = sign_test_request(&backend.core.signer(&op_ctx)).await;
        assert!(
            second.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS custom-ak-2:")
        );
        assert_eq!(second.headers["x-oss-security-token"], "custom-token-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let third = sign_test_request(&backend.core.signer(&op_ctx)).await;
        assert!(
            third.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS custom-ak-2:")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_signer_cache_isolates_distinct_transports() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = TestCredentialProvider::new(
            calls.clone(),
            vec![
                Credential {
                    access_key_id: "ak-1".to_string(),
                    access_key_secret: "sk-1".to_string(),
                    security_token: Some("token-1".to_string()),
                    expires_in: Some(Timestamp::now() + std::time::Duration::from_secs(3600)),
                },
                Credential {
                    access_key_id: "ak-2".to_string(),
                    access_key_secret: "sk-2".to_string(),
                    security_token: Some("token-2".to_string()),
                    expires_in: Some(Timestamp::now() + std::time::Duration::from_secs(3600)),
                },
                Credential {
                    access_key_id: "ak-3".to_string(),
                    access_key_secret: "sk-3".to_string(),
                    security_token: Some("token-3".to_string()),
                    expires_in: Some(Timestamp::now() + std::time::Duration::from_secs(3600)),
                },
            ],
        );
        let core = build_test_core(
            OssBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .credential_provider_chain(ProvideCredentialChain::new().push(provider)),
        );

        let ctx1 = OperationContext::new()
            .with_http_transport(HttpTransporter::new(PanicTransport::default()));
        let first = sign_test_request(&core.signer(&ctx1)).await;
        let second = sign_test_request(&core.signer(&ctx1)).await;
        assert!(
            first.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS ak-1:")
        );
        assert!(
            second.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS ak-1:")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let t2_dropped = Arc::new(AtomicBool::new(false));
        let ctx2 =
            OperationContext::new().with_http_transport(HttpTransporter::new(PanicTransport {
                dropped: Some(t2_dropped.clone()),
            }));
        let third = sign_test_request(&core.signer(&ctx2)).await;
        let fourth = sign_test_request(&core.signer(&ctx2)).await;
        assert!(
            third.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS ak-2:")
        );
        assert!(
            fourth.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS ak-2:")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // ctx1 must continue reusing its own cached signer and credential.
        let fifth = sign_test_request(&core.signer(&ctx1)).await;
        assert!(
            fifth.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS ak-1:")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // Dropping ctx2 must release t2 immediately (no strong reference cycle).
        drop(ctx2);
        assert!(t2_dropped.load(Ordering::SeqCst));

        // A third transport prunes t2's dead entry and stays isolated.
        let ctx3 = OperationContext::new()
            .with_http_transport(HttpTransporter::new(PanicTransport::default()));
        let sixth = sign_test_request(&core.signer(&ctx3)).await;
        assert!(
            sixth.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS ak-3:")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    fn build_test_core(builder: OssBuilder) -> Arc<OssCore> {
        let backend = builder.build().unwrap();
        let backend = (&backend as &dyn std::any::Any)
            .downcast_ref::<OssBackend>()
            .unwrap();
        backend.core.clone()
    }

    #[test]
    fn test_reqsign_aliyun_oss_is_reexported() {
        // `credential_provider_chain` takes providers built from the
        // re-exported crate, so this only compiles while the re-export exists
        // and matches the types used by the service.
        let provider = crate::reqsign_aliyun_oss::StaticCredentialProvider::new("ak", "sk");
        let chain = ProvideCredentialChain::new().push(provider);
        let _ = OssBuilder::default().credential_provider_chain(chain);
    }

    #[tokio::test]
    async fn test_oidc_credentials_from_builder_config() {
        let file_read = TestFileRead::new([("/test/oidc-token", "test-oidc-token")]);
        let http = CredentialHttpSend::default();
        // No `ALIBABA_CLOUD_OIDC_*` or `ALIBABA_CLOUD_ROLE_ARN` variables: the
        // STS request can only be built when every OIDC option comes from the
        // builder.
        let ctx = Context::new()
            .with_env(StaticEnv {
                home_dir: None,
                envs: HashMap::from([(
                    "ALIBABA_CLOUD_ECS_METADATA_DISABLED".to_string(),
                    "true".to_string(),
                )]),
            })
            .with_file_read(file_read.clone())
            .with_http_send(http.clone());

        let core = build_test_core(
            OssBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .role_arn("acs:ram::123:role/builder-role")
                .oidc_provider_arn("acs:ram::123:oidc-provider/builder-provider")
                .oidc_token_file("/test/oidc-token")
                .sts_endpoint("sts.example.com")
                .role_session_name("builder-session"),
        );
        let parts = sign_test_request(&core.signer_with_context(ctx)).await;

        // `role_arn` wraps the OIDC credential with an AK-based AssumeRole call.
        assert!(
            parts.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS assumed-ak:")
        );
        assert_eq!(parts.headers["x-oss-security-token"], "assumed-token");
        assert_eq!(file_read.reads(), vec!["/test/oidc-token".to_string()]);
        assert_eq!(
            *http.requests.lock().unwrap(),
            vec![
                "sts.example.com/".to_string(),
                "sts.example.com/".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn test_default_credential_provider_loads_oss_profile_file() {
        let http = CredentialHttpSend::default();
        let file_read = TestFileRead::new([(
            "/mock/oss-credentials",
            "[default]\naccess_key_id = profile-ak\naccess_key_secret = profile-secret\nsecurity_token = profile-token\n",
        )]);
        let ctx = Context::new()
            .with_env(StaticEnv {
                home_dir: None,
                envs: HashMap::from([
                    (
                        "OSS_CREDENTIAL_PROFILES_FILE".to_string(),
                        "/mock/oss-credentials".to_string(),
                    ),
                    (
                        "ALIBABA_CLOUD_ECS_METADATA_DISABLED".to_string(),
                        "true".to_string(),
                    ),
                ]),
            })
            .with_file_read(file_read.clone())
            .with_http_send(http.clone());

        let core = build_test_core(
            OssBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com"),
        );
        let parts = sign_test_request(&core.signer_with_context(ctx)).await;
        assert!(
            parts.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS profile-ak:")
        );
        assert_eq!(parts.headers["x-oss-security-token"], "profile-token");
        assert_eq!(file_read.reads(), vec!["/mock/oss-credentials".to_string()]);
        assert!(http.requests.lock().unwrap().is_empty());

        // Explicit access keys still take precedence over the profile file.
        let static_file_read = TestFileRead::new([]);
        let ctx = Context::new()
            .with_env(StaticEnv {
                home_dir: None,
                envs: HashMap::from([
                    (
                        "OSS_CREDENTIAL_PROFILES_FILE".to_string(),
                        "/mock/oss-credentials".to_string(),
                    ),
                    (
                        "ALIBABA_CLOUD_ECS_METADATA_DISABLED".to_string(),
                        "true".to_string(),
                    ),
                ]),
            })
            .with_file_read(static_file_read.clone())
            .with_http_send(http.clone());

        let core = build_test_core(
            OssBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .access_key_id("static-ak")
                .access_key_secret("static-secret")
                .security_token("static-token"),
        );
        let parts = sign_test_request(&core.signer_with_context(ctx)).await;
        assert!(
            parts.headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("OSS static-ak:")
        );
        assert_eq!(parts.headers["x-oss-security-token"], "static-token");
        assert!(static_file_read.reads().is_empty());
    }

    #[tokio::test]
    async fn test_disable_config_load_skips_ambient_credentials() {
        let file_read = TestFileRead::new([(
            "/mock/oss-credentials",
            "[default]\naccess_key_id = profile-ak\naccess_key_secret = profile-secret\n",
        )]);
        let http = CredentialHttpSend::default();
        let ctx = Context::new()
            .with_env(StaticEnv {
                home_dir: None,
                envs: HashMap::from([
                    (
                        "ALIBABA_CLOUD_ACCESS_KEY_ID".to_string(),
                        "env-ak".to_string(),
                    ),
                    (
                        "ALIBABA_CLOUD_ACCESS_KEY_SECRET".to_string(),
                        "env-secret".to_string(),
                    ),
                    (
                        "ALIBABA_CLOUD_SECURITY_TOKEN".to_string(),
                        "env-token".to_string(),
                    ),
                    (
                        "ALIBABA_CLOUD_CREDENTIALS_URI".to_string(),
                        "http://127.0.0.1/credentials".to_string(),
                    ),
                    (
                        "OSS_CREDENTIAL_PROFILES_FILE".to_string(),
                        "/mock/oss-credentials".to_string(),
                    ),
                    (
                        "ALIBABA_CLOUD_ECS_METADATA_DISABLED".to_string(),
                        "true".to_string(),
                    ),
                ]),
            })
            .with_file_read(file_read.clone())
            .with_http_send(http.clone());

        let core = build_test_core(
            OssBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .disable_config_load(),
        );

        let (mut parts, _) = http::Request::builder()
            .method(http::Method::PUT)
            .uri("https://test-bucket.oss-cn-hangzhou.aliyuncs.com/manifest.json")
            .body(())
            .unwrap()
            .into_parts();
        let err = core
            .signer_with_context(ctx)
            .sign(&mut parts, None)
            .await
            .expect_err("ambient credentials must not be loaded");
        assert_eq!(err.to_string(), "failed to load signing credential");
        assert!(file_read.reads().is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_presign_write_omits_content_length_header() {
        let op = Operator::new(
            OssBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .access_key_id("static-ak")
                .access_key_secret("static-secret")
                .disable_config_load(),
        )
        .unwrap();

        let presigned = op
            .presign_write("test.txt", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(presigned.method(), http::Method::PUT);
        assert!(
            presigned
                .header()
                .get(http::header::CONTENT_LENGTH)
                .is_none(),
            "presign_write must not inject Content-Length: 0"
        );

        let presigned_with_type = op
            .presign_write_with("test.txt", std::time::Duration::from_secs(60))
            .content_type("text/plain")
            .await
            .unwrap();
        assert!(
            presigned_with_type
                .header()
                .get(http::header::CONTENT_LENGTH)
                .is_none()
        );
        assert_eq!(
            presigned_with_type
                .header()
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/plain")
        );

        let core = build_test_core(
            OssBuilder::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .skip_signature()
                .disable_config_load(),
        );
        let put_req = core
            .oss_put_object_request(
                "test.txt",
                Some(5),
                &OpWrite::new(),
                Buffer::from("hello"),
                false,
            )
            .unwrap();
        assert_eq!(put_req.headers()[http::header::CONTENT_LENGTH], "5");

        let empty_put_req = core
            .oss_put_object_request("empty.txt", Some(0), &OpWrite::new(), Buffer::new(), false)
            .unwrap();
        assert_eq!(empty_put_req.headers()[http::header::CONTENT_LENGTH], "0");
    }
}
