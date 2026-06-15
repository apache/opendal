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
use log::debug;
use reqsign_core::Context;
use reqsign_core::Env as _;
use reqsign_core::OsEnv;
use reqsign_core::ProvideCredential;
use reqsign_core::ProvideCredentialChain;
use reqsign_core::Signer;
use reqsign_core::StaticEnv;
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_google::Credential;
use reqsign_google::DefaultCredentialProvider;
use reqsign_google::FileCredentialProvider;
use reqsign_google::RequestSigner;
use reqsign_google::StaticCredentialProvider;
use reqsign_google::TokenCredentialProvider;
use reqsign_google::VmMetadataCredentialProvider;

use super::GCS_SCHEME;
use super::config::GcsConfig;
use super::copier::GcsCopier;
use super::core::constants::GCS_REWRITE_MAX_CHUNK_SIZE;
use super::core::constants::GCS_REWRITE_MIN_CHUNK_SIZE;
use super::core::*;
use super::deleter::GcsDeleter;
use super::error::parse_error;
use super::lister::GcsLister;
use super::writer::GcsWriter;
use super::writer::GcsWriters;
use opendal_core::raw::*;
use opendal_core::*;

const DEFAULT_GCS_ENDPOINT: &str = "https://storage.googleapis.com";
const DEFAULT_GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// [Google Cloud Storage](https://cloud.google.com/storage) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GcsBuilder {
    pub(super) config: GcsConfig,
    pub(super) credential_provider_chain: Option<ProvideCredentialChain<Credential>>,
}

impl Debug for GcsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GcsBuilder {
    /// set the working directory root of backend
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// set the container's name
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();
        self
    }

    /// set the GCS service scope
    ///
    /// If not set, we will use `https://www.googleapis.com/auth/devstorage.read_write`.
    ///
    /// # Valid scope examples
    ///
    /// - read-only: `https://www.googleapis.com/auth/devstorage.read_only`
    /// - read-write: `https://www.googleapis.com/auth/devstorage.read_write`
    /// - full-control: `https://www.googleapis.com/auth/devstorage.full_control`
    ///
    /// Reference: [Cloud Storage authentication](https://cloud.google.com/storage/docs/authentication)
    pub fn scope(mut self, scope: &str) -> Self {
        if !scope.is_empty() {
            self.config.scope = Some(scope.to_string())
        };
        self
    }

    /// Set the GCS service account.
    ///
    /// service account will be used for fetch token from vm metadata.
    /// If not set, we will try to fetch with `default` service account.
    pub fn service_account(mut self, service_account: &str) -> Self {
        if !service_account.is_empty() {
            self.config.service_account = Some(service_account.to_string())
        };
        self
    }

    /// set the endpoint GCS service uses
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.to_string())
        };
        self
    }

    /// set the base64 hashed credentials string used for OAuth2 authentication.
    ///
    /// this method allows to specify the credentials directly as a base64 hashed string.
    /// alternatively, you can use `credential_path()` to provide the local path to a credentials file.
    /// we will use one of `credential` and `credential_path` to complete the OAuth2 authentication.
    ///
    /// Reference: [Google Cloud Storage Authentication](https://cloud.google.com/docs/authentication).
    pub fn credential(mut self, credential: &str) -> Self {
        if !credential.is_empty() {
            self.config.credential = Some(credential.to_string())
        };
        self
    }

    /// set the local path to credentials file which is used for OAuth2 authentication.
    ///
    /// credentials file contains the original credentials that have not been base64 hashed.
    /// we will use one of `credential` and `credential_path` to complete the OAuth2 authentication.
    ///
    /// Reference: [Google Cloud Storage Authentication](https://cloud.google.com/docs/authentication).
    pub fn credential_path(mut self, path: &str) -> Self {
        if !path.is_empty() {
            self.config.credential_path = Some(path.to_string())
        };
        self
    }

    /// Specify a customized credential provider used by this service.
    ///
    /// This provider will be pushed to the front of credential chain.
    pub fn credential_provider(
        mut self,
        provider: impl ProvideCredential<Credential = Credential> + 'static,
    ) -> Self {
        let chain = self.credential_provider_chain.unwrap_or_default();
        self.credential_provider_chain = Some(chain.push_front(provider));
        self
    }

    /// Specify a customized credential provider chain used by this service.
    ///
    /// This chain will be pushed to the front of default chain.
    pub fn credential_provider_chain(mut self, chain: ProvideCredentialChain<Credential>) -> Self {
        self.credential_provider_chain = Some(chain);
        self
    }

    /// Provide the OAuth2 token to use.
    pub fn token(mut self, token: String) -> Self {
        self.config.token = Some(token);
        self
    }

    /// Disable attempting to load credentials from the GCE metadata server.
    pub fn disable_vm_metadata(mut self) -> Self {
        self.config.disable_vm_metadata = true;
        self
    }

    /// Disable loading configuration from the environment.
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
        self
    }

    /// Set the predefined acl for GCS.
    ///
    /// Available values are:
    /// - `authenticatedRead`
    /// - `bucketOwnerFullControl`
    /// - `bucketOwnerRead`
    /// - `private`
    /// - `projectPrivate`
    /// - `publicRead`
    pub fn predefined_acl(mut self, acl: &str) -> Self {
        if !acl.is_empty() {
            self.config.predefined_acl = Some(acl.to_string())
        };
        self
    }

    /// Set the default storage class for GCS.
    ///
    /// Available values are:
    /// - `STANDARD`
    /// - `NEARLINE`
    /// - `COLDLINE`
    /// - `ARCHIVE`
    pub fn default_storage_class(mut self, class: &str) -> Self {
        if !class.is_empty() {
            self.config.default_storage_class = Some(class.to_string())
        };
        self
    }

    /// Skip signature will skip loading credentials and signing requests.
    ///
    /// This is typically used for buckets which are open to the public or GCS
    /// storage emulators.
    pub fn skip_signature(mut self) -> Self {
        self.config.skip_signature = true;
        self
    }

    /// Allow anonymous requests.
    #[deprecated(
        since = "0.57.0",
        note = "Please use `skip_signature` instead of `allow_anonymous`"
    )]
    pub fn allow_anonymous(self) -> Self {
        self.skip_signature()
    }
}

impl Builder for GcsBuilder {
    type Config = GcsConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {self:?}");

        #[allow(deprecated)]
        let skip_signature = self.config.skip_signature || self.config.allow_anonymous;

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        // Handle endpoint and bucket name
        let bucket = match self.config.bucket.is_empty() {
            false => Ok(&self.config.bucket),
            true => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_operation("Builder::build")
                    .with_context("service", GCS_SCHEME),
            ),
        }?;

        // TODO: server side encryption

        let endpoint = self
            .config
            .endpoint
            .clone()
            .unwrap_or_else(|| DEFAULT_GCS_ENDPOINT.to_string());
        debug!("backend use endpoint: {endpoint}");

        let scope = self
            .config
            .scope
            .clone()
            .unwrap_or_else(|| DEFAULT_GCS_SCOPE.to_string());

        let os_env = OsEnv;
        let mut envs = os_env.vars();
        envs.insert("GOOGLE_SCOPE".to_string(), scope.clone());

        let ctx = Context::new()
            .with_file_read(TokioFileRead)
            .with_env(StaticEnv {
                home_dir: os_env.home_dir(),
                envs,
            });

        let mut default_credential = DefaultCredentialProvider::builder();
        #[cfg(target_arch = "wasm32")]
        {
            default_credential = default_credential.no_env().no_well_known();
        }

        if self.config.disable_config_load {
            default_credential = default_credential.no_env().no_well_known();
        }

        if self.config.disable_vm_metadata || self.config.service_account.is_some() {
            default_credential = default_credential.no_vm_metadata();
        }

        let mut credential_chain = ProvideCredentialChain::new().push(default_credential.build());

        if !self.config.disable_vm_metadata {
            if let Some(service_account) = self.config.service_account.as_deref() {
                credential_chain = credential_chain.push(
                    VmMetadataCredentialProvider::new()
                        .with_scope(&scope)
                        .with_service_account(service_account),
                );
            }
        }

        if let Some(path) = self.config.credential_path.as_deref() {
            credential_chain =
                credential_chain.push_front(FileCredentialProvider::new(path).with_scope(&scope));
        }

        if let Some(content) = self.config.credential.as_deref() {
            if let Ok(provider) = StaticCredentialProvider::from_base64(content) {
                credential_chain = credential_chain.push_front(provider.with_scope(&scope));
            }
        }

        if let Some(token) = self.config.token.as_deref() {
            credential_chain = credential_chain.push_front(TokenCredentialProvider::new(token));
        }

        if let Some(customized_credential_chain) = self.credential_provider_chain {
            credential_chain = credential_chain.push_front(customized_credential_chain);
        }

        let sign_ctx = ctx;
        let signer = Signer::new(
            sign_ctx.clone(),
            credential_chain,
            RequestSigner::new("storage").with_scope(&scope),
        );

        let info = ServiceInfo::new(GCS_SCHEME, &root, bucket);
        let capability = Capability {
            stat: true,
            stat_with_if_match: true,
            stat_with_if_none_match: true,

            read: true,
            read_with_suffix: true,

            read_with_if_match: true,
            read_with_if_none_match: true,

            write: true,
            write_can_empty: true,
            write_can_multi: true,
            write_with_cache_control: true,
            write_with_content_type: true,
            write_with_content_encoding: true,
            write_with_user_metadata: true,
            write_with_if_not_exists: true,

            // The min multipart size of Gcs is 5 MiB.
            //
            // ref: <https://cloud.google.com/storage/docs/xml-api/put-object-multipart>
            write_multi_min_size: Some(5 * 1024 * 1024),
            // The max multipart size of Gcs is 5 GiB.
            //
            // ref: <https://cloud.google.com/storage/docs/xml-api/put-object-multipart>
            write_multi_max_size: if cfg!(target_pointer_width = "64") {
                Some(5 * 1024 * 1024 * 1024)
            } else {
                Some(usize::MAX)
            },

            delete: true,
            delete_max_size: Some(100),

            copy: true,
            copy_can_multi: true,
            // GCS rewrite requires maxBytesRewrittenPerCall to be an
            // integral multiple of 1 MiB if specified.
            //
            // ref: <https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite>
            copy_multi_min_size: Some(GCS_REWRITE_MIN_CHUNK_SIZE),
            copy_multi_max_size: Some(GCS_REWRITE_MAX_CHUNK_SIZE),

            list: true,
            list_with_limit: true,
            list_with_start_after: true,
            list_with_recursive: true,

            presign: true,
            presign_stat: true,
            presign_read: true,
            presign_write: true,

            shared: true,

            ..Default::default()
        };

        let backend = GcsBackend {
            core: Arc::new(GcsCore {
                info,
                capability,
                endpoint,
                bucket: bucket.to_string(),
                root,
                signer,
                sign_ctx,
                predefined_acl: self.config.predefined_acl.clone(),
                default_storage_class: self.config.default_storage_class.clone(),
                skip_signature,
            }),
        };

        Ok(backend)
    }
}

/// GCS storage backend
#[derive(Clone, Debug)]
pub struct GcsBackend {
    core: Arc<GcsCore>,
}

/// Reader returned by this backend.
pub struct GcsReader {
    backend: GcsBackend,
    // StreamReader opens requests lazily, so keep the per-operation context
    // available for later range requests.
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl GcsReader {
    fn new(backend: GcsBackend, ctx: OperationContext, path: &str, args: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for GcsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .gcs_get_object(&self.ctx, path, range, &args)
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

impl Service for GcsBackend {
    type Reader = oio::StreamReader<GcsReader>;
    type Writer = GcsWriters;
    type Lister = oio::PageLister<GcsLister>;
    type Deleter = oio::BatchDeleter<GcsDeleter>;
    type Copier = GcsCopier;

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.gcs_get_object_metadata(ctx, path, &args).await?;

        if !resp.status().is_success() {
            return Err(parse_error(resp));
        }

        let slc = resp.into_body();
        let m = GcsCore::build_metadata_from_object_response(path, slc)?;

        Ok(RpStat::new(m))
    }
    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<GcsReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(GcsReader::new(self.clone(), ctx.clone(), path, args)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, GcsWriters) = {
            let concurrent = args.concurrent();
            let w = GcsWriter::new(self.core.clone(), ctx.clone(), path, args);
            // Multipart uploads schedule work through the operation executor
            // supplied by the caller.
            let w = oio::MultipartWriter::new(ctx.executor().clone(), w, concurrent);

            Ok((RpWrite::default(), w))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::BatchDeleter<GcsDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::BatchDeleter::new(
                    GcsDeleter::new(self.core.clone(), ctx.clone()),
                    self.core.capability.delete_max_size,
                ),
            ))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, oio::PageLister<GcsLister>) = {
            let l = GcsLister::new(
                self.core.clone(),
                ctx.clone(),
                path,
                args.recursive(),
                args.limit(),
                args.start_after(),
            );

            Ok((RpList::default(), oio::PageLister::new(l)))
        }?;

        Ok((rp, output))
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        let (rp, output): (_, GcsCopier) = {
            let copier = GcsCopier::new(self.core.clone(), ctx.clone(), from, to, args, opts);
            Ok((RpCopy::default(), copier))
        }?;

        Ok((rp, output))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.gcs_head_object_xml_request(path, v),
            PresignOperation::Read(range, v) => {
                self.core.gcs_get_object_xml_request(path, *range, v)
            }
            PresignOperation::Write(v) => {
                self.core
                    .gcs_insert_object_xml_request(path, v, Buffer::new())
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
