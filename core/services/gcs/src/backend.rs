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

use log::debug;
#[cfg(not(target_arch = "wasm32"))]
use reqsign_command_execute_tokio::TokioCommandExecute;
use reqsign_core::OsEnv;
use reqsign_core::ProvideCredential;
use reqsign_core::ProvideCredentialChain;
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_google::Credential;
use reqsign_google::DefaultCredentialProvider;
use reqsign_google::EnvCredentialProvider;
use reqsign_google::FileCredentialProvider;
use reqsign_google::RequestSigner;
use reqsign_google::StaticCredentialProvider;
use reqsign_google::TokenCredentialProvider;
use reqsign_google::VmMetadataCredentialProvider;
use reqsign_google::WellKnownCredentialProvider;

use super::GCS_SCHEME;
use super::composer::GcsComposer;
use super::config::GcsConfig;
use super::copier::GcsCopier;
use super::core::constants::GCS_REWRITE_MAX_CHUNK_SIZE;
use super::core::constants::GCS_REWRITE_MIN_CHUNK_SIZE;
use super::core::parse_error;
use super::core::*;
use super::deleter::GcsDeleter;
use super::lister::GcsLister;
use super::reader::*;
use super::writer::GcsResumableWriter;
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
    ///
    /// Implement the provider against [`opendal_core::reqsign_core`] and
    /// [`crate::reqsign_google`] so that its types match the ones this service is
    /// built with.
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
    ///
    /// Build the chain from [`opendal_core::reqsign_core`] and
    /// [`crate::reqsign_google`] so that its types match the ones this service is
    /// built with.
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

impl GcsBuilder {
    fn build_backend(self) -> Result<GcsBackend> {
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

        let ctx = opendal_core::raw::reqsign_context()
            .with_file_read(TokioFileRead)
            .with_env(OsEnv);
        #[cfg(not(target_arch = "wasm32"))]
        let ctx = ctx.with_command_execute(TokioCommandExecute);

        let mut default_credential = DefaultCredentialProvider::builder();
        if self.config.disable_config_load || cfg!(target_arch = "wasm32") {
            default_credential = default_credential.no_env().no_well_known();
        } else {
            default_credential = default_credential
                .env(EnvCredentialProvider::new().with_scope(&scope))
                .well_known(WellKnownCredentialProvider::new().with_scope(&scope));
        }

        if self.config.disable_vm_metadata {
            default_credential = default_credential.no_vm_metadata();
        } else {
            let mut vm_metadata = VmMetadataCredentialProvider::new().with_scope(&scope);
            if let Some(service_account) = self.config.service_account.as_deref() {
                vm_metadata = vm_metadata.with_service_account(service_account);
            }
            default_credential = default_credential.vm_metadata(vm_metadata);
        }

        let mut credential_chain = ProvideCredentialChain::new().push(default_credential.build());

        if let Some(path) = self.config.credential_path.as_deref() {
            credential_chain =
                credential_chain.push_front(FileCredentialProvider::new(path).with_scope(&scope));
        }

        if let Some(content) = self.config.credential.as_deref() {
            let provider = StaticCredentialProvider::from_base64(content).map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "invalid base64 credential")
                    .with_operation("Builder::build")
                    .with_context("service", GCS_SCHEME)
                    .set_source(err)
            })?;
            credential_chain = credential_chain.push_front(provider.with_scope(&scope));
        }

        if let Some(token) = self.config.token.as_deref() {
            credential_chain = credential_chain.push_front(TokenCredentialProvider::new(token));
        }

        if let Some(customized_credential_chain) = self.credential_provider_chain {
            credential_chain = credential_chain.push_front(customized_credential_chain);
        }

        let sign_ctx = ctx;
        let credential_provider = Arc::new(credential_chain);
        let request_signer = Arc::new(RequestSigner::new("storage").with_scope(&scope));

        let authenticated_default_endpoint = endpoint == DEFAULT_GCS_ENDPOINT && !skip_signature;
        let gcs_can_checksum = authenticated_default_endpoint;
        let gcs_can_restore = authenticated_default_endpoint;
        let gcs_can_list_deleted = authenticated_default_endpoint;
        let info = ServiceInfo::new(GCS_SCHEME, &root, bucket);
        let capability = Capability {
            stat: true,
            stat_with_version: true,
            stat_with_if_match: true,
            stat_with_if_none_match: true,
            stat_with_if_version_match: true,
            stat_with_if_version_not_match: true,

            read: true,
            read_with_version: true,
            read_with_suffix: true,

            read_with_if_match: true,
            read_with_if_none_match: true,
            read_with_if_version_match: true,
            read_with_if_version_not_match: true,
            read_with_override_content_disposition: true,
            read_with_override_content_type: true,

            write: true,
            write_can_empty: true,
            write_can_multi: true,
            write_with_cache_control: true,
            write_with_content_type: true,
            write_with_content_encoding: true,
            write_with_user_metadata: true,
            write_with_if_not_exists: true,
            write_with_if_version_match: true,
            write_with_if_version_not_match: true,
            write_with_checksum: gcs_can_checksum,

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
            delete_with_version: true,
            delete_with_if_version_match: true,
            delete_with_if_version_not_match: true,
            delete_max_size: Some(100),

            copy: true,
            copy_with_source_version: true,
            copy_with_if_version_match: true,
            copy_with_if_version_not_match: true,
            copy_can_multi: true,
            // GCS rewrite requires maxBytesRewrittenPerCall to be an
            // integral multiple of 1 MiB if specified.
            //
            // ref: <https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite>
            copy_multi_min_size: Some(GCS_REWRITE_MIN_CHUNK_SIZE),
            copy_multi_max_size: Some(GCS_REWRITE_MAX_CHUNK_SIZE),

            restore: gcs_can_restore,
            restore_with_version: gcs_can_restore,
            restore_with_if_not_exists: gcs_can_restore,

            compose: true,
            compose_with_content_type: true,
            compose_with_content_disposition: true,
            compose_with_content_encoding: true,
            compose_with_cache_control: true,
            compose_with_user_metadata: true,
            compose_with_if_not_exists: true,
            compose_with_if_version_match: true,
            compose_with_source_version: true,

            list: true,
            list_with_limit: true,
            list_with_start_after: true,
            list_with_recursive: true,
            list_with_versions: true,
            list_with_deleted: gcs_can_list_deleted,

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
                sign_ctx,
                credential_provider,
                request_signer,
                signers: HttpTransportCache::new(),
                predefined_acl: self.config.predefined_acl.clone(),
                default_storage_class: self.config.default_storage_class.clone(),
                skip_signature,
            }),
        };

        Ok(backend)
    }
}

impl Builder for GcsBuilder {
    type Config = GcsConfig;

    fn build(self) -> Result<impl Service> {
        self.build_backend()
    }
}

/// GCS storage backend
#[derive(Clone, Debug)]
pub struct GcsBackend {
    pub(crate) core: Arc<GcsCore>,
}

impl Service for GcsBackend {
    type Reader = oio::StreamReader<GcsReader>;
    type Writer = GcsWriters;
    type Lister = GcsLister;
    type Deleter = oio::BatchDeleter<GcsDeleter>;
    type Copier = GcsCopier;
    type Composer = GcsComposer;

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

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        args.check(&self.capability())?;
        self.restore_object(ctx, path, args).await?;
        Ok(RpRestore::new())
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let error_ctx = ErrorContext::new(ServiceOperation("GetObject"))
            .with_caller_condition(args.is_conditional());
        let resp = self.core.gcs_get_object_metadata(ctx, path, &args).await?;

        if !resp.status().is_success() {
            return Err(parse_error(error_ctx, resp));
        }

        let slc = resp.into_body();
        let m = GcsCore::build_metadata_from_object_response(path, slc)?;

        Ok(RpStat::new(m))
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<GcsReader> = {
            Ok(oio::StreamReader::new(GcsReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let conditional = args.if_not_exists()
            || args.if_version_match().is_some()
            || args.if_version_not_match().is_some();
        let output: GcsWriters = if conditional || args.checksum().is_some() {
            TwoWays::Two(GcsResumableWriter::new(
                self.core.clone(),
                ctx.clone(),
                path,
                args,
            )?)
        } else {
            let concurrent = args.concurrent();
            let w = GcsWriter::new(self.core.clone(), ctx.clone(), path, args);
            // Multipart uploads schedule work through the operation executor
            // supplied by the caller.
            let w = oio::MultipartWriter::new(ctx.executor().clone(), w, concurrent);
            TwoWays::One(w)
        };

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::BatchDeleter<GcsDeleter> = {
            Ok(oio::BatchDeleter::new(
                GcsDeleter::new(self.core.clone(), ctx.clone()),
                self.core.capability.delete_max_size,
            ))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        Ok(GcsLister::new(self.core.clone(), ctx.clone(), path, args))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let output: GcsCopier = {
            let copier = GcsCopier::new(self.core.clone(), ctx.clone(), from, to, args);
            Ok(copier)
        }?;

        Ok(output)
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        if args.if_match().is_some()
            || args.if_none_match().is_some()
            || args.if_version_not_match().is_some()
        {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "GCS compose does not support the requested destination option",
            ));
        }
        Ok(GcsComposer::new(self.core.clone(), ctx.clone(), to, args))
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
        // We will not send this request out, just for signing.
        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.gcs_head_object_xml_request(path, v),
            PresignOperation::Read(range, v) => {
                self.core.gcs_get_object_xml_request(path, *range, v)
            }
            PresignOperation::Write(v) if v.checksum().is_some() => Err(Error::new(
                ErrorKind::Unsupported,
                "GCS presigned writes do not support checksum intents",
            )),
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use bytes::Bytes;
    use http::header::AUTHORIZATION;
    use reqsign_core::{FileRead, HttpSend, StaticEnv};

    use super::*;

    const TEST_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_only";
    const IMPERSONATED_ADC_JSON: &[u8] = br#"{
        "type": "impersonated_service_account",
        "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target%40example.com:generateAccessToken",
        "source_credentials": {
            "client_id": "client-id",
            "client_secret": "client-secret",
            "refresh_token": "refresh-token"
        }
    }"#;

    #[derive(Clone, Debug, Default)]
    struct MockFileRead {
        files: HashMap<String, Vec<u8>>,
        paths: Arc<Mutex<Vec<String>>>,
    }

    impl FileRead for MockFileRead {
        async fn file_read(&self, path: &str) -> reqsign_core::Result<Vec<u8>> {
            self.paths.lock().unwrap().push(path.to_string());
            self.files.get(path).cloned().ok_or_else(|| {
                reqsign_core::Error::config_invalid(format!("file not found: {path}"))
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct MockHttpSend {
        requests: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl HttpSend for MockHttpSend {
        async fn http_send(
            &self,
            req: http::Request<Bytes>,
        ) -> reqsign_core::Result<http::Response<Bytes>> {
            let uri = req.uri().to_string();
            let body = String::from_utf8_lossy(req.body()).into_owned();
            self.requests.lock().unwrap().push((uri.clone(), body));

            let response_body = if uri.contains("/computeMetadata/v1/") {
                r#"{"access_token":"vm-token","expires_in":3600}"#
            } else if uri == "https://oauth2.googleapis.com/token" {
                r#"{"access_token":"source-token","expires_in":3600}"#
            } else if uri == "https://sts.googleapis.com/v1/token" {
                r#"{"access_token":"sts-exec-token","expires_in":3600}"#
            } else if uri.contains(":generateAccessToken") {
                r#"{"accessToken":"impersonated-token","expireTime":"2100-01-01T00:00:00Z"}"#
            } else {
                panic!("unexpected request uri: {uri}");
            };

            Ok(http::Response::builder()
                .status(http::StatusCode::OK)
                .body(Bytes::from_static(response_body.as_bytes()))
                .unwrap())
        }
    }

    #[test]
    fn test_capability_supports_versioned_operations() {
        let backend = GcsBuilder::default()
            .bucket("test-bucket")
            .token("test-token".to_string())
            .disable_config_load()
            .disable_vm_metadata()
            .build()
            .expect("GCS backend must build");

        let capability = backend.capability();
        assert!(capability.stat_with_version);
        assert!(capability.read_with_version);
        assert!(capability.copy_with_source_version);
        assert!(capability.delete_with_version);
    }

    #[test]
    fn test_invalid_base64_credential_returns_config_invalid() {
        for invalid in ["not-valid-base64!!!", "//4="] {
            let err = GcsBuilder::default()
                .bucket("example-bucket")
                .credential(invalid)
                .disable_config_load()
                .disable_vm_metadata()
                .build()
                .expect_err("invalid base64 credential must fail build");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        assert!(
            GcsBuilder::default()
                .bucket("example-bucket")
                .credential("")
                .disable_config_load()
                .disable_vm_metadata()
                .build()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_vm_metadata_wires_scope_and_service_account_without_static_env() {
        let backend = GcsBuilder::default()
            .bucket("example-bucket")
            .scope(TEST_SCOPE)
            .service_account("custom-sa@example.iam.gserviceaccount.com")
            .disable_config_load()
            .build_backend()
            .unwrap();

        assert_eq!(
            backend.core.sign_ctx.env_var("GOOGLE_SCOPE"),
            std::env::var("GOOGLE_SCOPE").ok()
        );

        let http = MockHttpSend::default();
        let ctx = opendal_core::raw::reqsign_context()
            .with_env(StaticEnv::default())
            .with_http_send(http.clone());
        let (mut parts, ()) =
            http::Request::get("https://storage.googleapis.com/example-bucket/obj")
                .body(())
                .unwrap()
                .into_parts();

        backend
            .core
            .signer_with_context(ctx)
            .sign(&mut parts, None)
            .await
            .unwrap();

        assert_eq!(
            parts
                .headers
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok()),
            Some("Bearer vm-token")
        );
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].0,
            format!(
                "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/custom-sa@example.iam.gserviceaccount.com/token?scopes={TEST_SCOPE}"
            )
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_env_and_well_known_providers_wire_scope_directly() {
        for (env_key, env_value, expected_path) in [
            (
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/adc/env.json",
                "/adc/env.json",
            ),
            (
                "HOME",
                "/adc-home",
                "/adc-home/.config/gcloud/application_default_credentials.json",
            ),
        ] {
            let backend = GcsBuilder::default()
                .bucket("example-bucket")
                .scope(TEST_SCOPE)
                .disable_vm_metadata()
                .build_backend()
                .unwrap();

            let file_read = MockFileRead {
                files: HashMap::from([(expected_path.to_string(), IMPERSONATED_ADC_JSON.to_vec())]),
                paths: Arc::default(),
            };
            let http = MockHttpSend::default();
            let ctx = opendal_core::raw::reqsign_context()
                .with_env(StaticEnv {
                    home_dir: None,
                    envs: HashMap::from([(env_key.to_string(), env_value.to_string())]),
                })
                .with_file_read(file_read.clone())
                .with_http_send(http.clone());
            let (mut parts, ()) =
                http::Request::get("https://storage.googleapis.com/example-bucket/obj")
                    .body(())
                    .unwrap()
                    .into_parts();

            backend
                .core
                .signer_with_context(ctx)
                .sign(&mut parts, None)
                .await
                .unwrap();

            assert_eq!(
                parts
                    .headers
                    .get(AUTHORIZATION)
                    .and_then(|v| v.to_str().ok()),
                Some("Bearer impersonated-token")
            );
            assert_eq!(file_read.paths.lock().unwrap().as_slice(), &[expected_path]);
            let requests = http.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            let body: serde_json::Value = serde_json::from_str(&requests[1].1).unwrap();
            assert_eq!(body["scope"], serde_json::json!([TEST_SCOPE]));
        }
    }

    #[tokio::test]
    async fn test_disable_config_load_and_disable_vm_metadata_skip_default_providers() {
        let backend = GcsBuilder::default()
            .bucket("example-bucket")
            .scope(TEST_SCOPE)
            .service_account("custom-sa@example.iam.gserviceaccount.com")
            .disable_config_load()
            .disable_vm_metadata()
            .build_backend()
            .unwrap();

        let file_read = MockFileRead {
            files: HashMap::from([
                ("/adc/env.json".to_string(), IMPERSONATED_ADC_JSON.to_vec()),
                (
                    "/adc-home/.config/gcloud/application_default_credentials.json".to_string(),
                    IMPERSONATED_ADC_JSON.to_vec(),
                ),
            ]),
            paths: Arc::default(),
        };
        let http = MockHttpSend::default();
        let ctx = opendal_core::raw::reqsign_context()
            .with_env(StaticEnv {
                home_dir: None,
                envs: HashMap::from([
                    (
                        "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
                        "/adc/env.json".to_string(),
                    ),
                    ("HOME".to_string(), "/adc-home".to_string()),
                ]),
            })
            .with_file_read(file_read.clone())
            .with_http_send(http.clone());
        let (mut parts, ()) =
            http::Request::get("https://storage.googleapis.com/example-bucket/obj")
                .body(())
                .unwrap()
                .into_parts();

        let err = backend
            .core
            .signer_with_context(ctx)
            .sign(&mut parts, None)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), reqsign_core::ErrorKind::CredentialInvalid);
        assert!(file_read.paths.lock().unwrap().is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[cfg(unix)]
    struct TempScript {
        dir: std::path::PathBuf,
        script_path: std::path::PathBuf,
        marker_path: std::path::PathBuf,
    }

    #[cfg(unix)]
    impl TempScript {
        fn new(body: &str) -> Self {
            use std::os::unix::fs::PermissionsExt;
            use std::sync::atomic::{AtomicU64, Ordering};

            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "opendal-gcs-cmd-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let marker_path = dir.join("executed");
            let script_path = dir.join("cred.sh");
            let script = format!("#!/bin/sh\ntouch \"{}\"\n{}\n", marker_path.display(), body);
            std::fs::write(&script_path, script).unwrap();
            std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755)).unwrap();
            Self {
                dir,
                script_path,
                marker_path,
            }
        }
    }

    #[cfg(unix)]
    impl Drop for TempScript {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[cfg(unix)]
    fn executable_external_account_json(script_path: &std::path::Path) -> Vec<u8> {
        serde_json::json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
            "subject_token_type": "urn:ietf:params:oauth:token-type:id_token",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": {
                "executable": {
                    "command": script_path.to_str().unwrap(),
                    "timeout_millis": 5000,
                }
            }
        })
        .to_string()
        .into_bytes()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_executable_external_account_runs_via_wired_sign_ctx() {
        let script = TempScript::new(
            r#"printf '%s' '{"version":1,"success":true,"token_type":"urn:ietf:params:oauth:token-type:id_token","id_token":"exec-subject-token","expiration_time":4102444800}'"#,
        );
        let backend = GcsBuilder::default()
            .bucket("example-bucket")
            .disable_vm_metadata()
            .build_backend()
            .unwrap();

        let file_read = MockFileRead {
            files: HashMap::from([(
                "/adc/exec.json".to_string(),
                executable_external_account_json(&script.script_path),
            )]),
            paths: Arc::default(),
        };
        let http = MockHttpSend::default();
        let ctx = backend
            .core
            .sign_ctx
            .clone()
            .with_env(StaticEnv {
                home_dir: None,
                envs: HashMap::from([
                    (
                        "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
                        "/adc/exec.json".to_string(),
                    ),
                    (
                        "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES".to_string(),
                        "1".to_string(),
                    ),
                ]),
            })
            .with_file_read(file_read)
            .with_http_send(http.clone());
        let (mut parts, ()) =
            http::Request::get("https://storage.googleapis.com/example-bucket/obj")
                .body(())
                .unwrap()
                .into_parts();

        backend
            .core
            .signer_with_context(ctx)
            .sign(&mut parts, None)
            .await
            .unwrap();

        assert_eq!(
            parts
                .headers
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok()),
            Some("Bearer sts-exec-token")
        );
        assert!(script.marker_path.exists());
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "https://sts.googleapis.com/v1/token");
        assert!(
            requests[0].1.contains("exec-subject-token"),
            "{}",
            requests[0].1
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_disable_config_load_skips_ambient_executable_external_account() {
        let script = TempScript::new(
            r#"printf '%s' '{"version":1,"success":true,"token_type":"urn:ietf:params:oauth:token-type:id_token","id_token":"exec-subject-token","expiration_time":4102444800}'"#,
        );
        let backend = GcsBuilder::default()
            .bucket("example-bucket")
            .disable_config_load()
            .disable_vm_metadata()
            .build_backend()
            .unwrap();

        let file_read = MockFileRead {
            files: HashMap::from([(
                "/adc/exec.json".to_string(),
                executable_external_account_json(&script.script_path),
            )]),
            paths: Arc::default(),
        };
        let http = MockHttpSend::default();
        let ctx = backend
            .core
            .sign_ctx
            .clone()
            .with_env(StaticEnv {
                home_dir: None,
                envs: HashMap::from([
                    (
                        "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
                        "/adc/exec.json".to_string(),
                    ),
                    (
                        "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES".to_string(),
                        "1".to_string(),
                    ),
                ]),
            })
            .with_file_read(file_read.clone())
            .with_http_send(http.clone());
        let (mut parts, ()) =
            http::Request::get("https://storage.googleapis.com/example-bucket/obj")
                .body(())
                .unwrap()
                .into_parts();

        let err = backend
            .core
            .signer_with_context(ctx)
            .sign(&mut parts, None)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), reqsign_core::ErrorKind::CredentialInvalid);
        assert!(file_read.paths.lock().unwrap().is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
        assert!(!script.marker_path.exists());
    }
}
