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
use reqsign_core::{Context, OsEnv, ProvideCredential, ProvideCredentialChain, Signer};
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_google::{
    Credential, DefaultCredentialProvider, EnvCredentialProvider, FileCredentialProvider,
    RequestSigner, StaticCredentialProvider, TokenCredentialProvider, VmMetadataCredentialProvider,
    WellKnownCredentialProvider,
};
use tonic::transport::{ClientTlsConfig, Endpoint};

use opendal_core::raw::*;
use opendal_core::*;

use crate::GCS_GRPC_SCHEME;
use crate::config::GcsGrpcConfig;
use crate::copier::new_gcs_grpc_copier;
use crate::core::{ErrorContext, GcsGrpcCore, parse_generation, parse_object, parse_status};
use crate::deleter::GcsGrpcDeleter;
use crate::generated::google::storage::v2::GetObjectRequest;
use crate::lister::GcsGrpcLister;
use crate::reader::GcsGrpcReader;
use crate::writer::GcsGrpcWriter;

const DEFAULT_GCS_GRPC_ENDPOINT: &str = "https://storage.googleapis.com";
const DEFAULT_GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// Builder for the Google Cloud Storage gRPC service.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GcsGrpcBuilder {
    pub(super) config: GcsGrpcConfig,
    pub(super) credential_provider_chain: Option<ProvideCredentialChain<Credential>>,
}

impl Debug for GcsGrpcBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsGrpcBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GcsGrpcBuilder {
    /// Set the working directory root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = (!root.is_empty()).then(|| root.to_string());
        self
    }

    /// Set the bucket name.
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();
        self
    }

    /// Set the gRPC endpoint.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = (!endpoint.is_empty()).then(|| endpoint.to_string());
        self
    }

    /// Set the Google OAuth 2.0 scope.
    pub fn scope(mut self, scope: &str) -> Self {
        self.config.scope = (!scope.is_empty()).then(|| scope.to_string());
        self
    }

    /// Set the service account used by the GCE metadata server.
    pub fn service_account(mut self, service_account: &str) -> Self {
        self.config.service_account =
            (!service_account.is_empty()).then(|| service_account.to_string());
        self
    }

    /// Set a base64-encoded service account credential.
    pub fn credential(mut self, credential: &str) -> Self {
        self.config.credential = (!credential.is_empty()).then(|| credential.to_string());
        self
    }

    /// Set the path to a service account credential file.
    pub fn credential_path(mut self, path: &str) -> Self {
        self.config.credential_path = (!path.is_empty()).then(|| path.to_string());
        self
    }

    /// Set a custom Google credential provider.
    ///
    /// Implement the provider against [`opendal_core::reqsign_core`] and
    /// [`crate::reqsign_google`] so that its types match the ones this service is
    /// built with.
    pub fn credential_provider(
        mut self,
        provider: impl ProvideCredential<Credential = Credential> + 'static,
    ) -> Self {
        self.credential_provider_chain = Some(
            self.credential_provider_chain
                .unwrap_or_default()
                .push_front(provider),
        );
        self
    }

    /// Set a custom Google credential provider chain.
    ///
    /// Build the chain from [`opendal_core::reqsign_core`] and
    /// [`crate::reqsign_google`] so that its types match the ones this service is
    /// built with.
    pub fn credential_provider_chain(mut self, chain: ProvideCredentialChain<Credential>) -> Self {
        self.credential_provider_chain = Some(chain);
        self
    }

    /// Set an OAuth 2.0 access token.
    pub fn token(mut self, token: String) -> Self {
        self.config.token = Some(token);
        self
    }

    /// Disable the GCE metadata credential provider.
    pub fn disable_vm_metadata(mut self) -> Self {
        self.config.disable_vm_metadata = true;
        self
    }

    /// Disable environment and well-known credential loading.
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
        self
    }

    /// Send requests without authentication.
    pub fn skip_signature(mut self) -> Self {
        self.config.skip_signature = true;
        self
    }
}

impl GcsGrpcBuilder {
    fn build_backend(self) -> Result<GcsGrpcBackend> {
        debug!("backend build started: {self:?}");
        let root = normalize_root(&self.config.root.unwrap_or_default());
        if self.config.bucket.is_empty() {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_operation("Builder::build")
                    .with_context("service", GCS_GRPC_SCHEME),
            );
        }

        let endpoint = self
            .config
            .endpoint
            .clone()
            .unwrap_or_else(|| DEFAULT_GCS_GRPC_ENDPOINT.to_string());
        let channel_endpoint = build_endpoint(&endpoint)?;
        let scope = self
            .config
            .scope
            .clone()
            .unwrap_or_else(|| DEFAULT_GCS_SCOPE.to_string());

        let ctx = Context::new().with_file_read(TokioFileRead).with_env(OsEnv);

        let mut default_credential = DefaultCredentialProvider::builder();
        if self.config.disable_config_load {
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
                    .with_context("service", GCS_GRPC_SCHEME)
                    .set_source(err)
            })?;
            credential_chain = credential_chain.push_front(provider.with_scope(&scope));
        }
        if let Some(token) = self.config.token.as_deref() {
            credential_chain = credential_chain.push_front(TokenCredentialProvider::new(token));
        }
        if let Some(custom) = self.credential_provider_chain {
            credential_chain = credential_chain.push_front(custom);
        }

        let signer = Signer::new(
            ctx.clone(),
            credential_chain,
            RequestSigner::new("storage").with_scope(&scope),
        );
        let capability = Capability {
            stat: true,
            stat_with_version: true,
            read: true,
            read_with_version: true,
            read_with_suffix: true,
            write: true,
            write_can_empty: true,
            write_can_multi: true,
            write_with_content_type: true,
            write_with_content_disposition: true,
            write_with_content_encoding: true,
            write_with_cache_control: true,
            write_with_if_not_exists: true,
            write_with_user_metadata: true,
            delete: true,
            delete_with_version: true,
            copy: true,
            copy_with_if_not_exists: true,
            copy_with_source_version: true,
            list: true,
            list_with_limit: true,
            list_with_start_after: true,
            list_with_recursive: true,
            shared: true,
            ..Default::default()
        };
        let bucket = self.config.bucket;
        Ok(GcsGrpcBackend {
            core: Arc::new(GcsGrpcCore {
                info: ServiceInfo::new(GCS_GRPC_SCHEME, &root, &bucket),
                capability,
                endpoint,
                bucket,
                root,
                channel_endpoint,
                channel: Default::default(),
                signer,
                sign_ctx: ctx,
                skip_signature: self.config.skip_signature,
            }),
        })
    }
}

impl Builder for GcsGrpcBuilder {
    type Config = GcsGrpcConfig;

    fn build(self) -> Result<impl Service> {
        self.build_backend()
    }
}

fn build_endpoint(endpoint: &str) -> Result<Endpoint> {
    let mut endpoint_builder = Endpoint::from_shared(endpoint.to_string()).map_err(|err| {
        Error::new(ErrorKind::ConfigInvalid, "invalid GCS gRPC endpoint").set_source(err)
    })?;
    match endpoint_builder.uri().scheme() {
        Some(scheme) if scheme == &http::uri::Scheme::HTTPS => {
            endpoint_builder = endpoint_builder
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .map_err(|err| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "invalid GCS gRPC TLS configuration",
                    )
                    .set_source(err)
                })?;
        }
        Some(scheme) if scheme == &http::uri::Scheme::HTTP => {}
        _ => {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "GCS gRPC endpoint must use http or https",
            ));
        }
    }
    Ok(endpoint_builder)
}

/// Google Cloud Storage gRPC backend.
#[derive(Clone, Debug)]
pub struct GcsGrpcBackend {
    core: Arc<GcsGrpcCore>,
}

impl Service for GcsGrpcBackend {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
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
        if path == "/" {
            return Ok(RpStat::new(MetadataBuilder::dir().build()));
        }
        let object = self.core.object_name(path);
        let request = GetObjectRequest {
            bucket: self.core.bucket_resource(),
            object,
            generation: parse_generation(args.version())?,
        };
        let request = self
            .core
            .request(ctx, request, &[("bucket", &self.core.bucket_resource())])
            .await?;
        let response = self
            .core
            .client()
            .get_object(request)
            .await
            .map_err(|status| {
                parse_status(ErrorContext::new(ServiceOperation("GetObject")), status)
            })?;
        Ok(RpStat::new(parse_object(response.get_ref())))
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(Box::new(GcsGrpcReader::new(
            self.core.clone(),
            ctx.clone(),
            path,
            args,
        )))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(Box::new(GcsGrpcWriter::new(
            self.core.clone(),
            ctx.clone(),
            path,
            args,
        )))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(Box::new(GcsGrpcDeleter::new(
            self.core.clone(),
            ctx.clone(),
        )))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        Ok(Box::new(GcsGrpcLister::new(
            self.core.clone(),
            ctx.clone(),
            path,
            args,
        )))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        Ok(Box::new(new_gcs_grpc_copier(
            self.core.clone(),
            ctx.clone(),
            from,
            to,
            args,
        )))
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
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use bytes::Bytes;
    use futures::executor::block_on;
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
    fn builder_does_not_require_a_tokio_runtime() {
        GcsGrpcBuilder::default()
            .bucket("example-bucket")
            .skip_signature()
            .build()
            .unwrap();
    }

    #[test]
    fn endpoint_accepts_http_schemes_case_insensitively() {
        assert!(build_endpoint("HTTPS://storage.googleapis.com").is_ok());
        assert!(build_endpoint("ftp://storage.googleapis.com").is_err());
    }

    #[test]
    fn invalid_base64_credential_returns_config_invalid() {
        for invalid in ["not-valid-base64!!!", "//4="] {
            let err = GcsGrpcBuilder::default()
                .bucket("example-bucket")
                .credential(invalid)
                .disable_config_load()
                .disable_vm_metadata()
                .build()
                .expect_err("invalid base64 credential must fail build");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        assert!(
            GcsGrpcBuilder::default()
                .bucket("example-bucket")
                .credential("")
                .disable_config_load()
                .disable_vm_metadata()
                .build()
                .is_ok()
        );
    }

    #[test]
    fn vm_metadata_wires_scope_and_service_account_without_static_env() {
        let backend = GcsGrpcBuilder::default()
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
        let ctx = Context::new()
            .with_env(StaticEnv::default())
            .with_http_send(http.clone());
        let (mut parts, ()) = http::Request::get("https://storage.googleapis.com")
            .body(())
            .unwrap()
            .into_parts();

        block_on(
            backend
                .core
                .signer
                .clone()
                .with_context(ctx)
                .sign(&mut parts, None),
        )
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

    #[test]
    fn env_and_well_known_providers_wire_scope_directly() {
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
            let backend = GcsGrpcBuilder::default()
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
            let ctx = Context::new()
                .with_env(StaticEnv {
                    home_dir: None,
                    envs: HashMap::from([(env_key.to_string(), env_value.to_string())]),
                })
                .with_file_read(file_read.clone())
                .with_http_send(http.clone());
            let (mut parts, ()) = http::Request::get("https://storage.googleapis.com")
                .body(())
                .unwrap()
                .into_parts();

            block_on(
                backend
                    .core
                    .signer
                    .clone()
                    .with_context(ctx)
                    .sign(&mut parts, None),
            )
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

    #[test]
    fn disable_config_load_and_disable_vm_metadata_skip_default_providers() {
        let backend = GcsGrpcBuilder::default()
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
        let ctx = Context::new()
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
        let (mut parts, ()) = http::Request::get("https://storage.googleapis.com")
            .body(())
            .unwrap()
            .into_parts();

        let err = block_on(
            backend
                .core
                .signer
                .clone()
                .with_context(ctx)
                .sign(&mut parts, None),
        )
        .unwrap_err();
        assert_eq!(err.kind(), reqsign_core::ErrorKind::CredentialInvalid);
        assert!(file_read.paths.lock().unwrap().is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
    }
}
