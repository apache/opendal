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

use http::StatusCode;
use log::debug;
use reqsign_azure_storage::ClientSecretCredentialProvider;
use reqsign_azure_storage::Credential;
use reqsign_azure_storage::DefaultCredentialProvider;
use reqsign_azure_storage::RequestSigner;
use reqsign_azure_storage::StaticCredentialProvider;
#[cfg(not(target_arch = "wasm32"))]
use reqsign_command_execute_tokio::TokioCommandExecute;
use reqsign_core::Context;
use reqsign_core::OsEnv;
use reqsign_core::ProvideCredentialChain;
use reqsign_file_read_tokio::TokioFileRead;

use super::AZFILE_SCHEME;
use super::config::AzfileConfig;
use super::core::X_MS_META_PREFIX;
use super::core::parse_error;
use super::core::{AzfileCore, ErrorContext, normalize_sas_token};
use super::deleter::AzfileDeleter;
use super::lister::AzfileLister;
use super::reader::*;
use super::writer::AzfileWriter;
use super::writer::AzfileWriters;
use opendal_core::raw::*;
use opendal_core::*;
use opendal_service_azure_common::{
    AzureStorageConfig as AzureConnectionConfig, AzureStorageService,
    azure_account_name_from_endpoint, azure_config_from_connection_string,
};

impl From<AzureConnectionConfig> for AzfileConfig {
    fn from(config: AzureConnectionConfig) -> Self {
        AzfileConfig {
            account_name: config.account_name,
            account_key: config.account_key,
            sas_token: config.sas_token,
            endpoint: config.endpoint,
            client_id: config.client_id,
            client_secret: config.client_secret,
            tenant_id: config.tenant_id,
            authority_host: config.authority_host,
            ..Default::default()
        }
    }
}

/// Azure File services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct AzfileBuilder {
    pub(super) config: AzfileConfig,
    pub(super) credential_providers: Option<ProvideCredentialChain<Credential>>,
}

impl Debug for AzfileBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzfileBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AzfileBuilder {
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
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set account_name of this backend.
    ///
    /// - If account_name is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn account_name(mut self, account_name: &str) -> Self {
        if !account_name.is_empty() {
            self.config.account_name = Some(account_name.to_string());
        }

        self
    }

    /// Set account_key of this backend.
    ///
    /// - If account_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn account_key(mut self, account_key: &str) -> Self {
        if !account_key.is_empty() {
            self.config.account_key = Some(account_key.to_string());
        }

        self
    }

    /// Set file share name of this backend.
    ///
    /// # Notes
    /// You can find more about from: <https://learn.microsoft.com/en-us/rest/api/storageservices/operations-on-shares--file-service>
    pub fn share_name(mut self, share_name: &str) -> Self {
        if !share_name.is_empty() {
            self.config.share_name = share_name.to_string();
        }

        self
    }

    /// Set sas_token of this backend.
    ///
    /// - If sas_token is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn sas_token(mut self, sas_token: &str) -> Self {
        if !sas_token.is_empty() {
            self.config.sas_token = Some(sas_token.to_string());
        }

        self
    }

    /// Set client_id of this backend.
    ///
    /// - If client_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - Required for client_credentials authentication.
    pub fn client_id(mut self, client_id: &str) -> Self {
        if !client_id.is_empty() {
            self.config.client_id = Some(client_id.to_string());
        }

        self
    }

    /// Set client_secret of this backend.
    ///
    /// - If client_secret is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - Required for client_credentials authentication.
    pub fn client_secret(mut self, client_secret: &str) -> Self {
        if !client_secret.is_empty() {
            self.config.client_secret = Some(client_secret.to_string());
        }

        self
    }

    /// Set tenant_id of this backend.
    ///
    /// - If tenant_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - Required for client_credentials authentication.
    pub fn tenant_id(mut self, tenant_id: &str) -> Self {
        if !tenant_id.is_empty() {
            self.config.tenant_id = Some(tenant_id.to_string());
        }

        self
    }

    /// Set authority_host of this backend.
    ///
    /// - If authority_host is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - Defaults to `https://login.microsoftonline.com`.
    pub fn authority_host(mut self, authority_host: &str) -> Self {
        if !authority_host.is_empty() {
            self.config.authority_host = Some(authority_host.to_string());
        }

        self
    }

    /// Replace the credential providers with a custom chain.
    ///
    /// Build the chain from [`opendal_core::reqsign_core`] and
    /// [`crate::reqsign_azure_storage`] so that its types match the ones this
    /// service is built with.
    pub fn credential_provider_chain(mut self, chain: ProvideCredentialChain<Credential>) -> Self {
        self.credential_providers = Some(chain);
        self
    }

    /// Skip signature will skip loading credentials and signing requests.
    pub fn skip_signature(mut self) -> Self {
        self.config.skip_signature = true;
        self
    }

    /// Create a new `AfileBuilder` instance from an [Azure Storage connection string][1].
    ///
    /// [1]: https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
    ///
    /// # Example
    /// ```
    /// use opendal_core::Builder;
    /// use opendal_service_azfile::Azfile;
    ///
    /// let conn_str = "AccountName=example;DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net";
    ///
    /// let mut config = Azfile::from_connection_string(&conn_str)
    ///     .unwrap()
    ///     // Add additional configuration if needed
    ///     .share_name("myShare")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn from_connection_string(conn_str: &str) -> Result<Self> {
        let config = azure_config_from_connection_string(conn_str, AzureStorageService::File)?;

        Ok(AzfileConfig::from(config).into_builder())
    }
}

impl Builder for AzfileBuilder {
    type Config = AzfileConfig;

    fn build(self) -> Result<impl Service> {
        self.build_backend()
    }
}

impl AzfileBuilder {
    fn build_backend(self) -> Result<AzfileBackend> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.trim_end_matches('/').to_string()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", AZFILE_SCHEME)),
        }?;
        debug!("backend use endpoint {}", endpoint);

        let account_name = self
            .config
            .account_name
            .clone()
            .or_else(|| azure_account_name_from_endpoint(endpoint.as_str()));

        let ctx = Context::new().with_file_read(TokioFileRead).with_env(OsEnv);
        #[cfg(not(target_arch = "wasm32"))]
        let ctx = ctx.with_command_execute(TokioCommandExecute);

        let tenant_id = self.config.tenant_id.as_deref().filter(|v| !v.is_empty());
        let client_id = self.config.client_id.as_deref().filter(|v| !v.is_empty());
        let client_secret_value = self
            .config
            .client_secret
            .as_deref()
            .filter(|v| !v.is_empty());
        let authority_host = self
            .config
            .authority_host
            .as_deref()
            .filter(|v| !v.is_empty());

        let mut client_secret = ClientSecretCredentialProvider::new();
        if let Some(v) = tenant_id {
            client_secret = client_secret.with_tenant_id(v);
        }
        if let Some(v) = client_id {
            client_secret = client_secret.with_client_id(v);
        }
        if let Some(v) = client_secret_value {
            client_secret = client_secret.with_client_secret(v);
        }
        if let Some(v) = authority_host {
            client_secret = client_secret.with_authority_host(v);
        }

        let has_explicit_client_secret =
            tenant_id.is_some() && client_id.is_some() && client_secret_value.is_some();
        let mut credential_providers = if has_explicit_client_secret {
            ProvideCredentialChain::new().push(client_secret).push(
                DefaultCredentialProvider::builder()
                    .no_client_secret()
                    .build(),
            )
        } else {
            ProvideCredentialChain::new().push(
                DefaultCredentialProvider::builder()
                    .client_secret(client_secret)
                    .build(),
            )
        };

        if let (Some(account_name), Some(account_key)) = (
            account_name.as_deref().filter(|v| !v.is_empty()),
            self.config.account_key.as_deref().filter(|v| !v.is_empty()),
        ) {
            credential_providers = credential_providers.push_front(
                StaticCredentialProvider::new_shared_key(account_name, account_key),
            );
        }

        let sas_token = normalize_sas_token(self.config.sas_token.as_deref());
        if let Some(sas_token) = sas_token {
            credential_providers =
                credential_providers.push_front(StaticCredentialProvider::new_sas_token(sas_token));
        }

        if let Some(customized_credential_chain) = self.credential_providers {
            credential_providers = customized_credential_chain;
        }

        let sign_ctx = ctx;
        let credential_provider = Arc::new(credential_providers);
        let request_signer = Arc::new(RequestSigner::new());

        let info = ServiceInfo::new(AZFILE_SCHEME, &root, "");
        let capability = Capability {
            stat: true,

            read: true,

            write: true,
            write_with_user_metadata: true,
            // Azure Files limits each Put Range update to 4 MiB.
            // ref: <https://learn.microsoft.com/en-us/rest/api/storageservices/put-range>
            write_total_max_size: Some(4 * 1024 * 1024),

            create_dir: true,
            delete: true,
            rename: true,

            list: true,

            shared: true,

            ..Default::default()
        };

        Ok(AzfileBackend {
            core: Arc::new(AzfileCore {
                info,
                capability,
                root,
                endpoint,
                sign_ctx,
                credential_provider,
                request_signer,
                signers: HttpTransportCache::new(),
                share_name: self.config.share_name.clone(),
                skip_signature: self.config.skip_signature,
            }),
        })
    }
}

/// Backend for azfile services.
#[derive(Debug, Clone)]
pub struct AzfileBackend {
    pub(crate) core: Arc<AzfileCore>,
}

impl Service for AzfileBackend {
    type Reader = oio::StreamReader<AzfileReader>;
    type Writer = AzfileWriters;
    type Lister = oio::PageLister<AzfileLister>;
    type Deleter = oio::OneShotDeleter<AzfileDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.core.ensure_parent_dir_exists(ctx, path).await?;
        let resp = self.core.azfile_create_dir(ctx, path).await?;
        let status = resp.status();

        match status {
            StatusCode::CREATED => Ok(RpCreateDir::default()),
            _ => {
                // we cannot just check status code because 409 Conflict has two meaning:
                // 1. If a directory by the same name is being deleted when Create Directory is called, the server returns status code 409 (Conflict)
                // 2. If a directory or file with the same name already exists, the operation fails with status code 409 (Conflict).
                // but we just need case 2 (already exists)
                // ref: https://learn.microsoft.com/en-us/rest/api/storageservices/create-directory
                if resp
                    .headers()
                    .get("x-ms-error-code")
                    .map(|value| value.to_str().unwrap_or(""))
                    .unwrap_or_else(|| "")
                    == "ResourceAlreadyExists"
                {
                    Ok(RpCreateDir::default())
                } else {
                    Err(parse_error(
                        ErrorContext::new(ServiceOperation("CreateDirectory")),
                        resp,
                    ))
                }
            }
        }
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let resp = if path.ends_with('/') {
            self.core.azfile_get_directory_properties(ctx, path).await?
        } else {
            self.core.azfile_get_file_properties(ctx, path).await?
        };

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?.into_builder();
                let user_meta = parse_prefixed_headers(headers, X_MS_META_PREFIX);
                if !user_meta.is_empty() {
                    meta.user_metadata(user_meta);
                }
                Ok(RpStat::new(meta.build()))
            }
            _ => Err(parse_error(
                ErrorContext::new(if path.ends_with('/') {
                    ServiceOperation("GetDirectoryProperties")
                } else {
                    ServiceOperation("GetFileProperties")
                }),
                resp,
            )),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<AzfileReader> = {
            Ok(oio::StreamReader::new(AzfileReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: AzfileWriters = {
            let w = AzfileWriter::new(
                self.core.clone(),
                ctx.clone(),
                args.clone(),
                path.to_string(),
            );
            let w = if args.append() {
                AzfileWriters::Two(oio::AppendWriter::new(w))
            } else {
                AzfileWriters::One(oio::OneShotWriter::new(w))
            };
            Ok(w)
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<AzfileDeleter> = {
            Ok(oio::OneShotDeleter::new(AzfileDeleter::new(
                self.core.clone(),
                ctx.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: oio::PageLister<AzfileLister> = {
            let l = AzfileLister::new(
                self.core.clone(),
                ctx.clone(),
                path.to_string(),
                args.limit(),
            );

            Ok(oio::PageLister::new(l))
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        self.core.ensure_parent_dir_exists(ctx, to).await?;
        let resp = self.core.azfile_rename(ctx, from, to).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Rename")),
                resp,
            )),
        }
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
    use super::*;

    #[derive(Clone, Default, Debug)]
    struct MockTokenHttpSend {
        captured: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    }

    impl reqsign_core::HttpSend for MockTokenHttpSend {
        async fn http_send(
            &self,
            req: http::Request<bytes::Bytes>,
        ) -> reqsign_core::Result<http::Response<bytes::Bytes>> {
            let uri = req.uri().to_string();
            let body = String::from_utf8_lossy(req.body()).into_owned();
            self.captured.lock().unwrap().push((uri, body));
            Ok(http::Response::builder()
                .status(StatusCode::OK)
                .body(bytes::Bytes::from_static(
                    br#"{"access_token":"azfile-bearer-token","expires_in":3600}"#,
                ))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn build_with_sas_token_without_account_name_signs_requests() {
        let backend = AzfileBuilder::default()
            .endpoint("http://127.0.0.1:10002")
            .share_name("share")
            .sas_token("?sv=2022-11-02&sig=abc")
            .build_backend()
            .unwrap();

        let signer = backend
            .core
            .signer_with_context(Context::new().with_env(reqsign_core::StaticEnv::default()));
        let (mut parts, _) = http::Request::get("http://127.0.0.1:10002/share/file.txt")
            .body(())
            .unwrap()
            .into_parts();
        signer.sign(&mut parts, None).await.unwrap();

        assert_eq!(
            parts.uri.to_string(),
            "http://127.0.0.1:10002/share/file.txt?sv=2022-11-02&sig=abc"
        );
    }

    #[tokio::test]
    async fn skip_signature_builds_without_account_name_and_omits_auth() {
        let backend = AzfileBuilder::default()
            .endpoint("http://127.0.0.1:10002")
            .share_name("share")
            .sas_token("sv=2022-11-02&sig=abc")
            .skip_signature()
            .build_backend()
            .unwrap();

        assert!(backend.core.skip_signature);

        let req = http::Request::get("http://127.0.0.1:10002/share/file.txt")
            .body(Buffer::new())
            .unwrap();
        let signed = backend
            .core
            .sign(&OperationContext::new(), req)
            .await
            .unwrap();

        assert_eq!(signed.headers()["x-ms-version"], "2022-11-02");
        assert!(signed.headers().get(http::header::AUTHORIZATION).is_none());
        assert_eq!(
            signed.uri().to_string(),
            "http://127.0.0.1:10002/share/file.txt"
        );
    }

    #[tokio::test]
    async fn credential_provider_chain_replaces_default_and_static_credentials() {
        let chain = ProvideCredentialChain::new().push(StaticCredentialProvider::new_sas_token(
            "sv=custom&sig=chain",
        ));
        let backend = AzfileBuilder::default()
            .endpoint("http://127.0.0.1:10002")
            .share_name("share")
            .sas_token("sv=ignored&sig=ignored")
            .credential_provider_chain(chain)
            .build_backend()
            .unwrap();

        let signer = backend
            .core
            .signer_with_context(Context::new().with_env(reqsign_core::StaticEnv::default()));
        let (mut parts, _) = http::Request::get("http://127.0.0.1:10002/share/file.txt")
            .body(())
            .unwrap()
            .into_parts();
        signer.sign(&mut parts, None).await.unwrap();

        assert_eq!(
            parts.uri.to_string(),
            "http://127.0.0.1:10002/share/file.txt?sv=custom&sig=chain"
        );
    }

    #[tokio::test]
    async fn client_secret_credentials_use_configured_authority_and_tenant() {
        let backend = AzfileBuilder::default()
            .endpoint("http://127.0.0.1:10002")
            .share_name("share")
            .client_id("client-123")
            .client_secret("secret-123")
            .tenant_id("tenant-123")
            .authority_host("https://custom.authority.example")
            .build_backend()
            .unwrap();

        let http = MockTokenHttpSend::default();
        let ctx = Context::new()
            .with_env(reqsign_core::StaticEnv::default())
            .with_http_send(http.clone());
        let signer = backend.core.signer_with_context(ctx);

        let (mut parts, _) = http::Request::get("http://127.0.0.1:10002/share/file.txt")
            .body(())
            .unwrap()
            .into_parts();
        signer.sign(&mut parts, None).await.unwrap();

        assert_eq!(
            parts.headers[http::header::AUTHORIZATION],
            "Bearer azfile-bearer-token"
        );
        let captured = http.captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].0,
            "https://custom.authority.example/tenant-123/oauth2/v2.0/token"
        );
        assert!(captured[0].1.contains("client_id=client-123"));
        assert!(captured[0].1.contains("client_secret=secret-123"));
        assert!(captured[0].1.contains("grant_type=client_credentials"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Clone, Default, Debug)]
    struct MockAzCli {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl reqsign_core::CommandExecute for MockAzCli {
        async fn command_execute(
            &self,
            program: &str,
            args: &[&str],
        ) -> reqsign_core::Result<reqsign_core::CommandOutput> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(program, "az");
            assert_eq!(
                args,
                [
                    "account",
                    "get-access-token",
                    "--resource",
                    "https://storage.azure.com/",
                    "--output",
                    "json",
                ]
            );
            Ok(reqsign_core::CommandOutput {
                status: 0,
                stdout: br#"{"accessToken":"az-cli-token","expires_on":4102444800,"tokenType":"Bearer"}"#
                    .to_vec(),
                stderr: Vec::new(),
            })
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn sign_ctx_wires_tokio_command_execute() {
        let backend = AzfileBuilder::default()
            .endpoint("http://127.0.0.1:10002")
            .share_name("share")
            .build_backend()
            .unwrap();

        #[cfg(unix)]
        let (program, args) = ("/bin/sh", &["-c", "printf ok"][..]);
        #[cfg(windows)]
        let (program, args) = ("cmd", &["/C", "echo ok"][..]);

        let output = backend
            .core
            .sign_ctx
            .command_execute(program, args)
            .await
            .unwrap();
        assert!(output.success());
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "ok");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn azure_cli_credential_signs_requests() {
        let backend = AzfileBuilder::default()
            .endpoint("http://127.0.0.1:10002")
            .share_name("share")
            .build_backend()
            .unwrap();

        let az_cli = MockAzCli::default();
        let ctx = Context::new()
            .with_env(reqsign_core::StaticEnv::default())
            .with_command_execute(az_cli.clone());
        let signer = backend.core.signer_with_context(ctx);

        let (mut parts, _) = http::Request::get("http://127.0.0.1:10002/share/file.txt")
            .body(())
            .unwrap()
            .into_parts();
        signer.sign(&mut parts, None).await.unwrap();

        assert_eq!(
            parts.headers[http::header::AUTHORIZATION],
            "Bearer az-cli-token"
        );
        assert_eq!(az_cli.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn explicit_client_secret_precedes_azure_cli() {
        let backend = AzfileBuilder::default()
            .endpoint("http://127.0.0.1:10002")
            .share_name("share")
            .client_id("client-123")
            .client_secret("secret-123")
            .tenant_id("tenant-123")
            .authority_host("https://custom.authority.example")
            .build_backend()
            .unwrap();

        let http = MockTokenHttpSend::default();
        let az_cli = MockAzCli::default();
        let ctx = Context::new()
            .with_env(reqsign_core::StaticEnv::default())
            .with_http_send(http.clone())
            .with_command_execute(az_cli.clone());
        let signer = backend.core.signer_with_context(ctx);

        let (mut parts, _) = http::Request::get("http://127.0.0.1:10002/share/file.txt")
            .body(())
            .unwrap()
            .into_parts();
        signer.sign(&mut parts, None).await.unwrap();

        assert_eq!(
            parts.headers[http::header::AUTHORIZATION],
            "Bearer azfile-bearer-token"
        );
        assert_eq!(az_cli.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(http.captured.lock().unwrap().len(), 1);
    }
}
