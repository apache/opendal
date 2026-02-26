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
use std::sync::LazyLock;
use std::time::Duration;

use bytes::Bytes;
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
use reqsign_core::time::Timestamp;
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_google::Credential;
use reqsign_google::DefaultCredentialProvider;
use reqsign_google::RequestSigner;
use reqsign_google::StaticCredentialProvider;
use reqsign_google::Token;
use reqsign_http_send_reqwest::ReqwestHttpSend;
use serde::Deserialize;

use super::GCS_SCHEME;
use super::config::GcsConfig;
use super::core::*;
use super::deleter::GcsDeleter;
use super::error::parse_error;
use super::lister::GcsLister;
use super::writer::GcsWriter;
use super::writer::GcsWriters;
use opendal_core::raw::*;
use opendal_core::*;

static GLOBAL_REQWEST_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

const DEFAULT_GCS_ENDPOINT: &str = "https://storage.googleapis.com";
const DEFAULT_GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// [Google Cloud Storage](https://cloud.google.com/storage) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GcsBuilder {
    pub(super) config: GcsConfig,
    pub(super) credential_provider_chain: Option<ProvideCredentialChain<Credential>>,
}

#[derive(Clone, Debug)]
struct StaticTokenCredentialProvider {
    token: String,
}

impl StaticTokenCredentialProvider {
    fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait::async_trait]
impl ProvideCredential for StaticTokenCredentialProvider {
    type Credential = Credential;

    async fn provide_credential(
        &self,
        _: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        Ok(Some(Credential::with_token(Token {
            access_token: self.token.clone(),
            expires_at: None,
        })))
    }
}

#[derive(Clone, Debug)]
struct PathCredentialProvider {
    path: String,
    scope: String,
}

impl PathCredentialProvider {
    fn new(path: impl Into<String>, scope: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            scope: scope.into(),
        }
    }
}

#[async_trait::async_trait]
impl ProvideCredential for PathCredentialProvider {
    type Credential = Credential;

    async fn provide_credential(
        &self,
        ctx: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let content = String::from_utf8(ctx.file_read(&self.path).await?).map_err(|err| {
            reqsign_core::Error::unexpected("credential file content is invalid utf-8")
                .with_source(err)
        })?;

        StaticCredentialProvider::new(content)
            .with_scope(&self.scope)
            .provide_credential(ctx)
            .await
    }
}

#[derive(Debug, Deserialize)]
struct VmMetadataTokenResponse {
    access_token: String,
    expires_in: u64,
}

#[derive(Clone, Debug)]
struct ServiceAccountVmMetadataCredentialProvider {
    service_account: String,
    scope: String,
}

impl ServiceAccountVmMetadataCredentialProvider {
    fn new(service_account: impl Into<String>, scope: impl Into<String>) -> Self {
        Self {
            service_account: service_account.into(),
            scope: scope.into(),
        }
    }
}

#[async_trait::async_trait]
impl ProvideCredential for ServiceAccountVmMetadataCredentialProvider {
    type Credential = Credential;

    async fn provide_credential(
        &self,
        ctx: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let metadata_host = ctx
            .env_var("GCE_METADATA_HOST")
            .unwrap_or_else(|| "metadata.google.internal".to_string());

        let url = format!(
            "http://{metadata_host}/computeMetadata/v1/instance/service-accounts/{}/token?scopes={}",
            self.service_account, self.scope
        );

        let req = http::Request::builder()
            .method(http::Method::GET)
            .uri(&url)
            .header("Metadata-Flavor", "Google")
            .body(Bytes::new())
            .map_err(|err| {
                reqsign_core::Error::unexpected("failed to build vm metadata request")
                    .with_source(err)
            })?;

        let resp = ctx.http_send(req).await?;
        if resp.status() != StatusCode::OK {
            return Ok(None);
        }

        let resp: VmMetadataTokenResponse = serde_json::from_slice(resp.body()).map_err(|err| {
            reqsign_core::Error::unexpected("failed to parse vm metadata token response")
                .with_source(err)
        })?;

        Ok(Some(Credential::with_token(Token {
            access_token: resp.access_token,
            expires_at: Some(Timestamp::now() + Duration::from_secs(resp.expires_in)),
        })))
    }
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

    /// Allow anonymous requests.
    ///
    /// This is typically used for buckets which are open to the public or GCS
    /// storage emulators.
    pub fn allow_anonymous(mut self) -> Self {
        self.config.allow_anonymous = true;
        self
    }
}

impl Builder for GcsBuilder {
    type Config = GcsConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {self:?}");

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
            .with_http_send(ReqwestHttpSend::new(GLOBAL_REQWEST_CLIENT.clone()))
            .with_env(StaticEnv {
                home_dir: os_env.home_dir(),
                envs,
            });

        let mut default_credential = DefaultCredentialProvider::builder();
        #[cfg(target_arch = "wasm32")]
        {
            default_credential = default_credential
                .disable_env(true)
                .disable_well_known(true);
        }

        if self.config.disable_config_load {
            default_credential = default_credential
                .disable_env(true)
                .disable_well_known(true);
        }

        if self.config.disable_vm_metadata || self.config.service_account.is_some() {
            default_credential = default_credential.disable_vm_metadata(true);
        }

        let mut credential_chain = ProvideCredentialChain::new().push(default_credential.build());

        if !self.config.disable_vm_metadata {
            if let Some(service_account) = self.config.service_account.as_deref() {
                credential_chain = credential_chain.push(
                    ServiceAccountVmMetadataCredentialProvider::new(service_account, &scope),
                );
            }
        }

        if let Some(path) = self.config.credential_path.as_deref() {
            credential_chain =
                credential_chain.push_front(PathCredentialProvider::new(path, scope.clone()));
        }

        if let Some(content) = self.config.credential.as_deref() {
            if let Ok(provider) = StaticCredentialProvider::from_base64(content) {
                credential_chain = credential_chain.push_front(provider.with_scope(&scope));
            }
        }

        if let Some(token) = self.config.token.as_deref() {
            credential_chain =
                credential_chain.push_front(StaticTokenCredentialProvider::new(token));
        }

        if let Some(customized_credential_chain) = self.credential_provider_chain {
            credential_chain = credential_chain.push_front(customized_credential_chain);
        }

        let signer = Signer::new(
            ctx,
            credential_chain,
            RequestSigner::new("storage").with_scope(&scope),
        );

        let backend = GcsBackend {
            core: Arc::new(GcsCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(GCS_SCHEME)
                        .set_root(&root)
                        .set_name(bucket)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_with_if_match: true,
                            stat_with_if_none_match: true,

                            read: true,

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
                        });

                    am.into()
                },
                endpoint,
                bucket: bucket.to_string(),
                root,
                signer,
                predefined_acl: self.config.predefined_acl.clone(),
                default_storage_class: self.config.default_storage_class.clone(),
                allow_anonymous: self.config.allow_anonymous,
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

impl Access for GcsBackend {
    type Reader = HttpBody;
    type Writer = GcsWriters;
    type Lister = oio::PageLister<GcsLister>;
    type Deleter = oio::BatchDeleter<GcsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.gcs_get_object_metadata(path, &args).await?;

        if !resp.status().is_success() {
            return Err(parse_error(resp));
        }

        let slc = resp.into_body();
        let m = GcsCore::build_metadata_from_object_response(path, slc)?;

        Ok(RpStat::new(m))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.gcs_get_object(path, args.range(), &args).await?;

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
        let concurrent = args.concurrent();
        let w = GcsWriter::new(self.core.clone(), path, args);
        let w = oio::MultipartWriter::new(self.core.info.clone(), w, concurrent);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(
                GcsDeleter::new(self.core.clone()),
                self.core.info.full_capability().delete_max_size,
            ),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = GcsLister::new(
            self.core.clone(),
            path,
            args.recursive(),
            args.limit(),
            args.start_after(),
        );

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        let resp = self.core.gcs_copy_object(from, to).await?;

        if resp.status().is_success() {
            Ok(RpCopy::default())
        } else {
            Err(parse_error(resp))
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.gcs_head_object_xml_request(path, v),
            PresignOperation::Read(v) => self.core.gcs_get_object_xml_request(path, v),
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
        let req = self.core.sign_query(req, args.expire()).await?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}
