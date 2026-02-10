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
use bytes::Bytes;
use http::Request;
use http::Response;
use http::header;
use serde::Deserialize;

#[cfg(feature = "xet")]
use xet_utils::auth::TokenRefresher;

use super::error::parse_error;
use super::uri::HfRepo;
use opendal_core::raw::*;
use opendal_core::*;

/// API payload structures for commit operations
#[derive(Debug, serde::Serialize)]
pub(super) struct CommitFile {
    pub path: String,
    pub content: String,
    pub encoding: String,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct LfsFile {
    pub path: String,
    pub oid: String,
    pub algo: String,
    pub size: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct DeletedFile {
    pub path: String,
}

#[derive(serde::Serialize)]
pub(super) struct MixedCommitPayload {
    pub summary: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<CommitFile>,
    #[serde(rename = "lfsFiles", skip_serializing_if = "Vec::is_empty")]
    pub lfs_files: Vec<LfsFile>,
    #[serde(rename = "deletedFiles", skip_serializing_if = "Vec::is_empty")]
    pub deleted_files: Vec<DeletedFile>,
}

// API response types

#[derive(serde::Deserialize, Debug)]
pub(super) struct CommitResponse {
    #[serde(rename = "commitOid")]
    pub commit_oid: Option<String>,
    #[allow(dead_code)]
    #[serde(rename = "commitUrl")]
    pub commit_url: Option<String>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct PathInfo {
    #[serde(rename = "type")]
    pub type_: String,
    pub oid: String,
    pub size: u64,
    #[serde(default)]
    pub lfs: Option<LfsInfo>,
    pub path: String,
    #[serde(default)]
    pub last_commit: Option<LastCommit>,
}

impl PathInfo {
    pub fn entry_mode(&self) -> EntryMode {
        match self.type_.as_str() {
            "directory" => EntryMode::DIR,
            "file" => EntryMode::FILE,
            _ => EntryMode::Unknown,
        }
    }

    pub fn metadata(&self) -> Result<Metadata> {
        let mode = self.entry_mode();
        let mut meta = Metadata::new(mode);

        if let Some(commit_info) = self.last_commit.as_ref() {
            meta.set_last_modified(commit_info.date.parse::<Timestamp>()?);
        }

        if mode == EntryMode::FILE {
            meta.set_content_length(self.size);
            let etag = if let Some(lfs) = &self.lfs {
                &lfs.oid
            } else {
                &self.oid
            };
            meta.set_etag(etag);
        }

        Ok(meta)
    }
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub(super) struct LfsInfo {
    pub oid: String,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub(super) struct LastCommit {
    pub date: String,
}

#[cfg(feature = "xet")]
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct XetToken {
    pub access_token: String,
    pub cas_url: String,
    pub exp: u64,
}

// Core HuggingFace client that manages API interactions, authentication
// and shared logic for reader/writer/lister.

#[derive(Clone)]
pub struct HfCore {
    pub info: Arc<AccessorInfo>,

    pub repo: HfRepo,
    pub root: String,
    pub token: Option<String>,
    pub endpoint: String,
    pub max_retries: usize,

    // Whether XET storage protocol is enabled for reads. When true
    // and the `xet` feature is compiled in, reads will check for
    // XET-backed files and use the XET protocol for downloading.
    #[cfg(feature = "xet")]
    pub xet_enabled: bool,

    /// HTTP client with redirects disabled, used by XET probes to
    /// inspect headers on 302 responses.
    #[cfg(feature = "xet")]
    pub no_redirect_client: HttpClient,
}

impl Debug for HfCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("HfCore");
        s.field("repo", &self.repo)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint);
        #[cfg(feature = "xet")]
        s.field("xet_enabled", &self.xet_enabled);
        s.finish_non_exhaustive()
    }
}

impl HfCore {
    pub fn new(
        info: Arc<AccessorInfo>,
        repo: HfRepo,
        root: String,
        token: Option<String>,
        endpoint: String,
        max_retries: usize,
        #[cfg(feature = "xet")] xet_enabled: bool,
    ) -> Result<Self> {
        // When xet is enabled at runtime, use dedicated reqwest clients instead
        // of the global one. This avoids "dispatch task is gone" errors when
        // multiple tokio runtimes exist (e.g. in tests) and ensures the
        // no-redirect client shares the same runtime as the standard client.
        // When xet is disabled, preserve whatever HTTP client is already set
        // on `info` (important for mock-based unit tests).
        #[cfg(feature = "xet")]
        let no_redirect_client = if xet_enabled {
            let standard = HttpClient::with(build_reqwest(reqwest::redirect::Policy::default())?);
            let no_redirect = HttpClient::with(build_reqwest(reqwest::redirect::Policy::none())?);
            info.update_http_client(|_| standard);
            no_redirect
        } else {
            info.http_client()
        };

        Ok(Self {
            info,
            repo,
            root,
            token,
            endpoint,
            max_retries,
            #[cfg(feature = "xet")]
            xet_enabled,
            #[cfg(feature = "xet")]
            no_redirect_client,
        })
    }

    /// Build an authenticated HTTP request.
    pub(super) fn request(
        &self,
        method: http::Method,
        url: &str,
        op: Operation,
    ) -> http::request::Builder {
        let mut req = Request::builder().method(method).uri(url).extension(op);
        if let Some(token) = &self.token {
            if let Ok(auth) = format_authorization_by_bearer(token) {
                req = req.header(header::AUTHORIZATION, auth);
            }
        }
        req
    }

    pub(super) fn uri(&self, path: &str) -> super::uri::HfUri {
        self.repo.uri(&self.root, path)
    }

    /// Exponential backoff: 200ms, 400ms, 800ms, … capped at ~6s.
    async fn backoff(attempt: usize) {
        let millis = 200u64 * (1u64 << attempt.min(5));
        tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    }

    /// Send a request with retries, returning the successful response.
    ///
    /// Retries on commit conflicts (HTTP 412) and transient server errors
    /// (HTTP 5xx) up to `self.max_retries` attempts with exponential backoff.
    pub(super) async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        let client = self.info.http_client();
        let mut attempt = 0;
        loop {
            match client.send(req.clone()).await {
                Ok(resp) if resp.status().is_success() => {
                    return Ok(resp);
                }
                Ok(resp) => {
                    attempt += 1;
                    let err = parse_error(resp);
                    let retryable =
                        err.kind() == ErrorKind::ConditionNotMatch || err.is_temporary();
                    if attempt >= self.max_retries || !retryable {
                        return Err(err);
                    }
                    Self::backoff(attempt).await;
                }
                Err(err) => {
                    attempt += 1;
                    if attempt >= self.max_retries || !err.is_temporary() {
                        return Err(err);
                    }
                    Self::backoff(attempt).await;
                }
            }
        }
    }

    /// Send a request, check for success, and deserialize the JSON response.
    ///
    /// Returns the response parts (status, headers, etc.) alongside the
    /// deserialized body so callers can inspect headers when needed.
    pub(super) async fn send_parse<T: serde::de::DeserializeOwned>(
        &self,
        req: Request<Buffer>,
    ) -> Result<(http::response::Parts, T)> {
        let (parts, body) = self.send(req).await?.into_parts();
        let parsed = serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;
        Ok((parts, parsed))
    }

    pub(super) async fn path_info(&self, path: &str) -> Result<PathInfo> {
        let uri = self.uri(path);
        let url = uri.paths_info_url(&self.endpoint);
        let form_body = format!("paths={}&expand=True", percent_encode_path(&uri.path));

        let req = self
            .request(http::Method::POST, &url, Operation::Stat)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(Bytes::from(form_body)))
            .map_err(new_request_build_error)?;
        let (_, mut files) = self.send_parse::<Vec<PathInfo>>(req).await?;

        // NOTE: if the file is not found, the server will return 200 with an empty array
        if files.is_empty() {
            return Err(Error::new(ErrorKind::NotFound, "path not found"));
        }

        Ok(files.remove(0))
    }

    #[cfg(feature = "xet")]
    pub(super) async fn get_xet_token(&self, token_type: &str) -> Result<XetToken> {
        let url = self.repo.xet_token_url(&self.endpoint, token_type);
        let req = self
            .request(http::Method::GET, &url, Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let (_, token) = self.send_parse(req).await?;
        Ok(token)
    }

    /// Commit file changes (uploads and/or deletions) to the repository.
    ///
    /// Retries on commit conflicts (HTTP 412) and transient server errors
    /// (HTTP 5xx), matching the behavior of the official HuggingFace Hub
    /// client.
    pub(super) async fn commit_files(
        &self,
        regular_files: Vec<CommitFile>,
        lfs_files: Vec<LfsFile>,
        deleted_files: Vec<DeletedFile>,
    ) -> Result<CommitResponse> {
        let _token = self.token.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::PermissionDenied,
                "token is required for commit operations",
            )
            .with_operation("commit")
        })?;

        let first_path = regular_files
            .first()
            .map(|f| f.path.as_str())
            .or_else(|| lfs_files.first().map(|f| f.path.as_str()))
            .or_else(|| deleted_files.first().map(|f| f.path.as_str()))
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "no files to commit"))?;

        let uri = self.uri(first_path);
        let url = uri.commit_url(&self.endpoint);

        let payload = MixedCommitPayload {
            summary: "Commit via OpenDAL".to_string(),
            files: regular_files,
            lfs_files,
            deleted_files,
        };

        let json_body = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;

        let req = self
            .request(http::Method::POST, &url, Operation::Write)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, json_body.len())
            .body(Buffer::from(json_body))
            .map_err(new_request_build_error)?;

        let (_, resp) = self.send_parse::<CommitResponse>(req).await?;
        Ok(resp)
    }
}

#[cfg(feature = "xet")]
pub(super) struct XetTokenRefresher {
    core: HfCore,
    token_type: &'static str,
}

#[cfg(feature = "xet")]
impl XetTokenRefresher {
    pub(super) fn new(core: &HfCore, token_type: &'static str) -> Self {
        Self {
            core: core.clone(),
            token_type,
        }
    }
}

#[cfg(feature = "xet")]
#[async_trait::async_trait]
impl TokenRefresher for XetTokenRefresher {
    async fn refresh(&self) -> std::result::Result<(String, u64), xet_utils::errors::AuthError> {
        let token = self
            .core
            .get_xet_token(self.token_type)
            .await
            .map_err(xet_utils::errors::AuthError::token_refresh_failure)?;
        Ok((token.access_token, token.exp))
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use http::{Request, Response, StatusCode};
    use std::sync::{Arc, Mutex};

    use super::super::uri::RepoType;
    use super::*;

    #[derive(Clone)]
    pub(crate) struct MockHttpClient {
        url: Arc<Mutex<Option<String>>>,
    }

    impl MockHttpClient {
        pub(crate) fn new() -> Self {
            Self {
                url: Arc::new(Mutex::new(None)),
            }
        }

        pub(crate) fn get_captured_url(&self) -> String {
            self.url.lock().unwrap().clone().unwrap()
        }
    }

    impl HttpFetch for MockHttpClient {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            *self.url.lock().unwrap() = Some(req.uri().to_string());

            // Return a minimal valid JSON response for API requests
            let body = if req.uri().to_string().contains("/paths-info/")
                || req.uri().to_string().contains("/tree/")
            {
                let data =
                    Bytes::from(r#"[{"type":"file","oid":"abc123","size":100,"path":"test.txt"}]"#);
                let size = data.len() as u64;
                let buffer = Buffer::from(data);
                HttpBody::new(futures::stream::iter(vec![Ok(buffer)]), Some(size))
            } else {
                HttpBody::new(futures::stream::empty(), Some(0))
            };

            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .unwrap())
        }
    }

    pub(crate) fn create_test_core(
        repo_type: RepoType,
        repo_id: &str,
        revision: &str,
        endpoint: &str,
    ) -> (HfCore, MockHttpClient) {
        let mock_client = MockHttpClient::new();
        let http_client = HttpClient::with(mock_client.clone());

        let info = AccessorInfo::default();
        info.set_scheme("hf")
            .set_native_capability(Capability::default());
        info.update_http_client(|_| http_client);

        let core = HfCore::new(
            Arc::new(info),
            HfRepo::new(repo_type, repo_id.to_string(), Some(revision.to_string())),
            "/".to_string(),
            None,
            endpoint.to_string(),
            3,
            #[cfg(feature = "xet")]
            false,
        )
        .unwrap();

        (core, mock_client)
    }
}

#[cfg(test)]
mod tests {
    use super::super::uri::RepoType;
    use super::test_utils::create_test_core;
    use super::*;

    #[tokio::test]
    async fn test_hf_path_info_url_model() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );

        core.path_info("test.txt").await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/models/test-user/test-repo/paths-info/main"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_path_info_url_dataset() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Dataset,
            "test-org/test-dataset",
            "v1.0.0",
            "https://huggingface.co",
        );

        core.path_info("data/file.csv").await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/datasets/test-org/test-dataset/paths-info/v1%2E0%2E0"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_path_info_url_custom_endpoint() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Model,
            "test-org/test-dataset",
            "refs/convert/parquet",
            "https://custom-hf.example.com",
        );

        core.path_info("model.bin").await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://custom-hf.example.com/api/models/test-org/test-dataset/paths-info/refs%2Fconvert%2Fparquet"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_path_info_url_space() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Space,
            "test-user/test-space",
            "main",
            "https://huggingface.co",
        );

        core.path_info("app.py").await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/spaces/test-user/test-space/paths-info/main"
        );

        Ok(())
    }
}

#[cfg(feature = "xet")]
pub(super) fn map_xet_error(err: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Unexpected, "xet operation failed").set_source(err)
}

#[cfg(feature = "xet")]
fn build_reqwest(policy: reqwest::redirect::Policy) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(policy)
        .build()
        .map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to build http client").set_source(err)
        })
}
