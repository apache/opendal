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

use backon::ExponentialBuilder;
use backon::Retryable;
use bytes::Buf;
use bytes::Bytes;
use http::Request;
use http::Response;
use http::header;
use serde::Deserialize;

use subxet::data::XetFileInfo;
use subxet::data::streaming::XetClient;
use subxet::utils::auth::TokenRefresher;

use super::error::parse_error;
use super::uri::HfRepo;
use opendal_core::raw::*;
use opendal_core::*;

/// API payload structures for preupload operations
#[derive(serde::Serialize)]
struct PreuploadFile {
    path: String,
    size: i64,
    sample: String,
}

#[derive(serde::Serialize)]
struct PreuploadRequest {
    files: Vec<PreuploadFile>,
}

#[derive(serde::Deserialize, Debug)]
struct PreuploadFileResponse {
    #[allow(dead_code)]
    path: String,
    #[serde(rename = "uploadMode")]
    upload_mode: String,
}

#[derive(serde::Deserialize, Debug)]
struct PreuploadResponse {
    files: Vec<PreuploadFileResponse>,
}

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

/// Bucket batch operation payload structures
#[derive(Debug, serde::Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum BucketOperation {
    #[serde(rename_all = "camelCase")]
    AddFile { path: String, xet_hash: String },
    #[serde(rename_all = "camelCase")]
    #[allow(dead_code)]
    DeleteFile { path: String },
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
    #[serde(default)]
    pub oid: Option<String>,
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
            // For buckets, oid may be None; for regular repos, prefer lfs.oid then oid
            if let Some(lfs) = &self.lfs {
                meta.set_etag(&lfs.oid);
            } else if let Some(oid) = &self.oid {
                meta.set_etag(oid);
            }
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

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct XetToken {
    pub access_token: String,
    pub cas_url: String,
    pub exp: u64,
}

pub(super) struct XetTokenRefresher {
    core: HfCore,
    token_type: &'static str,
}

impl XetTokenRefresher {
    pub(super) fn new(core: &HfCore, token_type: &'static str) -> Self {
        Self {
            core: core.clone(),
            token_type,
        }
    }
}

#[async_trait::async_trait]
impl TokenRefresher for XetTokenRefresher {
    async fn refresh(
        &self,
    ) -> std::result::Result<(String, u64), subxet::utils::errors::AuthError> {
        let token = self
            .core
            .xet_token(self.token_type)
            .await
            .map_err(subxet::utils::errors::AuthError::token_refresh_failure)?;
        Ok((token.access_token, token.exp))
    }
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

    /// HTTP client with redirects disabled, used by XET probes to
    /// inspect headers on 302 responses.
    pub no_redirect_client: HttpClient,
}

impl Debug for HfCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HfCore")
            .field("repo", &self.repo)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
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
        no_redirect_client: HttpClient,
    ) -> Self {
        Self {
            info,
            repo,
            root,
            token,
            endpoint,
            max_retries,
            no_redirect_client,
        }
    }

    /// Build HfCore with dedicated reqwest HTTP clients.
    ///
    /// Uses separate clients for standard and no-redirect requests to
    /// avoid "dispatch task is gone" errors with multiple tokio runtimes.
    pub fn build(
        info: Arc<AccessorInfo>,
        repo: HfRepo,
        root: String,
        token: Option<String>,
        endpoint: String,
        max_retries: usize,
    ) -> Result<Self> {
        let standard = HttpClient::with(build_reqwest(reqwest::redirect::Policy::default())?);
        let no_redirect = HttpClient::with(build_reqwest(reqwest::redirect::Policy::none())?);
        info.update_http_client(|_| standard);

        Ok(Self::new(
            info,
            repo,
            root,
            token,
            endpoint,
            max_retries,
            no_redirect,
        ))
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

    /// Send a request with retries, returning the successful response.
    ///
    /// Retries on commit conflicts (HTTP 412) and transient server errors
    /// (HTTP 5xx) up to `self.max_retries` attempts with exponential backoff.
    pub(super) async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        let backoff = ExponentialBuilder::default()
            .with_min_delay(std::time::Duration::from_millis(200))
            .with_max_delay(std::time::Duration::from_millis(6400))
            .with_max_times(self.max_retries.saturating_sub(1));
        let client = self.info.http_client();

        let send_once = || async {
            let resp = client.send(req.clone()).await?;
            if resp.status().is_success() {
                Ok(resp)
            } else {
                Err(parse_error(resp))
            }
        };

        send_once
            .retry(backoff)
            .when(|e: &Error| e.kind() == ErrorKind::ConditionNotMatch || e.is_temporary())
            .await
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

    pub(super) async fn xet_token(&self, token_type: &str) -> Result<XetToken> {
        let url = self.repo.xet_token_url(&self.endpoint, token_type);
        let req = self
            .request(http::Method::GET, &url, Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let (_, token) = self.send_parse(req).await?;
        Ok(token)
    }

    pub(super) async fn xet_client(&self, token_type: &'static str) -> Result<XetClient> {
        let token = self.xet_token(token_type).await?;
        let refresher = Arc::new(XetTokenRefresher::new(self, token_type));
        XetClient::new(
            Some(token.cas_url),
            Some((token.access_token, token.exp)),
            Some(refresher),
            "opendal/1.0".to_string(),
        )
        .map_err(map_xet_error)
    }

    /// Issue a HEAD request and extract XET file info (hash and size).
    ///
    /// Returns `None` if the `X-Xet-Hash` header is absent or empty.
    ///
    /// Uses a dedicated no-redirect HTTP client so we can inspect
    /// headers (e.g. `X-Xet-Hash`) on the 302 response.
    pub(super) async fn maybe_xet_file(&self, path: &str) -> Result<Option<XetFileInfo>> {
        let uri = self.uri(path);
        let url = uri.resolve_url(&self.endpoint);

        let req = self
            .request(http::Method::HEAD, &url, Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let mut attempt = 0;
        let resp = loop {
            let resp = self.no_redirect_client.send(req.clone()).await?;

            attempt += 1;
            let retryable = resp.status().is_server_error();
            if attempt >= self.max_retries || !retryable {
                break resp;
            }
        };

        let hash = resp
            .headers()
            .get("X-Xet-Hash")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty());

        let Some(hash) = hash else {
            return Ok(None);
        };

        let size = resp
            .headers()
            .get("X-Linked-Size")
            .or_else(|| resp.headers().get(header::CONTENT_LENGTH))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        Ok(Some(XetFileInfo::new(hash.to_string(), size)))
    }

    /// Commit file changes (uploads and/or deletions) to the repository.
    ///
    /// Retries on commit conflicts (HTTP 412) and transient server errors
    /// (HTTP 5xx), matching the behavior of the official HuggingFace Hub
    /// client.
    /// Determine upload mode by calling the preupload API.
    ///
    /// Returns the upload mode string from the API (e.g., "regular" or "lfs").
    pub(super) async fn determine_upload_mode(&self, path: &str) -> Result<String> {
        let uri = self.uri(path);
        let preupload_url = uri.preupload_url(&self.endpoint);

        let preupload_payload = PreuploadRequest {
            files: vec![PreuploadFile {
                path: path.to_string(),
                size: -1,
                sample: String::new(),
            }],
        };
        let json_body = serde_json::to_vec(&preupload_payload).map_err(new_json_serialize_error)?;

        let req = self
            .request(http::Method::POST, &preupload_url, Operation::Write)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Buffer::from(json_body))
            .map_err(new_request_build_error)?;

        let (_, preupload_resp): (_, PreuploadResponse) = self.send_parse(req).await?;

        let mode = preupload_resp
            .files
            .first()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "no files in preupload response"))?
            .upload_mode
            .clone();

        Ok(mode)
    }

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

    /// Upload files to a bucket using the batch API.
    ///
    /// Sends operations as JSON lines (one operation per line).
    pub(super) async fn bucket_batch(&self, operations: Vec<BucketOperation>) -> Result<()> {
        let _token = self.token.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::PermissionDenied,
                "token is required for bucket operations",
            )
            .with_operation("bucket_batch")
        })?;

        if operations.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "no operations to perform",
            ));
        }

        let url = self.repo.bucket_batch_url(&self.endpoint);

        let mut body = String::new();
        for op in operations {
            let json = serde_json::to_string(&op).map_err(new_json_serialize_error)?;
            body.push_str(&json);
            body.push('\n');
        }

        let req = self
            .request(http::Method::POST, &url, Operation::Write)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        self.send(req).await?;
        Ok(())
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
            HttpClient::with(mock_client.clone()),
        );

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

pub(super) fn map_xet_error(err: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Unexpected, "xet operation failed").set_source(err)
}

fn build_reqwest(policy: reqwest::redirect::Policy) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(policy)
        .build()
        .map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to build http client").set_source(err)
        })
}
