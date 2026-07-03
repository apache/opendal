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

use bytes::Buf;
use bytes::Bytes;
use http::Request;
use http::Response;
use http::header;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use xet::xet_session::{XetDownloadStreamGroup, XetSession, XetSessionBuilder, XetUploadCommit};

use opendal_core::raw::*;
use opendal_core::*;

use super::HUGGINGFACE_SCHEME;

/// Repository type of Huggingface. Supports `model`, `dataset`, `space`, and `bucket`.
/// [Reference](https://huggingface.co/docs/hub/repositories)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HfRepoType {
    Model,
    Dataset,
    Space,
    Bucket,
}

impl HfRepoType {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().replace(' ', "").as_str() {
            "model" | "models" => Ok(Self::Model),
            "dataset" | "datasets" => Ok(Self::Dataset),
            "space" | "spaces" => Ok(Self::Space),
            "bucket" | "buckets" => Ok(Self::Bucket),
            other => Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("unknown repo type: {other}"),
            )
            .with_context("service", HUGGINGFACE_SCHEME)),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Dataset => "dataset",
            Self::Space => "space",
            Self::Bucket => "bucket",
        }
    }

    pub fn as_plural_str(&self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
            Self::Bucket => "buckets",
        }
    }
}

/// Download mode for HuggingFace files.
///
/// - `xet` (default): uses the XET protocol, asks resolve for XET file metadata,
///   and routes XET files through the CAS download stream.
/// - `http`: follows the resolve redirect and streams bytes directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum HfDownloadMode {
    #[default]
    Xet,
    Http,
}

impl HfDownloadMode {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "xet" => Ok(Self::Xet),
            "http" => Ok(Self::Http),
            other => Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("unknown download mode: {other}"),
            )
            .with_context("service", HUGGINGFACE_SCHEME)),
        }
    }
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

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct DeletedFolder {
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
    #[serde(rename = "deletedFolders", skip_serializing_if = "Vec::is_empty")]
    pub deleted_folders: Vec<DeletedFolder>,
}

// API response types

#[derive(Deserialize)]
pub(super) struct XetFileResponse {
    pub hash: String,
    pub size: u64,
}

#[derive(serde::Deserialize, Debug)]
pub(super) struct CommitResponse {
    #[allow(dead_code)]
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
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub lfs: Option<LfsInfo>,
    pub path: String,
    #[serde(default)]
    pub last_commit: Option<LastCommit>,
    /// BLAKE3 Merkle hash for XET-stored files; absent for plain git or non-XET LFS files.
    #[serde(rename = "xetHash", default)]
    pub xet_hash: Option<String>,
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
    pub id: String,
    pub date: String,
}

// Core HuggingFace client that manages API interactions, authentication
// and shared logic for reader/writer/lister.

#[derive(Clone)]
pub struct HfCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub repo: HfRepo,
    pub root: String,
    pub token: Option<String>,
    pub endpoint: String,
    pub xet_session: XetSession,
    pub download_mode: HfDownloadMode,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        info: ServiceInfo,
        capability: Capability,
        repo: HfRepo,
        root: String,
        token: Option<String>,
        endpoint: String,
        xet_session: XetSession,
        download_mode: HfDownloadMode,
    ) -> Self {
        Self {
            info,
            capability,
            repo,
            root,
            token,
            endpoint,
            xet_session,
            download_mode,
        }
    }

    pub fn build(
        info: ServiceInfo,
        capability: Capability,
        repo: HfRepo,
        root: String,
        token: Option<String>,
        endpoint: String,
        download_mode: HfDownloadMode,
    ) -> Result<Self> {
        let xet_session = XetSessionBuilder::new().build().map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to create xet session").set_source(err)
        })?;

        Ok(Self::new(
            info,
            capability,
            repo,
            root,
            token,
            endpoint,
            xet_session,
            download_mode,
        ))
    }

    fn xet_token_refresh_headers(&self) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        if let Some(token) = &self.token
            && let Ok(val) = format!("Bearer {}", token).parse()
        {
            headers.insert(header::AUTHORIZATION, val);
        }
        headers
    }

    /// Create a new XET upload commit with token refresh configured.
    ///
    /// Each call creates a fresh upload commit from the shared XET session.
    pub(super) async fn xet_upload_commit(&self) -> Result<XetUploadCommit> {
        let refresh_url = self.repo.xet_token_url(&self.endpoint, "write");
        let refresh_headers = self.xet_token_refresh_headers();
        self.xet_session
            .new_upload_commit()
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "failed to create xet upload commit")
                    .set_source(err)
            })?
            .with_token_refresh_url(refresh_url, refresh_headers)
            .build()
            .await
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "failed to build xet upload commit")
                    .set_source(err)
            })
    }

    /// Create a new XET download stream group with token refresh configured.
    ///
    /// Each call creates a fresh download group from the shared XET session.
    pub(super) async fn xet_download_group(&self) -> Result<XetDownloadStreamGroup> {
        let refresh_url = self.repo.xet_token_url(&self.endpoint, "read");
        let refresh_headers = self.xet_token_refresh_headers();
        self.xet_session
            .new_download_stream_group()
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to create download stream group",
                )
                .set_source(err)
            })?
            .with_token_refresh_url(refresh_url, refresh_headers)
            .build()
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to build download stream group",
                )
                .set_source(err)
            })
    }

    /// Build an authenticated HTTP request.
    ///
    /// Returns `PermissionDenied` for write operations when no token
    /// is configured.
    pub(super) fn request(
        &self,
        method: http::Method,
        url: &str,
        op: Operation,
        service_operation: &'static str,
    ) -> Result<http::request::Builder> {
        let mut req = Request::builder()
            .method(method)
            .uri(url)
            .extension(op)
            .extension(ServiceOperation(service_operation));
        match &self.token {
            Some(token) => {
                if let Ok(auth) = format_authorization_by_bearer(token) {
                    req = req.header(header::AUTHORIZATION, auth);
                }
            }
            None if matches!(op, Operation::Write | Operation::Delete) => {
                return Err(Error::new(
                    ErrorKind::PermissionDenied,
                    "token is required for write operations",
                ));
            }
            None => {}
        }
        Ok(req)
    }

    /// Build an [`HfUri`] for the given operator-relative path.
    pub(super) fn uri(&self, path: &str) -> HfUri {
        self.repo.uri(&self.root, path)
    }

    /// Convert an operator-relative path to a repo-absolute path
    /// (no leading `/`) for use in commit/delete/batch payloads.
    pub(super) fn repo_path(&self, path: &str) -> String {
        build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string()
    }

    /// Send a request and return the successful streaming response or a parsed error.
    ///
    /// Uses `fetch` so error response bodies are never read into memory —
    /// `parse_error` reads only response headers.
    async fn send(
        &self,
        ctx: &OperationContext,
        req: Request<Buffer>,
    ) -> Result<Response<HttpBody>> {
        let resp = ctx.http_transport().fetch(req).await?;
        let (parts, body) = resp.into_parts();
        if parts.status.is_success() {
            Ok(Response::from_parts(parts, body))
        } else {
            // Drop the streaming body without reading it.
            Err(parse_error(parts))
        }
    }

    /// Send a request, check for success, and deserialize the JSON response.
    ///
    /// Returns the response parts (status, headers, etc.) alongside the
    /// deserialized body so callers can inspect headers when needed.
    pub(super) async fn send_parse<T: serde::de::DeserializeOwned>(
        &self,
        ctx: &OperationContext,
        req: Request<Buffer>,
    ) -> Result<(http::response::Parts, T)> {
        let (parts, mut body) = self.send(ctx, req).await?.into_parts();
        let buffer = body.to_buffer().await?;
        let parsed =
            serde_json::from_reader(buffer.reader()).map_err(new_json_deserialize_error)?;
        Ok((parts, parsed))
    }

    pub(super) async fn path_info(&self, ctx: &OperationContext, path: &str) -> Result<PathInfo> {
        let uri = self.uri(path);
        let url = uri.paths_info_url(&self.endpoint);
        let form_body = format!("paths={}&expand=True", percent_encode_path(&uri.path));

        let req = self
            .request(http::Method::POST, &url, Operation::Stat, "PathInfo")?
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(Bytes::from(form_body)))
            .map_err(new_request_build_error)?;
        let (_, mut files) = self.send_parse::<Vec<PathInfo>>(ctx, req).await?;

        // NOTE: if the file is not found, the server will return 200 with an empty array
        if files.is_empty() {
            return Err(Error::new(ErrorKind::NotFound, "path not found"));
        }

        Ok(files.remove(0))
    }

    /// Send `GET /resolve` and return the raw streaming response.
    ///
    /// In `Xet` mode adds `Accept: application/vnd.xet-fileinfo+json` so the
    /// server returns XET metadata instead of redirecting; in `Http` mode the
    /// redirect is followed and the file bytes are streamed directly.
    pub(super) async fn resolve(
        &self,
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
        mode: HfDownloadMode,
    ) -> Result<Response<HttpBody>> {
        let uri = self.uri(path);
        let url = uri.resolve_url(&self.endpoint, self.repo.revision());

        let mut req = self.request(http::Method::GET, &url, Operation::Read, "Resolve")?;

        if mode == HfDownloadMode::Xet {
            req = req.header(header::ACCEPT, "application/vnd.xet-fileinfo+json");
        }

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        let resp = ctx.http_transport().fetch(req).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            // Drop the streaming body without reading it — parse_error reads
            // only response headers, so there is no need to buffer the body
            // (which may be a large HTML error page).
            let (parts, _) = resp.into_parts();
            let mut err = parse_error(parts);
            if status == http::StatusCode::NOT_FOUND && self.path_info(ctx, path).await.is_ok() {
                err = err.set_temporary();
            }
            return Err(err);
        }

        Ok(resp)
    }

    /// Commit file changes to a git-based repo (model/dataset/space).
    ///
    /// Counterpart of [`commit_bucket`](Self::commit_bucket) for bucket repos.
    pub(super) async fn commit_git(
        &self,
        ctx: &OperationContext,
        regular_files: Vec<CommitFile>,
        lfs_files: Vec<LfsFile>,
        deleted_files: Vec<DeletedFile>,
        deleted_folders: Vec<DeletedFolder>,
    ) -> Result<CommitResponse> {
        let url = self.repo.git_commit_url(&self.endpoint);

        let payload = MixedCommitPayload {
            summary: "Commit via OpenDAL".to_string(),
            files: regular_files,
            lfs_files,
            deleted_files,
            deleted_folders,
        };

        let json_body = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;

        let req = self
            .request(http::Method::POST, &url, Operation::Write, "CommitGit")?
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, json_body.len())
            .body(Buffer::from(json_body))
            .map_err(new_request_build_error)?;

        let (_, resp) = self.send_parse::<CommitResponse>(ctx, req).await?;
        Ok(resp)
    }

    /// Commit file changes to a bucket repo via the NDJSON batch API.
    ///
    /// Counterpart of [`commit_git`](Self::commit_git) for git-based repos.
    pub(super) async fn commit_bucket(
        &self,
        ctx: &OperationContext,
        operations: Vec<BucketOperation>,
    ) -> Result<()> {
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
            .request(http::Method::POST, &url, Operation::Write, "CommitBucket")?
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        self.send(ctx, req).await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use http::{Request, Response, StatusCode};
    use std::sync::{Arc, Mutex};

    use super::super::core::HfRepoType;
    use super::*;

    #[derive(Clone)]
    pub(crate) struct MockHttpTransport {
        url: Arc<Mutex<Option<String>>>,
        body: Arc<Mutex<Option<String>>>,
    }

    impl MockHttpTransport {
        pub(crate) fn new() -> Self {
            Self {
                url: Arc::new(Mutex::new(None)),
                body: Arc::new(Mutex::new(None)),
            }
        }

        pub(crate) fn get_captured_url(&self) -> String {
            self.url.lock().unwrap().clone().unwrap()
        }

        pub(crate) fn get_captured_body(&self) -> String {
            self.body.lock().unwrap().clone().unwrap_or_default()
        }
    }

    impl HttpTransport for MockHttpTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            *self.url.lock().unwrap() = Some(req.uri().to_string());
            *self.body.lock().unwrap() = Some(
                String::from_utf8(req.body().to_bytes().to_vec())
                    .expect("request body must be utf-8 for test payloads"),
            );

            // Return a minimal valid JSON response for API requests
            let (body, content_length) = if req.uri().to_string().contains("/paths-info/")
                || req.uri().to_string().contains("/tree/")
            {
                let data =
                    Bytes::from(r#"[{"type":"file","oid":"abc123","size":100,"path":"test.txt"}]"#);
                let size = data.len() as u64;
                let buffer = Buffer::from(data);
                (
                    HttpBody::new(futures::stream::iter(vec![Ok(buffer)]), Some(size)),
                    size,
                )
            } else if req.uri().to_string().contains("/commit/") {
                let data = Bytes::from(r#"{}"#);
                let size = data.len() as u64;
                let buffer = Buffer::from(data);
                (
                    HttpBody::new(futures::stream::iter(vec![Ok(buffer)]), Some(size)),
                    size,
                )
            } else {
                let data = Bytes::from_static(b"hello");
                let size = data.len() as u64;
                let buffer = Buffer::from(data);
                (
                    HttpBody::new(futures::stream::iter(vec![Ok(buffer)]), Some(size)),
                    size,
                )
            };

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, content_length)
                .body(body)
                .unwrap())
        }
    }

    pub(crate) fn create_test_core(
        repo_type: HfRepoType,
        repo_id: &str,
        revision: &str,
        endpoint: &str,
    ) -> (HfCore, OperationContext, MockHttpTransport) {
        let mock_client = MockHttpTransport::new();
        let http_transport = HttpTransporter::new(mock_client.clone());
        let ctx = OperationContext::from_parts(http_transport, Executor::default());

        let info = ServiceInfo::new("hf", "", "");
        let capability = Capability::default();

        let xet_session = XetSessionBuilder::new()
            .build()
            .expect("failed to create xet session");
        let core = HfCore::new(
            info,
            capability,
            HfRepo::new(repo_type, repo_id.to_string(), Some(revision.to_string())),
            "/".to_string(),
            None,
            endpoint.to_string(),
            xet_session,
            HfDownloadMode::Xet,
        );

        (core, ctx, mock_client)
    }
}

#[cfg(test)]
mod tests {
    use super::super::core::HfRepoType;
    use super::test_utils::create_test_core;
    use super::*;

    #[tokio::test]
    async fn test_hf_path_info_url_model() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );

        core.path_info(&ctx, "test.txt").await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/models/test-user/test-repo/paths-info/main"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_path_info_url_dataset() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Dataset,
            "test-org/test-dataset",
            "v1.0.0",
            "https://huggingface.co",
        );

        core.path_info(&ctx, "data/file.csv").await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/datasets/test-org/test-dataset/paths-info/v1%2E0%2E0"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_path_info_url_custom_endpoint() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Model,
            "test-org/test-dataset",
            "refs/convert/parquet",
            "https://custom-hf.example.com",
        );

        core.path_info(&ctx, "model.bin").await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://custom-hf.example.com/api/models/test-org/test-dataset/paths-info/refs%2Fconvert%2Fparquet"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_path_info_url_space() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Space,
            "test-user/test-space",
            "main",
            "https://huggingface.co",
        );

        core.path_info(&ctx, "app.py").await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/spaces/test-user/test-space/paths-info/main"
        );

        Ok(())
    }
}

mod error {
    use http::StatusCode;

    use opendal_core::raw::*;
    use opendal_core::*;

    pub(crate) fn parse_error(parts: http::response::Parts) -> Error {
        // HF sets x-error-message on every error response with a short human-readable
        // description. Using the header avoids reading the response body, which can be
        // a large HTML error page (e.g. 52 KB on 404s from the /resolve/ endpoint).
        let message = parts
            .headers
            .get("x-error-message")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown error")
            .to_string();

        // HF git-style commit APIs reject stale branch snapshots with 412.
        // Treat this specific conflict as temporary so RetryLayer can replay
        // the whole write/delete close sequence on a fresh branch head.
        let branch_updated_conflict = parts.status == StatusCode::PRECONDITION_FAILED
            && message
                .to_ascii_lowercase()
                .contains("branch was updated since you opened this page");

        let (kind, retryable) = match parts.status {
            StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                (ErrorKind::PermissionDenied, false)
            }
            StatusCode::PRECONDITION_FAILED => {
                (ErrorKind::ConditionNotMatch, branch_updated_conflict)
            }
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
            _ => (ErrorKind::Unexpected, false),
        };

        let mut err = Error::new(kind, message);

        err = with_error_response_context(err, parts);

        if retryable {
            err = err.set_temporary();
        }

        err
    }

    #[cfg(test)]
    mod test {
        use http::Response;
        use http::StatusCode;

        use super::*;

        #[test]
        fn test_parse_error_branch_update_conflict_is_temporary() {
            let (parts, _) = Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .header(
                "x-error-message",
                "The branch was updated since you opened this page. Please refresh and try again.",
            )
            .body(())
            .unwrap()
            .into_parts();

            let err = parse_error(parts);

            assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
            assert!(err.is_temporary());
        }

        #[test]
        fn test_parse_error_other_precondition_failed_is_not_temporary() {
            let (parts, _) = Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .header("x-error-message", "etag mismatch")
                .body(())
                .unwrap()
                .into_parts();

            let err = parse_error(parts);

            assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
            assert!(!err.is_temporary());
        }
    }
}

pub(super) use error::*;

mod uri {
    use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

    pub use super::HfRepoType;
    use crate::HUGGINGFACE_SCHEME;
    use opendal_core::raw::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct HfRepo {
        pub repo_type: HfRepoType,
        pub repo_id: String,
        pub revision: Option<String>,
    }

    impl HfRepo {
        pub fn new(repo_type: HfRepoType, repo_id: String, revision: Option<String>) -> Self {
            Self {
                repo_type,
                repo_id,
                revision,
            }
        }

        /// Whether this repo is a bucket (as opposed to a git-based repo).
        pub fn is_bucket(&self) -> bool {
            self.repo_type == HfRepoType::Bucket
        }

        /// Return the revision, defaulting to "main" if unset.
        pub fn revision(&self) -> &str {
            self.revision.as_deref().unwrap_or("main")
        }

        /// Create an `HfUri` for the given root and path within this repo.
        pub fn uri(&self, root: &str, path: &str) -> HfUri {
            HfUri {
                repo: self.clone(),
                path: build_abs_path(root, path)
                    .trim_start_matches('/')
                    .trim_end_matches('/')
                    .to_string(),
            }
        }

        /// Build the paths-info API URL for this repository.
        pub fn paths_info_url(&self, endpoint: &str) -> String {
            match self.repo_type {
                HfRepoType::Bucket => {
                    format!("{}/api/buckets/{}/paths-info", endpoint, &self.repo_id)
                }
                _ => {
                    format!(
                        "{}/api/{}/{}/paths-info/{}",
                        endpoint,
                        self.repo_type.as_plural_str(),
                        &self.repo_id,
                        percent_encode_revision(self.revision()),
                    )
                }
            }
        }

        /// Build the XET token API URL for this repository.
        pub fn xet_token_url(&self, endpoint: &str, token_type: &str) -> String {
            match self.repo_type {
                HfRepoType::Bucket => {
                    format!(
                        "{}/api/buckets/{}/xet-{}-token",
                        endpoint, &self.repo_id, token_type
                    )
                }
                _ => {
                    format!(
                        "{}/api/{}/{}/xet-{}-token/{}",
                        endpoint,
                        self.repo_type.as_plural_str(),
                        &self.repo_id,
                        token_type,
                        self.revision(),
                    )
                }
            }
        }

        /// Build the bucket batch API URL for this repository.
        pub fn bucket_batch_url(&self, endpoint: &str) -> String {
            format!("{}/api/buckets/{}/batch", endpoint, &self.repo_id)
        }

        /// Build the git commit API URL for this repository.
        pub fn git_commit_url(&self, endpoint: &str) -> String {
            format!(
                "{}/api/{}/{}/commit/{}",
                endpoint,
                self.repo_type.as_plural_str(),
                &self.repo_id,
                percent_encode_revision(self.revision()),
            )
        }
    }

    /// Parsed Hugging Face URI following the official format:
    /// `hf://[<repo_type_prefix>/]<repo_id>[@<revision>][/<path_in_repo>]`
    ///
    /// Use this directly when you need access to `path_in_repo` separately
    /// from the config (e.g. to resolve a specific file within the repo).
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct HfUri {
        pub repo: HfRepo,
        pub path: String,
    }

    impl HfUri {
        /// Parse a Hugging Face path into its components.
        /// Path format: `[<repo_type>/]<repo_id>[@<revision>][/<path_in_repo>]`
        pub fn parse(path: &str) -> opendal_core::Result<Self> {
            if path.is_empty() {
                return Err(opendal_core::Error::new(
                    opendal_core::ErrorKind::ConfigInvalid,
                    "repo_id is required in uri path",
                )
                .with_context("service", HUGGINGFACE_SCHEME));
            }

            let mut path = path.to_string();

            // Strip repo_type prefix if present (e.g. "datasets/user/repo" → "user/repo")
            let repo_type = if let Some((first, rest)) = path.split_once('/') {
                if let Ok(rt) = HfRepoType::parse(first) {
                    path = rest.to_string();
                    rt
                } else {
                    HfRepoType::Model
                }
            } else if HfRepoType::parse(&path).is_ok() {
                return Err(opendal_core::Error::new(
                    opendal_core::ErrorKind::ConfigInvalid,
                    "repository name is required in uri path",
                )
                .with_context("service", HUGGINGFACE_SCHEME));
            } else {
                HfRepoType::Model
            };

            // Parse repo_id, revision, and path_in_repo.
            // Path is now: <repo_id>[@<revision>][/<path_in_repo>]
            let (repo_id, revision, path_in_repo) = if path.contains('/') {
                // Check if @ appears in the first two segments (the repo_id portion).
                // This distinguishes "user/repo@rev/file" from "user/repo/path/to/@file".
                let first_two: String = path.splitn(3, '/').take(2).collect::<Vec<_>>().join("/");

                if first_two.contains('@') {
                    let (repo_id, rev_and_path) = path.split_once('@').unwrap();
                    let rev_and_path = rev_and_path.replace("%2F", "/");
                    let (revision, path_in_repo) = Self::parse_revision(&rev_and_path);
                    (repo_id.to_string(), Some(revision), path_in_repo)
                } else {
                    let segments: Vec<_> = path.splitn(3, '/').collect();
                    let repo_id = format!("{}/{}", segments[0], segments[1]);
                    let path_in_repo = segments.get(2).copied().unwrap_or("").to_string();
                    (repo_id, None, path_in_repo)
                }
            } else if let Some((repo_id, rev)) = path.split_once('@') {
                let rev = rev.replace("%2F", "/");
                (
                    repo_id.to_string(),
                    if rev.is_empty() { None } else { Some(rev) },
                    String::new(),
                )
            } else {
                (path, None, String::new())
            };

            Ok(Self {
                repo: HfRepo::new(repo_type, repo_id, revision),
                path: path_in_repo,
            })
        }

        /// Split a string after `@` into (revision, path_in_repo).
        /// Handles special refs like `refs/convert/parquet` and `refs/pr/10`.
        fn parse_revision(rev_and_path: &str) -> (String, String) {
            if !rev_and_path.contains('/') {
                return (rev_and_path.to_string(), String::new());
            }

            // Match special refs: refs/(convert|pr)/<segment>
            if let Some(rest) = rev_and_path.strip_prefix("refs/convert/") {
                return if let Some(slash) = rest.find('/') {
                    (
                        rev_and_path[..14 + slash].to_string(),
                        rest[slash + 1..].to_string(),
                    )
                } else {
                    (rev_and_path.to_string(), String::new())
                };
            }
            if let Some(rest) = rev_and_path.strip_prefix("refs/pr/") {
                return if let Some(slash) = rest.find('/') {
                    let revision = format!("refs/pr/{}", &rest[..slash]);
                    (revision, rest[slash + 1..].to_string())
                } else {
                    (rev_and_path.to_string(), String::new())
                };
            }

            // Regular revision: split on first /
            let (rev, path) = rev_and_path.split_once('/').unwrap();
            (rev.to_string(), path.to_string())
        }

        /// Return the revision, defaulting to "main" if unset.
        pub fn revision(&self) -> &str {
            self.repo.revision()
        }

        /// Build the resolve URL for this URI using an explicit revision (e.g. a commit OID).
        ///
        /// Pinning to a specific commit OID avoids CDN consistency lag that can occur
        /// when using a branch name like "main" immediately after a commit.
        pub fn resolve_url(&self, endpoint: &str, revision: &str) -> String {
            let revision = percent_encode_revision(revision);
            let path = percent_encode_path(&self.path);
            match self.repo.repo_type {
                HfRepoType::Model => {
                    format!(
                        "{}/{}/resolve/{}/{}",
                        endpoint, &self.repo.repo_id, revision, path
                    )
                }
                HfRepoType::Dataset => {
                    format!(
                        "{}/datasets/{}/resolve/{}/{}",
                        endpoint, &self.repo.repo_id, revision, path
                    )
                }
                HfRepoType::Space => {
                    format!(
                        "{}/spaces/{}/resolve/{}/{}",
                        endpoint, &self.repo.repo_id, revision, path
                    )
                }
                HfRepoType::Bucket => {
                    format!(
                        "{}/buckets/{}/resolve/{}",
                        endpoint, &self.repo.repo_id, path
                    )
                }
            }
        }

        /// Build the paths-info API URL for this URI.
        pub fn paths_info_url(&self, endpoint: &str) -> String {
            self.repo.paths_info_url(endpoint)
        }

        /// Build the file tree API URL for this URI.
        pub fn file_tree_url(
            &self,
            endpoint: &str,
            recursive: bool,
            cursor: Option<&str>,
        ) -> String {
            let mut url = if self.repo.is_bucket() {
                format!(
                    "{}/api/buckets/{}/tree/{}?expand=True",
                    endpoint,
                    &self.repo.repo_id,
                    percent_encode_path(&self.path),
                )
            } else {
                format!(
                    "{}/api/{}/{}/tree/{}/{}?expand=True",
                    endpoint,
                    self.repo.repo_type.as_plural_str(),
                    &self.repo.repo_id,
                    percent_encode_revision(self.revision()),
                    percent_encode_path(&self.path),
                )
            };

            if recursive {
                url.push_str("&recursive=True");
            } else if self.repo.is_bucket() {
                // Bucket tree API defaults to recursive; must opt out explicitly.
                url.push_str("&recursive=false");
            }

            if let Some(cursor_val) = cursor {
                url.push_str(&format!("&cursor={}", cursor_val));
            }

            url
        }
    }

    pub(crate) fn percent_encode_revision(revision: &str) -> String {
        utf8_percent_encode(revision, NON_ALPHANUMERIC).to_string()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn resolve(path: &str) -> HfUri {
            HfUri::parse(path).unwrap()
        }

        #[test]
        fn test_repo_type_parse() {
            assert_eq!(HfRepoType::parse("models").unwrap(), HfRepoType::Model);
            assert_eq!(HfRepoType::parse("Models").unwrap(), HfRepoType::Model);
            assert_eq!(HfRepoType::parse("MODELS").unwrap(), HfRepoType::Model);
            assert_eq!(HfRepoType::parse("datasets").unwrap(), HfRepoType::Dataset);
            assert_eq!(HfRepoType::parse("Datasets").unwrap(), HfRepoType::Dataset);
            assert_eq!(HfRepoType::parse("spaces").unwrap(), HfRepoType::Space);
            assert_eq!(HfRepoType::parse("Spaces").unwrap(), HfRepoType::Space);
            assert_eq!(HfRepoType::parse("model").unwrap(), HfRepoType::Model);
            assert_eq!(HfRepoType::parse("dataset").unwrap(), HfRepoType::Dataset);
            assert_eq!(HfRepoType::parse("space").unwrap(), HfRepoType::Space);
            assert_eq!(HfRepoType::parse("data sets").unwrap(), HfRepoType::Dataset);
            assert_eq!(HfRepoType::parse("Data Sets").unwrap(), HfRepoType::Dataset);
            assert!(HfRepoType::parse("unknown").is_err());
            assert!(HfRepoType::parse("foobar").is_err());
        }

        #[test]
        fn resolve_with_namespace() {
            let p = resolve("username/my_model");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "username/my_model");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_with_revision() {
            let p = resolve("username/my_model@dev");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "username/my_model");
            assert_eq!(p.repo.revision.as_deref(), Some("dev"));
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_datasets_prefix() {
            let p = resolve("datasets/username/my_dataset");
            assert_eq!(p.repo.repo_type, HfRepoType::Dataset);
            assert_eq!(p.repo.repo_id, "username/my_dataset");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_datasets_prefix_and_revision() {
            let p = resolve("datasets/username/my_dataset@dev");
            assert_eq!(p.repo.repo_type, HfRepoType::Dataset);
            assert_eq!(p.repo.repo_id, "username/my_dataset");
            assert_eq!(p.repo.revision.as_deref(), Some("dev"));
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_with_path_in_repo() {
            let p = resolve("username/my_model/config.json");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "username/my_model");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "config.json");
        }

        #[test]
        fn resolve_with_revision_and_path() {
            let p = resolve("username/my_model@dev/path/to/file.txt");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "username/my_model");
            assert_eq!(p.repo.revision.as_deref(), Some("dev"));
            assert_eq!(p.path, "path/to/file.txt");
        }

        #[test]
        fn resolve_datasets_revision_and_path() {
            let p = resolve("datasets/username/my_dataset@dev/train/data.csv");
            assert_eq!(p.repo.repo_type, HfRepoType::Dataset);
            assert_eq!(p.repo.repo_id, "username/my_dataset");
            assert_eq!(p.repo.revision.as_deref(), Some("dev"));
            assert_eq!(p.path, "train/data.csv");
        }

        #[test]
        fn resolve_refs_convert_revision() {
            let p = resolve("datasets/squad@refs/convert/parquet");
            assert_eq!(p.repo.repo_type, HfRepoType::Dataset);
            assert_eq!(p.repo.repo_id, "squad");
            assert_eq!(p.repo.revision.as_deref(), Some("refs/convert/parquet"));
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_refs_pr_revision() {
            let p = resolve("username/my_model@refs/pr/10");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "username/my_model");
            assert_eq!(p.repo.revision.as_deref(), Some("refs/pr/10"));
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_encoded_revision() {
            let p = resolve("username/my_model@refs%2Fpr%2F10");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "username/my_model");
            assert_eq!(p.repo.revision.as_deref(), Some("refs/pr/10"));
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_at_in_path_not_revision() {
            let p = resolve("username/my_model/path/to/@not-a-revision.txt");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "username/my_model");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "path/to/@not-a-revision.txt");
        }

        #[test]
        fn resolve_bare_repo_type_fails() {
            assert!(HfUri::parse("datasets").is_err());
            assert!(HfUri::parse("").is_err());
        }

        #[test]
        fn resolve_bare_repo_no_namespace() {
            let p = resolve("gpt2");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "gpt2");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_bare_repo_with_revision() {
            let p = resolve("gpt2@dev");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "gpt2");
            assert_eq!(p.repo.revision.as_deref(), Some("dev"));
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_bare_dataset_no_namespace() {
            let p = resolve("datasets/squad");
            assert_eq!(p.repo.repo_type, HfRepoType::Dataset);
            assert_eq!(p.repo.repo_id, "squad");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_bare_dataset_with_revision() {
            let p = resolve("datasets/squad@dev");
            assert_eq!(p.repo.repo_type, HfRepoType::Dataset);
            assert_eq!(p.repo.repo_id, "squad");
            assert_eq!(p.repo.revision.as_deref(), Some("dev"));
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_models_prefix() {
            let p = resolve("models/username/my_model");
            assert_eq!(p.repo.repo_type, HfRepoType::Model);
            assert_eq!(p.repo.repo_id, "username/my_model");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_spaces_prefix() {
            let p = resolve("spaces/username/my_space");
            assert_eq!(p.repo.repo_type, HfRepoType::Space);
            assert_eq!(p.repo.repo_id, "username/my_space");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_buckets_prefix() {
            let p = resolve("buckets/username/my_bucket");
            assert_eq!(p.repo.repo_type, HfRepoType::Bucket);
            assert_eq!(p.repo.repo_id, "username/my_bucket");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "");
        }

        #[test]
        fn resolve_buckets_with_path() {
            let p = resolve("buckets/username/my_bucket/data/file.txt");
            assert_eq!(p.repo.repo_type, HfRepoType::Bucket);
            assert_eq!(p.repo.repo_id, "username/my_bucket");
            assert!(p.repo.revision.is_none());
            assert_eq!(p.path, "data/file.txt");
        }

        #[test]
        fn test_bucket_resolve_url() {
            let p = resolve("buckets/user/bucket/file.txt");
            let url = p.resolve_url("https://huggingface.co", p.revision());
            assert_eq!(
                url,
                "https://huggingface.co/buckets/user/bucket/resolve/file.txt"
            );
        }

        #[test]
        fn test_bucket_xet_token_urls() {
            let p = resolve("buckets/user/bucket");
            let read_url = p.repo.xet_token_url("https://huggingface.co", "read");
            let write_url = p.repo.xet_token_url("https://huggingface.co", "write");
            assert_eq!(
                read_url,
                "https://huggingface.co/api/buckets/user/bucket/xet-read-token"
            );
            assert_eq!(
                write_url,
                "https://huggingface.co/api/buckets/user/bucket/xet-write-token"
            );
        }

        #[test]
        fn test_bucket_batch_url() {
            let p = resolve("buckets/user/bucket");
            let url = p.repo.bucket_batch_url("https://huggingface.co");
            assert_eq!(url, "https://huggingface.co/api/buckets/user/bucket/batch");
        }
    }
}

pub(super) use uri::*;
