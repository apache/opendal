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
use serde::{Deserialize, Serialize};

use xet::xet_session::{XetDownloadStreamGroup, XetSession, XetSessionBuilder, XetUploadCommit};

use opendal_core::raw::*;
use opendal_core::*;

use super::HUGGINGFACE_SCHEME;
use super::error::parse_error;
use super::uri::{HfRepo, HfUri};

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
    pub info: Arc<AccessorInfo>,
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
    pub fn new(
        info: Arc<AccessorInfo>,
        repo: HfRepo,
        root: String,
        token: Option<String>,
        endpoint: String,
        xet_session: XetSession,
        download_mode: HfDownloadMode,
    ) -> Self {
        Self {
            info,
            repo,
            root,
            token,
            endpoint,
            xet_session,
            download_mode,
        }
    }

    pub fn build(
        info: Arc<AccessorInfo>,
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
        if let Some(token) = &self.token {
            if let Ok(val) = format!("Bearer {}", token).parse() {
                headers.insert(header::AUTHORIZATION, val);
            }
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
    ) -> Result<http::request::Builder> {
        let mut req = Request::builder().method(method).uri(url).extension(op);
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
    async fn send(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        let resp = self.info.http_client().fetch(req).await?;
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
        req: Request<Buffer>,
    ) -> Result<(http::response::Parts, T)> {
        let (parts, mut body) = self.send(req).await?.into_parts();
        let buffer = body.to_buffer().await?;
        let parsed =
            serde_json::from_reader(buffer.reader()).map_err(new_json_deserialize_error)?;
        Ok((parts, parsed))
    }

    pub(super) async fn path_info(&self, path: &str) -> Result<PathInfo> {
        let uri = self.uri(path);
        let url = uri.paths_info_url(&self.endpoint);
        let form_body = format!("paths={}&expand=True", percent_encode_path(&uri.path));

        let req = self
            .request(http::Method::POST, &url, Operation::Stat)?
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

    /// Send `GET /resolve` and return the raw streaming response.
    ///
    /// In `Xet` mode adds `Accept: application/vnd.xet-fileinfo+json` so the
    /// server returns XET metadata instead of redirecting; in `Http` mode the
    /// redirect is followed and the file bytes are streamed directly.
    pub(super) async fn resolve(
        &self,
        path: &str,
        range: BytesRange,
        mode: HfDownloadMode,
    ) -> Result<Response<HttpBody>> {
        let uri = self.uri(path);
        let url = uri.resolve_url(&self.endpoint, self.repo.revision());

        let mut req = self.request(http::Method::GET, &url, Operation::Read)?;

        if mode == HfDownloadMode::Xet {
            req = req.header(header::ACCEPT, "application/vnd.xet-fileinfo+json");
        }

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        let resp = self.info.http_client().fetch(req).await?;

        if !resp.status().is_success() {
            // Drop the streaming body without reading it — parse_error reads
            // only response headers, so there is no need to buffer the body
            // (which may be a large HTML error page).
            let (parts, _) = resp.into_parts();
            return Err(parse_error(parts));
        }

        Ok(resp)
    }

    /// Commit file changes to a git-based repo (model/dataset/space).
    ///
    /// Counterpart of [`commit_bucket`](Self::commit_bucket) for bucket repos.
    pub(super) async fn commit_git(
        &self,
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
            .request(http::Method::POST, &url, Operation::Write)?
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, json_body.len())
            .body(Buffer::from(json_body))
            .map_err(new_request_build_error)?;

        let (_, resp) = self.send_parse::<CommitResponse>(req).await?;
        Ok(resp)
    }

    /// Commit file changes to a bucket repo via the NDJSON batch API.
    ///
    /// Counterpart of [`commit_git`](Self::commit_git) for git-based repos.
    pub(super) async fn commit_bucket(&self, operations: Vec<BucketOperation>) -> Result<()> {
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
            .request(http::Method::POST, &url, Operation::Write)?
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

    use super::super::uri::HfRepoType;
    use super::*;

    #[derive(Clone)]
    pub(crate) struct MockHttpClient {
        url: Arc<Mutex<Option<String>>>,
        body: Arc<Mutex<Option<String>>>,
    }

    impl MockHttpClient {
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

    impl HttpFetch for MockHttpClient {
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
    ) -> (HfCore, MockHttpClient) {
        let mock_client = MockHttpClient::new();
        let http_client = HttpClient::with(mock_client.clone());

        let info = AccessorInfo::default();
        info.set_scheme("hf")
            .set_native_capability(Capability::default());
        info.update_http_client(|_| http_client);

        let xet_session = XetSessionBuilder::new()
            .build()
            .expect("failed to create xet session");
        let core = HfCore::new(
            Arc::new(info),
            HfRepo::new(repo_type, repo_id.to_string(), Some(revision.to_string())),
            "/".to_string(),
            None,
            endpoint.to_string(),
            xet_session,
            HfDownloadMode::Xet,
        );

        (core, mock_client)
    }
}

#[cfg(test)]
mod tests {
    use super::super::uri::HfRepoType;
    use super::test_utils::create_test_core;
    use super::*;

    #[tokio::test]
    async fn test_hf_path_info_url_model() -> Result<()> {
        let (core, mock_client) = create_test_core(
            HfRepoType::Model,
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
            HfRepoType::Dataset,
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
            HfRepoType::Model,
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
            HfRepoType::Space,
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
