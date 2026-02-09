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
use http::header;
use serde::Deserialize;

#[cfg(feature = "xet")]
use xet_utils::auth::TokenRefresher;

use super::error::parse_error;
use super::uri::HfRepo;
use opendal_core::raw::*;
use opendal_core::*;

/// API payload structures for preupload operations
#[derive(serde::Serialize)]
pub(super) struct PreuploadFile {
    pub path: String,
    pub size: u64,
    pub sample: String,
    #[serde(rename = "sha256")]
    pub sha256: String,
}

#[derive(serde::Serialize)]
pub(super) struct PreuploadRequest {
    pub files: Vec<PreuploadFile>,
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

#[derive(serde::Serialize)]
pub(super) struct MixedCommitPayload {
    pub summary: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<CommitFile>,
    #[serde(rename = "lfsFiles", skip_serializing_if = "Vec::is_empty")]
    pub lfs_files: Vec<LfsFile>,
}

// API response types

#[derive(serde::Deserialize, Debug)]
pub(super) struct PreuploadFileResponse {
    #[allow(dead_code)]
    pub path: String,
    #[serde(rename = "uploadMode")]
    pub upload_mode: String,
}

#[derive(serde::Deserialize, Debug)]
pub(super) struct PreuploadResponse {
    pub files: Vec<PreuploadFileResponse>,
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

/// Response from the tree/list API endpoint
#[derive(Debug)]
pub(super) struct FileTree {
    pub files: Vec<PathInfo>,
    pub next_cursor: Option<String>,
}

#[cfg(feature = "xet")]
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct XetToken {
    pub access_token: String,
    pub cas_url: String,
    pub exp: u64,
}

#[cfg(feature = "xet")]
#[derive(Clone, Debug)]
pub(super) struct XetFile {
    pub hash: String,
    pub size: u64,
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

    #[cfg(feature = "xet")]
    pub xet_enabled: bool,
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

/// Extract the cursor value from a Link header's "next" URL.
fn parse_next_cursor(link_str: &str) -> Option<String> {
    for link in link_str.split(',') {
        if link.contains("rel=\"next\"") || link.contains("rel='next'") {
            let (_, rest) = link.split_once('<')?;
            let (url, _) = rest.split_once('>')?;
            let query = url.split_once('?')?.1;
            return query
                .split('&')
                .find_map(|p| p.strip_prefix("cursor="))
                .map(|v| v.to_string());
        }
    }
    None
}

impl HfCore {
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

    /// Send a request, check for success, and deserialize the JSON response.
    ///
    /// Returns the response parts (status, headers, etc.) alongside the
    /// deserialized body so callers can inspect headers when needed.
    async fn send_request<T: serde::de::DeserializeOwned>(
        &self,
        req: Request<Buffer>,
    ) -> Result<(http::response::Parts, T)> {
        let resp = self.info.http_client().send(req).await?;
        if !resp.status().is_success() {
            return Err(parse_error(resp));
        }
        let (parts, body) = resp.into_parts();
        let parsed = serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;
        Ok((parts, parsed))
    }

    pub async fn path_info(&self, path: &str) -> Result<PathInfo> {
        let uri = self.repo.uri(&self.root, path);
        let url = uri.paths_info_url(&self.endpoint);
        let form_body = format!("paths={}&expand=True", percent_encode_path(&uri.path));

        let req = self
            .request(http::Method::POST, &url, Operation::Stat)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(Bytes::from(form_body)))
            .map_err(new_request_build_error)?;
        let (_, mut files) = self.send_request::<Vec<PathInfo>>(req).await?;

        // NOTE: if the file is not found, the server will return 200 with an empty array
        if files.is_empty() {
            return Err(Error::new(ErrorKind::NotFound, "path not found"));
        }

        Ok(files.remove(0))
    }

    pub async fn file_tree(
        &self,
        path: &str,
        recursive: bool,
        cursor: Option<&str>,
    ) -> Result<FileTree> {
        let uri = self.repo.uri(&self.root, path);
        let url = uri.file_tree_url(&self.endpoint, recursive, cursor);

        let req = self
            .request(http::Method::GET, &url, Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let (parts, files) = self.send_request::<Vec<PathInfo>>(req).await?;

        let next_cursor = parts
            .headers
            .get(http::header::LINK)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_next_cursor);

        Ok(FileTree { files, next_cursor })
    }

    #[cfg(feature = "xet")]
    pub(super) async fn get_xet_token(&self, token_type: &str) -> Result<XetToken> {
        let url = self.repo.xet_token_url(&self.endpoint, token_type);
        let req = self
            .request(http::Method::GET, &url, Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let (_, token) = self.send_request(req).await?;
        Ok(token)
    }

    /// Issue a HEAD request and extract XET file info (hash and size).
    ///
    /// Uses a custom HTTP client that does NOT follow redirects so we can
    /// inspect response headers (e.g. `X-Xet-Hash`) from the 302 response.
    ///
    /// Returns `None` if the `X-Xet-Hash` header is absent or empty.
    #[cfg(feature = "xet")]
    pub(super) async fn get_xet_file(&self, path: &str) -> Result<Option<XetFile>> {
        let uri = self.repo.uri(&self.root, path);
        let url = uri.resolve_url(&self.endpoint);

        let reqwest_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "failed to build http client").set_source(err)
            })?;
        let client = HttpClient::with(reqwest_client);

        let req = self
            .request(http::Method::HEAD, &url, Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = client.send(req).await?;

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

        Ok(Some(XetFile {
            hash: hash.to_string(),
            size,
        }))
    }

    /// Call the preupload API to determine upload strategy for files.
    pub(super) async fn preupload_files(
        &self,
        files: Vec<PreuploadFile>,
    ) -> Result<PreuploadResponse> {
        let _token = self.token.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::PermissionDenied,
                "token is required for write operations",
            )
            .with_operation("preupload")
        })?;

        let first_path = files
            .first()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "no files to preupload"))?;

        let uri = self.repo.uri(&self.root, &first_path.path);
        let url = uri.preupload_url(&self.endpoint);

        let payload = PreuploadRequest { files };
        let json_body = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;

        let req = self
            .request(http::Method::POST, &url, Operation::Write)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Buffer::from(json_body))
            .map_err(new_request_build_error)?;

        let (_, resp) = self.send_request(req).await?;
        Ok(resp)
    }

    /// Commit uploaded files to the repository.
    pub(super) async fn commit_files(
        &self,
        regular_files: Vec<CommitFile>,
        lfs_files: Vec<LfsFile>,
    ) -> Result<http::Response<Buffer>> {
        let _token = self.token.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::PermissionDenied,
                "token is required for write operations",
            )
            .with_operation("commit")
        })?;

        let mut summary_paths = Vec::new();
        for file in &regular_files {
            summary_paths.push(file.path.clone());
        }
        for file in &lfs_files {
            summary_paths.push(file.path.clone());
        }

        let summary = if summary_paths.len() == 1 {
            format!("Upload {} via OpenDAL", summary_paths[0])
        } else {
            format!("Upload {} files via OpenDAL", summary_paths.len())
        };

        let client = self.info.http_client();
        // Use the first file's path to determine the commit URL
        let first_path = summary_paths
            .first()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "no files to commit"))?;
        let uri = self.repo.uri(&self.root, first_path);
        let url = uri.commit_url(&self.endpoint);

        let payload = MixedCommitPayload {
            summary,
            files: regular_files,
            lfs_files,
        };

        let json_body = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;

        let req = self
            .request(http::Method::POST, &url, Operation::Write)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, json_body.len())
            .body(Buffer::from(json_body))
            .map_err(new_request_build_error)?;

        client.send(req).await
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

        let core = HfCore {
            info: Arc::new(info),
            repo: HfRepo::new(repo_type, repo_id.to_string(), Some(revision.to_string())),
            root: "/".to_string(),
            token: None,
            endpoint: endpoint.to_string(),
            #[cfg(feature = "xet")]
            xet_enabled: false,
        };

        (core, mock_client)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

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
    async fn test_hf_list_url_non_recursive() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Model,
            "org/model",
            "main",
            "https://huggingface.co",
        );

        core.file_tree("path1", false, None).await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/models/org/model/tree/main/path1?expand=True"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_list_url_recursive() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Model,
            "org/model",
            "main",
            "https://huggingface.co",
        );

        core.file_tree("path2", true, None).await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/models/org/model/tree/main/path2?expand=True&recursive=True"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_list_url_with_cursor() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Model,
            "org/model",
            "main",
            "https://huggingface.co",
        );

        core.file_tree("path3", false, Some("abc123")).await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/models/org/model/tree/main/path3?expand=True&cursor=abc123"
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

    #[tokio::test]
    async fn test_hf_list_url_space() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Space,
            "org/space",
            "main",
            "https://huggingface.co",
        );

        core.file_tree("static", false, None).await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/spaces/org/space/tree/main/static?expand=True"
        );

        Ok(())
    }

    #[test]
    fn parse_list_response_test() -> Result<()> {
        let resp = Bytes::from(
            r#"
            [
                {
                    "type": "file",
                    "oid": "45fa7c3d85ee7dd4139adbc056da25ae136a65f2",
                    "size": 69512435,
                    "lfs": {
                        "oid": "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c",
                        "size": 69512435,
                        "pointerSize": 133
                    },
                    "path": "maelstrom/lib/maelstrom.jar"
                },
                {
                    "type": "directory",
                    "oid": "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c",
                    "size": 69512435,
                    "path": "maelstrom/lib/plugins"
                }
            ]
            "#,
        );

        let decoded_response =
            serde_json::from_slice::<Vec<PathInfo>>(&resp).map_err(new_json_deserialize_error)?;

        assert_eq!(decoded_response.len(), 2);

        let file_entry = PathInfo {
            type_: "file".to_string(),
            oid: "45fa7c3d85ee7dd4139adbc056da25ae136a65f2".to_string(),
            size: 69512435,
            lfs: Some(LfsInfo {
                oid: "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c".to_string(),
            }),
            path: "maelstrom/lib/maelstrom.jar".to_string(),
            last_commit: None,
        };

        assert_eq!(decoded_response[0], file_entry);

        let dir_entry = PathInfo {
            type_: "directory".to_string(),
            oid: "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c".to_string(),
            size: 69512435,
            lfs: None,
            path: "maelstrom/lib/plugins".to_string(),
            last_commit: None,
        };

        assert_eq!(decoded_response[1], dir_entry);

        Ok(())
    }

    #[test]
    fn parse_files_info_test() -> Result<()> {
        let resp = Bytes::from(
            r#"
            [
                {
                    "type": "file",
                    "oid": "45fa7c3d85ee7dd4139adbc056da25ae136a65f2",
                    "size": 69512435,
                    "lfs": {
                        "oid": "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c",
                        "size": 69512435,
                        "pointerSize": 133
                    },
                    "path": "maelstrom/lib/maelstrom.jar",
                    "lastCommit": {
                        "id": "bc1ef030bf3743290d5e190695ab94582e51ae2f",
                        "title": "Upload 141 files",
                        "date": "2023-11-17T23:50:28.000Z"
                    },
                    "security": {
                        "blobId": "45fa7c3d85ee7dd4139adbc056da25ae136a65f2",
                        "name": "maelstrom/lib/maelstrom.jar",
                        "safe": true,
                        "avScan": {
                            "virusFound": false,
                            "virusNames": null
                        },
                        "pickleImportScan": {
                            "highestSafetyLevel": "innocuous",
                            "imports": [
                                {"module": "torch", "name": "FloatStorage", "safety": "innocuous"},
                                {"module": "collections", "name": "OrderedDict", "safety": "innocuous"},
                                {"module": "torch", "name": "LongStorage", "safety": "innocuous"},
                                {"module": "torch._utils", "name": "_rebuild_tensor_v2", "safety": "innocuous"}
                            ]
                        }
                    }
                }
            ]
            "#,
        );

        let decoded_response =
            serde_json::from_slice::<Vec<PathInfo>>(&resp).map_err(new_json_deserialize_error)?;

        assert_eq!(decoded_response.len(), 1);

        let file_info = PathInfo {
            type_: "file".to_string(),
            oid: "45fa7c3d85ee7dd4139adbc056da25ae136a65f2".to_string(),
            size: 69512435,
            lfs: Some(LfsInfo {
                oid: "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c".to_string(),
            }),
            path: "maelstrom/lib/maelstrom.jar".to_string(),
            last_commit: Some(LastCommit {
                date: "2023-11-17T23:50:28.000Z".to_string(),
            }),
        };

        assert_eq!(decoded_response[0], file_info);

        Ok(())
    }
}

#[cfg(feature = "xet")]
pub(super) fn map_xet_error(err: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Unexpected, "xet operation failed").set_source(err)
}
