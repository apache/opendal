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

use bytes::Bytes;
use http::Request;
use http::Response;
use http::header;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;

use super::backend::RepoType;
use crate::raw::*;
use crate::*;

fn percent_encode_revision(revision: &str) -> String {
    utf8_percent_encode(revision, NON_ALPHANUMERIC).to_string()
}

pub struct HuggingfaceCore {
    pub info: Arc<AccessorInfo>,

    pub repo_type: RepoType,
    pub repo_id: String,
    pub revision: String,
    pub root: String,
    pub token: Option<String>,
    pub endpoint: String,
}

impl Debug for HuggingfaceCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HuggingfaceCore")
            .field("repo_type", &self.repo_type)
            .field("repo_id", &self.repo_id)
            .field("revision", &self.revision)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl HuggingfaceCore {
    pub async fn hf_path_info(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = match self.repo_type {
            RepoType::Model => format!(
                "{}/api/models/{}/paths-info/{}",
                &self.endpoint,
                &self.repo_id,
                percent_encode_revision(&self.revision)
            ),
            RepoType::Dataset => format!(
                "{}/api/datasets/{}/paths-info/{}",
                &self.endpoint,
                &self.repo_id,
                percent_encode_revision(&self.revision)
            ),
        };

        let mut req = Request::post(&url);
        // Inject operation to the request.
        req = req.extension(Operation::Stat);
        if let Some(token) = &self.token {
            let auth_header_content = format_authorization_by_bearer(token)?;
            req = req.header(header::AUTHORIZATION, auth_header_content);
        }

        req = req.header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");

        let req_body = format!("paths={}&expand=True", percent_encode_path(&p));

        let req = req
            .body(Buffer::from(Bytes::from(req_body)))
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn hf_list(&self, path: &str, recursive: bool) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = match self.repo_type {
            RepoType::Model => format!(
                "{}/api/models/{}/tree/{}/{}?expand=True",
                &self.endpoint,
                &self.repo_id,
                percent_encode_revision(&self.revision),
                percent_encode_path(&p)
            ),
            RepoType::Dataset => format!(
                "{}/api/datasets/{}/tree/{}/{}?expand=True",
                &self.endpoint,
                &self.repo_id,
                percent_encode_revision(&self.revision),
                percent_encode_path(&p)
            ),
        };

        if recursive {
            url.push_str("&recursive=True");
        }

        let mut req = Request::get(&url);
        // Inject operation to the request.
        req = req.extension(Operation::List);
        if let Some(token) = &self.token {
            let auth_header_content = format_authorization_by_bearer(token)?;
            req = req.header(header::AUTHORIZATION, auth_header_content);
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn hf_resolve(
        &self,
        path: &str,
        range: BytesRange,
        _args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = match self.repo_type {
            RepoType::Model => format!(
                "{}/{}/resolve/{}/{}",
                &self.endpoint,
                &self.repo_id,
                percent_encode_revision(&self.revision),
                percent_encode_path(&p)
            ),
            RepoType::Dataset => format!(
                "{}/datasets/{}/resolve/{}/{}",
                &self.endpoint,
                &self.repo_id,
                percent_encode_revision(&self.revision),
                percent_encode_path(&p)
            ),
        };

        let mut req = Request::get(&url);

        if let Some(token) = &self.token {
            let auth_header_content = format_authorization_by_bearer(token)?;
            req = req.header(header::AUTHORIZATION, auth_header_content);
        }

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }
        // Inject operation to the request.
        let req = req.extension(Operation::Read);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().fetch(req).await
    }
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingfaceStatus {
    #[serde(rename = "type")]
    pub type_: String,
    pub oid: String,
    pub size: u64,
    pub lfs: Option<HuggingfaceLfs>,
    pub path: String,
    pub last_commit: Option<HuggingfaceLastCommit>,
    pub security: Option<HuggingfaceSecurity>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingfaceLfs {
    pub oid: String,
    pub size: u64,
    pub pointer_size: u64,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingfaceLastCommit {
    pub id: String,
    pub title: String,
    pub date: String,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingfaceSecurity {
    pub blob_id: String,
    pub safe: bool,
    pub av_scan: Option<HuggingfaceAvScan>,
    pub pickle_import_scan: Option<HuggingfacePickleImportScan>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct HuggingfaceAvScan {
    pub virus_found: bool,
    pub virus_names: Option<Vec<String>>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingfacePickleImportScan {
    pub highest_safety_level: String,
    pub imports: Vec<HuggingfaceImport>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[allow(dead_code)]
pub(super) struct HuggingfaceImport {
    pub module: String,
    pub name: String,
    pub safety: String,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::raw::new_json_deserialize_error;
    use crate::types::Result;
    use http::{Request, Response, StatusCode};
    use std::sync::{Arc, Mutex};

    // Mock HTTP client that captures the request URL and headers
    #[derive(Clone)]
    struct MockHttpClient {
        url: Arc<Mutex<Option<String>>>,
        headers: Arc<Mutex<Option<http::HeaderMap>>>,
    }

    impl MockHttpClient {
        fn new() -> Self {
            Self {
                url: Arc::new(Mutex::new(None)),
                headers: Arc::new(Mutex::new(None)),
            }
        }

        fn get_captured_url(&self) -> String {
            self.url.lock().unwrap().clone().unwrap()
        }

        fn get_captured_headers(&self) -> http::HeaderMap {
            self.headers.lock().unwrap().clone().unwrap()
        }
    }

    impl HttpFetch for MockHttpClient {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            // Capture the URL and headers
            *self.url.lock().unwrap() = Some(req.uri().to_string());
            *self.headers.lock().unwrap() = Some(req.headers().clone());

            // Return a mock response with empty body
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        }
    }

    /// Utility function to create HuggingfaceCore with mocked HTTP client
    fn create_test_core(
        repo_type: RepoType,
        repo_id: &str,
        revision: &str,
        endpoint: &str,
    ) -> (HuggingfaceCore, MockHttpClient) {
        let mock_client = MockHttpClient::new();
        let http_client = HttpClient::with(mock_client.clone());

        let info = AccessorInfo::default();
        info.set_scheme("huggingface")
            .set_native_capability(Capability::default());
        info.update_http_client(|_| http_client);

        let core = HuggingfaceCore {
            info: Arc::new(info),
            repo_type,
            repo_id: repo_id.to_string(),
            revision: revision.to_string(),
            root: "/".to_string(),
            token: None,
            endpoint: endpoint.to_string(),
        };

        (core, mock_client)
    }

    #[tokio::test]
    async fn test_hf_path_info_url_model() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );

        core.hf_path_info("test.txt").await?;

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

        core.hf_path_info("data/file.csv").await?;

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

        core.hf_path_info("model.bin").await?;

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

        core.hf_list("path1", false).await?;

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

        core.hf_list("path2", true).await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/api/models/org/model/tree/main/path2?expand=True&recursive=True"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_resolve_url_model() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Model,
            "user/model",
            "main",
            "https://huggingface.co",
        );

        let args = OpRead::default();
        core.hf_resolve("config.json", BytesRange::default(), &args)
            .await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/user/model/resolve/main/config.json"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_resolve_url_dataset() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Dataset,
            "org/data",
            "v1.0",
            "https://huggingface.co",
        );

        let args = OpRead::default();
        core.hf_resolve("train.csv", BytesRange::default(), &args)
            .await?;

        let url = mock_client.get_captured_url();
        assert_eq!(
            url,
            "https://huggingface.co/datasets/org/data/resolve/v1%2E0/train.csv"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hf_resolve_with_range() -> Result<()> {
        let (core, mock_client) = create_test_core(
            RepoType::Model,
            "user/model",
            "main",
            "https://huggingface.co",
        );

        let args = OpRead::default();
        let range = BytesRange::new(0, Some(1024));
        core.hf_resolve("large_file.bin", range, &args).await?;

        let url = mock_client.get_captured_url();
        let headers = mock_client.get_captured_headers();
        assert_eq!(
            url,
            "https://huggingface.co/user/model/resolve/main/large_file.bin"
        );
        assert_eq!(headers.get(http::header::RANGE).unwrap(), "bytes=0-1023");

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

        let decoded_response = serde_json::from_slice::<Vec<HuggingfaceStatus>>(&resp)
            .map_err(new_json_deserialize_error)?;

        assert_eq!(decoded_response.len(), 2);

        let file_entry = HuggingfaceStatus {
            type_: "file".to_string(),
            oid: "45fa7c3d85ee7dd4139adbc056da25ae136a65f2".to_string(),
            size: 69512435,
            lfs: Some(HuggingfaceLfs {
                oid: "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c".to_string(),
                size: 69512435,
                pointer_size: 133,
            }),
            path: "maelstrom/lib/maelstrom.jar".to_string(),
            last_commit: None,
            security: None,
        };

        assert_eq!(decoded_response[0], file_entry);

        let dir_entry = HuggingfaceStatus {
            type_: "directory".to_string(),
            oid: "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c".to_string(),
            size: 69512435,
            lfs: None,
            path: "maelstrom/lib/plugins".to_string(),
            last_commit: None,
            security: None,
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

        let decoded_response = serde_json::from_slice::<Vec<HuggingfaceStatus>>(&resp)
            .map_err(new_json_deserialize_error)?;

        assert_eq!(decoded_response.len(), 1);

        let file_info = HuggingfaceStatus {
            type_: "file".to_string(),
            oid: "45fa7c3d85ee7dd4139adbc056da25ae136a65f2".to_string(),
            size: 69512435,
            lfs: Some(HuggingfaceLfs {
                oid: "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c".to_string(),
                size: 69512435,
                pointer_size: 133,
            }),
            path: "maelstrom/lib/maelstrom.jar".to_string(),
            last_commit: Some(HuggingfaceLastCommit {
                id: "bc1ef030bf3743290d5e190695ab94582e51ae2f".to_string(),
                title: "Upload 141 files".to_string(),
                date: "2023-11-17T23:50:28.000Z".to_string(),
            }),
            security: Some(HuggingfaceSecurity {
                blob_id: "45fa7c3d85ee7dd4139adbc056da25ae136a65f2".to_string(),
                safe: true,
                av_scan: Some(HuggingfaceAvScan {
                    virus_found: false,
                    virus_names: None,
                }),
                pickle_import_scan: Some(HuggingfacePickleImportScan {
                    highest_safety_level: "innocuous".to_string(),
                    imports: vec![
                        HuggingfaceImport {
                            module: "torch".to_string(),
                            name: "FloatStorage".to_string(),
                            safety: "innocuous".to_string(),
                        },
                        HuggingfaceImport {
                            module: "collections".to_string(),
                            name: "OrderedDict".to_string(),
                            safety: "innocuous".to_string(),
                        },
                        HuggingfaceImport {
                            module: "torch".to_string(),
                            name: "LongStorage".to_string(),
                            safety: "innocuous".to_string(),
                        },
                        HuggingfaceImport {
                            module: "torch._utils".to_string(),
                            name: "_rebuild_tensor_v2".to_string(),
                            safety: "innocuous".to_string(),
                        },
                    ],
                }),
            }),
        };

        assert_eq!(decoded_response[0], file_info);

        Ok(())
    }
}
