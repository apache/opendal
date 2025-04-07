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

use bytes::Bytes;
use http::header;
use http::Request;
use http::Response;
use serde::Deserialize;
use std::fmt::Debug;
use std::sync::Arc;

use super::backend::RepoType;
use crate::raw::*;
use crate::*;

pub struct HuggingfaceCore {
    pub info: Arc<AccessorInfo>,

    pub repo_type: RepoType,
    pub repo_id: String,
    pub revision: String,
    pub root: String,
    pub token: Option<String>,
}

impl Debug for HuggingfaceCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HuggingfaceCore")
            .field("repo_type", &self.repo_type)
            .field("repo_id", &self.repo_id)
            .field("revision", &self.revision)
            .field("root", &self.root)
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
                "https://huggingface.co/api/models/{}/paths-info/{}",
                &self.repo_id, &self.revision
            ),
            RepoType::Dataset => format!(
                "https://huggingface.co/api/datasets/{}/paths-info/{}",
                &self.repo_id, &self.revision
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
                "https://huggingface.co/api/models/{}/tree/{}/{}?expand=True",
                &self.repo_id,
                &self.revision,
                percent_encode_path(&p)
            ),
            RepoType::Dataset => format!(
                "https://huggingface.co/api/datasets/{}/tree/{}/{}?expand=True",
                &self.repo_id,
                &self.revision,
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
                "https://huggingface.co/{}/resolve/{}/{}",
                &self.repo_id,
                &self.revision,
                percent_encode_path(&p)
            ),
            RepoType::Dataset => format!(
                "https://huggingface.co/datasets/{}/resolve/{}/{}",
                &self.repo_id,
                &self.revision,
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
