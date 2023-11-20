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

//! HuggingFace response messages

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingFaceStatus {
    #[serde(rename = "type")]
    pub type_: String,
    pub oid: String,
    pub size: u64,
    pub lfs: Option<HuggingFaceLfs>,
    pub path: String,
    pub last_commit: Option<HuggingFaceLastCommit>,
    pub security: Option<HuggingFaceSecurity>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingFaceLfs {
    pub oid: String,
    pub size: u64,
    pub pointer_size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingFaceLastCommit {
    pub id: String,
    pub title: String,
    pub date: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingFaceSecurity {
    pub blob_id: String,
    pub name: String,
    pub safe: bool,
    pub av_scan: Option<HuggingFaceAvScan>,
    pub pickle_import_scan: Option<HuggingFacePickleImportScan>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct HuggingFaceAvScan {
    pub virus_found: bool,
    pub virus_names: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(super) struct HuggingFacePickleImportScan {
    pub highest_safety_level: String,
    pub imports: Vec<HuggingFaceImport>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub(super) struct HuggingFaceImport {
    pub module: String,
    pub name: String,
    pub safety: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::new_json_deserialize_error;
    use crate::types::Result;
    use bytes::Bytes;

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

        let decoded_response = serde_json::from_slice::<Vec<HuggingFaceStatus>>(&resp)
            .map_err(new_json_deserialize_error)?;

        assert_eq!(decoded_response.len(), 2);

        assert_eq!(decoded_response[0].type_, "file");
        assert_eq!(
            decoded_response[0].oid,
            "45fa7c3d85ee7dd4139adbc056da25ae136a65f2"
        );
        assert_eq!(decoded_response[0].size, 69512435);
        assert_eq!(decoded_response[0].path, "maelstrom/lib/maelstrom.jar");

        assert_eq!(
            decoded_response[0].lfs.as_ref().unwrap().oid,
            "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c"
        );
        assert_eq!(decoded_response[0].lfs.as_ref().unwrap().size, 69512435);
        assert_eq!(decoded_response[0].lfs.as_ref().unwrap().pointer_size, 133);

        assert_eq!(decoded_response[1].type_, "directory");
        assert_eq!(
            decoded_response[1].oid,
            "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c"
        );
        assert_eq!(decoded_response[1].size, 69512435);
        assert_eq!(decoded_response[1].path, "maelstrom/lib/plugins");

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

        let decoded_response = serde_json::from_slice::<Vec<HuggingFaceStatus>>(&resp)
            .map_err(new_json_deserialize_error)?;

        assert_eq!(decoded_response.len(), 1);

        assert_eq!(decoded_response[0].type_, "file");
        assert_eq!(
            decoded_response[0].oid,
            "45fa7c3d85ee7dd4139adbc056da25ae136a65f2"
        );
        assert_eq!(decoded_response[0].size, 69512435);
        assert_eq!(decoded_response[0].path, "maelstrom/lib/maelstrom.jar");

        assert_eq!(
            decoded_response[0].lfs.as_ref().unwrap().oid,
            "b43f4c2ea569da1d66ca74e26ca8ea4430dfc29195e97144b2d0b4f3f6cafa1c"
        );

        assert_eq!(decoded_response[0].lfs.as_ref().unwrap().size, 69512435);
        assert_eq!(decoded_response[0].lfs.as_ref().unwrap().pointer_size, 133);

        assert_eq!(
            decoded_response[0].last_commit.as_ref().unwrap().id,
            "bc1ef030bf3743290d5e190695ab94582e51ae2f"
        );
        assert_eq!(
            decoded_response[0].last_commit.as_ref().unwrap().title,
            "Upload 141 files"
        );
        assert_eq!(
            decoded_response[0].last_commit.as_ref().unwrap().date,
            "2023-11-17T23:50:28.000Z"
        );

        assert_eq!(
            decoded_response[0].security.as_ref().unwrap().blob_id,
            "45fa7c3d85ee7dd4139adbc056da25ae136a65f2"
        );
        assert_eq!(
            decoded_response[0].security.as_ref().unwrap().name,
            "maelstrom/lib/maelstrom.jar"
        );
        assert_eq!(decoded_response[0].security.as_ref().unwrap().safe, true);

        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .av_scan
                .as_ref()
                .unwrap()
                .virus_found,
            false
        );
        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .av_scan
                .as_ref()
                .unwrap()
                .virus_names,
            None
        );

        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .highest_safety_level,
            "innocuous"
        );

        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[0]
                .module,
            "torch"
        );
        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[0]
                .name,
            "FloatStorage"
        );
        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[0]
                .safety,
            "innocuous"
        );

        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[1]
                .module,
            "collections"
        );
        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[1]
                .name,
            "OrderedDict"
        );

        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[2]
                .module,
            "torch"
        );
        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[2]
                .name,
            "LongStorage"
        );

        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[3]
                .module,
            "torch._utils"
        );
        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[3]
                .name,
            "_rebuild_tensor_v2"
        );
        assert_eq!(
            decoded_response[0]
                .security
                .as_ref()
                .unwrap()
                .pickle_import_scan
                .as_ref()
                .unwrap()
                .imports[3]
                .safety,
            "innocuous"
        );

        Ok(())
    }
}
