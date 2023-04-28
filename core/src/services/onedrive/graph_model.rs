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

use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
struct GraphApiOneDriveResponse {
    #[serde(rename = "@odata.context")]
    odata_context: String,

    #[serde(rename = "@odata.count")]
    odata_count: usize,

    value: Vec<OneDriveItem>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OneDriveItem {
    #[serde(rename = "createdDateTime")]
    created_date_time: String,
    #[serde(rename = "eTag")]
    e_tag: String,
    id: String,
    #[serde(rename = "lastModifiedDateTime")]
    last_modified_date_time: String,
    name: String,

    size: usize,
    #[serde(rename = "webUrl")]
    web_url: String,
    #[serde(rename = "parentReference")]
    parent_reference: ParentReference,
    #[serde(rename = "fileSystemInfo")]
    file_system_info: FileSystemInfo,
    #[serde(flatten)]
    item_type: ItemType,
}

#[derive(Debug, Serialize, Deserialize)]
struct ParentReference {
    #[serde(rename = "driveId")]
    drive_id: String,
    #[serde(rename = "driveType")]
    drive_type: String,
    id: String,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileSystemInfo {
    #[serde(rename = "createdDateTime")]
    created_date_time: String,
    #[serde[rename = "lastModifiedDateTime"]]
    last_modified_date_time: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
enum ItemType {
    Folder {
        folder: HashMap<String, serde_json::Value>,
        #[serde(rename = "specialFolder")]
        special_folder: HashMap<String, String>,
    },
    File {
        file: HashMap<String, serde_json::Value>,
    },
}

fn parse_one_drive_json(data: &str) -> Result<GraphApiOneDriveResponse> {
    let response: GraphApiOneDriveResponse = serde_json::from_str(data)?;
    Ok(response)
}

#[test]
fn test_parse_one_drive_json() {
    let data = r#"{
        "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#users('user_id')/drive/root/children",
        "@odata.count": 1,
        "value": [
            {
                "createdDateTime": "2020-01-01T00:00:00Z",
                "cTag": "cTag",
                "eTag": "eTag",
                "id": "id",
                "lastModifiedDateTime": "2020-01-01T00:00:00Z",
                "name": "name",
                "size": 0,
                "webUrl": "webUrl",
                "reactions": {
                    "like": 0
                },
                "parentReference": {
                    "driveId": "driveId",
                    "driveType": "driveType",
                    "id": "id",
                    "path": "/drive/root:"
                },
                "fileSystemInfo": {
                    "createdDateTime": "2020-01-01T00:00:00Z",
                    "lastModifiedDateTime": "2020-01-01T00:00:00Z"
                },
                "folder": {
                    "childCount": 0
                },
                "specialFolder": {
                    "name": "name"
                }
            },
            {
                "@microsoft.graph.downloadUrl": "https://public.ch.files.1drv.com/y4mPh7u0QjYTl5j9aZDj77EoplXNhXFzSbakI4iYoUXMaGUOSmpx1d20AnCoU9G32nj6W2qsKNfecsgfmF6O8ZE89yUYj7qnhsIvfikcJjJ0_skDA12gl2cCScQ3opoza_RcG2Lb_Pa2jyqiqgruh0TJRcC1y7mtEw89wqXx2bgjOvmo0ozTAwopTtpti9yo43Zb7nBI1efm3IwWhFKcHUUKx7WlD_8VPXPB4Xffokz61NiXoxMeq0hbwrblcznywz2AcE71SprDyCi8E7kDRjwmiTNoyfZc_FuUMZDO29WUbA",
                "createdDateTime": "2018-12-30T05:32:55.46Z",
                "cTag": "aYzozMjIxN0ZDMTE1NEFFQzNEITEwMi4yNTc",
                "eTag": "aMzIyMTdGQzExNTRBRUMzRCExMDIuMw",
                "id": "32217FC1154AEC3D!102",
                "lastModifiedDateTime": "2018-12-30T05:33:23.557Z",
                "name": "Getting started with OneDrive.pdf",
                "size": 1025867,
                "webUrl": "https://1drv.ms/b/s!AD3sShXBfyEyZg",
                "reactions": {
                    "commentCount": 0
                },
                "createdBy": {
                    "user": {
                        "displayName": "Great Cat",
                        "id": "32217fc1154aec3d"
                    }
                },
                "lastModifiedBy": {
                    "user": {
                        "displayName": "Great Cat",
                        "id": "32217fc1154aec3d"
                    }
                },
                "parentReference": {
                    "driveId": "32217fc1154aec3d",
                    "driveType": "personal",
                    "id": "32217FC1154AEC3D!101",
                    "path": "/drive/root:"
                },
                "file": {
                    "mimeType": "application/pdf",
                    "hashes": {
                        "quickXorHash": "NIfFZIvQVZH260260iUuQN5GscM=",
                        "sha1Hash": "E8890F3D1CE6E3FCCE46D08B188275D6CAE3292C"
                    }
                },
                "fileSystemInfo": {
                    "createdDateTime": "2018-12-30T05:32:55.46Z",
                    "lastModifiedDateTime": "2018-12-30T05:32:55.46Z"
                }
            }
        ]
    }"#;
    let response = parse_one_drive_json(data).unwrap();
    assert_eq!(
        response.odata_context,
        "https://graph.microsoft.com/v1.0/$metadata#users('user_id')/drive/root/children"
    );
    assert_eq!(response.odata_count, 1);
    assert_eq!(response.value.len(), 2);
    let item = &response.value[0];
    assert_eq!(item.created_date_time, "2020-01-01T00:00:00Z");
    assert_eq!(item.e_tag, "eTag");
    assert_eq!(item.id, "id");
    assert_eq!(item.last_modified_date_time, "2020-01-01T00:00:00Z");
    assert_eq!(item.name, "name");
    assert_eq!(item.size, 0);
    assert_eq!(item.web_url, "webUrl");
    assert_eq!(
        item.item_type,
        ItemType::Folder {
            folder: {
                let mut map = HashMap::new();
                map.insert(
                    "childCount".to_string(),
                    serde_json::Value::Number(0.into()),
                );
                map
            },
            special_folder: {
                let mut map = HashMap::new();
                map.insert("name".to_string(), "name".to_string());
                map
            },
        }
    );
}
