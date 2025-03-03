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

use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct GraphApiOnedriveListResponse {
    #[serde(rename = "@odata.nextLink")]
    pub next_link: Option<String>,
    pub value: Vec<OneDriveItem>,
}

/// mapping for a DriveItem representation
/// read more at https://learn.microsoft.com/en-us/onedrive/developer/rest-api/resources/driveitem
#[derive(Debug, Serialize, Deserialize)]
pub struct OneDriveItem {
    pub name: String,

    #[serde(rename = "lastModifiedDateTime")]
    pub last_modified_date_time: String,

    #[serde(rename = "eTag")]
    pub e_tag: String,

    pub size: i64,

    #[serde(rename = "parentReference")]
    pub parent_reference: ParentReference,

    #[serde(flatten)]
    pub item_type: ItemType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ParentReference {
    pub path: String,
}

/// Additional properties when represents a facet of a "DriveItem":
/// - "file", read more at https://learn.microsoft.com/en-us/onedrive/developer/rest-api/resources/file
/// - "folder", read more at https://learn.microsoft.com/en-us/onedrive/developer/rest-api/resources/folder
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum ItemType {
    Folder {
        folder: Folder,
        #[serde(rename = "specialFolder")]
        special_folder: Option<HashMap<String, String>>,
    },
    File {
        file: File,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OnedriveGetItemBody {
    #[serde(rename = "cTag")]
    pub(crate) c_tag: String,
    #[serde(rename = "eTag")]
    pub(crate) e_tag: String,
    id: String,
    #[serde(rename = "lastModifiedDateTime")]
    pub(crate) last_modified_date_time: String,
    pub(crate) name: String,
    pub(crate) size: u64,

    #[serde(flatten)]
    pub(crate) item_type: ItemType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct File {
    #[serde(rename = "mimeType")]
    mime_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Folder {
    #[serde(rename = "childCount")]
    child_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDirPayload {
    // folder: String,
    #[serde(rename = "@microsoft.graph.conflictBehavior")]
    conflict_behavior: String,
    name: String,
    folder: EmptyStruct,
}

impl CreateDirPayload {
    pub fn new(name: String) -> Self {
        Self {
            conflict_behavior: "replace".to_string(),
            name,
            folder: EmptyStruct {},
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmptyStruct {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileUploadItem {
    #[serde(rename = "@odata.type")]
    odata_type: String,
    #[serde(rename = "@microsoft.graph.conflictBehavior")]
    microsoft_graph_conflict_behavior: String,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OneDriveUploadSessionCreationResponseBody {
    #[serde(rename = "uploadUrl")]
    pub upload_url: String,
    #[serde(rename = "expirationDateTime")]
    pub expiration_date_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OneDriveUploadSessionCreationRequestBody {
    item: FileUploadItem,
}

impl OneDriveUploadSessionCreationRequestBody {
    pub fn new(path: String) -> Self {
        OneDriveUploadSessionCreationRequestBody {
            item: FileUploadItem {
                odata_type: "microsoft.graph.driveItemUploadableProperties".to_string(),
                microsoft_graph_conflict_behavior: "replace".to_string(),
                name: path,
            },
        }
    }
}

#[test]
fn test_parse_one_drive_json() {
    let data = r#"{
        "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#users('user_id')/drive/root/children",
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
                "createdDateTime": "2018-12-30T05:32:55.46Z",
                "cTag": "sample",
                "eTag": "sample",
                "id": "ID!102",
                "lastModifiedDateTime": "2018-12-30T05:33:23.557Z",
                "name": "Getting started with OneDrive.pdf",
                "size": 1025867,
                "reactions": {
                    "commentCount": 0
                },
                "createdBy": {
                    "user": {
                        "displayName": "Foo bar",
                        "id": "ID"
                    }
                },
                "lastModifiedBy": {
                    "user": {
                        "displayName": "Foo bar",
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
                    "mimeType": "application/pdf"
                },
                "fileSystemInfo": {
                    "createdDateTime": "2018-12-30T05:32:55.46Z",
                    "lastModifiedDateTime": "2018-12-30T05:32:55.46Z"
                }
            }
        ]
    }"#;

    let response: GraphApiOnedriveListResponse = serde_json::from_str(data).unwrap();
    assert_eq!(response.value.len(), 2);
    let item = &response.value[0];
    assert_eq!(item.name, "name");
}

#[test]
fn test_parse_folder_single() {
    let response_json = r#"
    {
        "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#users('great.cat%40outlook.com')/drive/root/children",
        "value": [
          {
            "createdDateTime": "2023-02-01T00:51:02.803Z",
            "cTag": "sample",
            "eTag": "sample",
            "id": "ID!3003",
            "lastModifiedDateTime": "2023-02-01T00:51:10.703Z",
            "name": "misc",
            "size": 1084627,
            "webUrl": "sample",
            "reactions": {
              "commentCount": 0
            },
            "createdBy": {
              "application": {
                "displayName": "OneDrive",
                "id": "481710a4"
              },
              "user": {
                "displayName": "Foo bar",
                "id": "01"
              }
            },
            "lastModifiedBy": {
              "application": {
                "displayName": "OneDrive",
                "id": "481710a4"
              },
              "user": {
                "displayName": "Foo bar",
                "id": "02"
              }
            },
            "parentReference": {
              "driveId": "ID",
              "driveType": "personal",
              "id": "ID!101",
              "path": "/drive/root:"
            },
            "fileSystemInfo": {
              "createdDateTime": "2023-02-01T00:51:02.803Z",
              "lastModifiedDateTime": "2023-02-01T00:51:02.803Z"
            },
            "folder": {
              "childCount": 9,
              "view": {
                "viewType": "thumbnails",
                "sortBy": "name",
                "sortOrder": "ascending"
              }
            }
          }
        ]
      }"#;

    let response: GraphApiOnedriveListResponse = serde_json::from_str(response_json).unwrap();
    assert_eq!(response.value.len(), 1);
    let item = &response.value[0];
    if let ItemType::Folder { folder, .. } = &item.item_type {
        assert_eq!(folder.child_count, serde_json::Value::Number(9.into()));
    } else {
        panic!("item_type is not folder");
    }
}
