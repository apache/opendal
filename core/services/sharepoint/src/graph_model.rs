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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize)]
pub struct GraphOAuthRefreshTokenResponseBody {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64, // in seconds
}

/// We `$select` some fields when sending GET requests.
/// Please keep [`SharePointItem`] fields and this variable in sync.
/// Read more at https://learn.microsoft.com/en-us/graph/query-parameters?tabs=http#select-parameter
pub const GENERAL_SELECT_PARAM: &str =
    "$select=id,name,lastModifiedDateTime,eTag,size,parentReference,folder,file";

/// We `$select` some fields when listing versions.
/// Please keep [`SharePointItemVersion`] fields and this variable in sync.
/// Read more at https://learn.microsoft.com/en-us/graph/query-parameters?tabs=http#select-parameter
pub const VERSION_SELECT_PARAM: &str = "$select=id,size,lastModifiedDateTime";

/// We `$select` only what is needed to anchor the operator on a `driveItem`.
pub const ANCHOR_SELECT_PARAM: &str = "$select=id,name,parentReference";

/// The `driveItem` returned when resolving a sharing URL through `/shares`.
///
/// Read more at https://learn.microsoft.com/en-us/graph/api/shares-get
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharePointAnchorItem {
    pub id: String,
    pub name: String,
    pub parent_reference: AnchorParentReference,
}

/// A trimmed `itemReference`.
///
/// Anchor resolution only needs the drive that owns the item. The full
/// [`ParentReference`] requires `path` and `id`, which the `/shares` endpoint
/// omits when the item sits at the root of a document library.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorParentReference {
    pub drive_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GraphApiSharePointListResponse {
    #[serde(rename = "@odata.nextLink")]
    pub next_link: Option<String>,
    pub value: Vec<SharePointItem>,
}

/// A `DriveItem`
/// read more at https://learn.microsoft.com/en-us/graph/api/resources/driveitem
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharePointItem {
    pub id: String,
    pub name: String,
    pub last_modified_date_time: String,
    // Absent for the document library root itself (same Graph quirk as
    // `ParentReference`'s `path`/`id` above) — every ordinary file/folder
    // has one.
    #[serde(default)]
    pub e_tag: Option<String>,
    pub size: i64,
    pub parent_reference: ParentReference,
    #[serde(flatten)]
    pub item_type: ItemType,
    pub versions: Option<Vec<SharePointItemVersion>>,
}

/// `path`/`id` are absent for a `SharePointItem` whose parent is the
/// document library root itself — the same Graph quirk `AnchorParentReference`
/// documents above, but hit here when *listing* the root's direct children
/// (their own `parentReference` still omits both) rather than when resolving
/// the root as an anchor. Neither field is read anywhere in this crate (only
/// `drive_id` is) — they exist purely to round-trip through
/// `SharePointPatchRequestBody` for copy/rename, which sends its own
/// synthetic values regardless (see `core.rs`'s two `ParentReference`
/// construction sites) — so making them optional costs nothing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub drive_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// Additional properties when represents a facet of a "DriveItem":
/// - "file", read more at https://learn.microsoft.com/en-us/graph/api/resources/file
/// - "folder", read more at https://learn.microsoft.com/en-us/graph/api/resources/folder
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum ItemType {
    Folder { folder: Folder },
    File { file: File },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct File {
    mime_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Folder {
    child_count: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GraphApiSharePointVersionsResponse {
    pub value: Vec<SharePointItemVersion>,
}

/// A `driveItemVersion`
///
/// Read more at https://learn.microsoft.com/en-us/graph/api/resources/driveitemversion
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharePointItemVersion {
    pub id: String,
    pub last_modified_date_time: String,
    pub size: i64,
}

// Microsoft's documentation wants developers to set this as URL parameters. Some APIs use
// this as an data field in the payload.
pub const REPLACE_EXISTING_ITEM_WHEN_CONFLICT: &str = "replace";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDirPayload {
    #[serde(rename = "@microsoft.graph.conflictBehavior")]
    conflict_behavior: String,
    name: String,
    folder: EmptyStruct,
}

impl CreateDirPayload {
    pub fn new(name: String) -> Self {
        Self {
            conflict_behavior: REPLACE_EXISTING_ITEM_WHEN_CONFLICT.to_string(),
            name,
            folder: EmptyStruct {},
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmptyStruct {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileUploadItem {
    #[serde(rename = "@microsoft.graph.conflictBehavior")]
    conflict_behavior: String,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharePointUploadSessionCreationResponseBody {
    pub upload_url: String,
    pub expiration_date_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharePointUploadSessionCreationRequestBody {
    item: FileUploadItem,
}

impl SharePointUploadSessionCreationRequestBody {
    pub fn new(path: String) -> Self {
        SharePointUploadSessionCreationRequestBody {
            item: FileUploadItem {
                conflict_behavior: REPLACE_EXISTING_ITEM_WHEN_CONFLICT.to_string(),
                name: path,
            },
        }
    }
}

/// represents copy and rename (update) operations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharePointPatchRequestBody {
    pub parent_reference: ParentReference,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharePointMonitorStatus {
    pub percentage_complete: f64, // useful for debugging
    pub status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_share_point_anchor_item_json() {
        // The shape returned by `GET /shares/u!{share-id}/driveItem`.
        let data = r#"{
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#shares('u%21aHR0cHM6...')/driveItem/$entity",
            "id": "01ABCDEF23456789ABCDEF0123456789ABCDEF0123",
            "name": "Reports",
            "webUrl": "https://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports",
            "parentReference": {
                "driveType": "documentLibrary",
                "driveId": "b!xYzLongOpaqueDriveIdentifierValue0123456789",
                "id": "01ABCDEF0000000000000000000000000000000000",
                "path": "/drives/b!xYzLongOpaqueDriveIdentifierValue0123456789/root:",
                "siteId": "contoso.sharepoint.com,5f1f11f3-a6b4-4414-aee0-215c774f80db,2c9f3e1a-1111-2222-3333-444455556666"
            },
            "folder": {
                "childCount": 3
            }
        }"#;

        let item: SharePointAnchorItem = serde_json::from_str(data).unwrap();
        assert_eq!(item.name, "Reports");
        assert_eq!(item.id, "01ABCDEF23456789ABCDEF0123456789ABCDEF0123");
        assert_eq!(
            item.parent_reference.drive_id,
            "b!xYzLongOpaqueDriveIdentifierValue0123456789"
        );
    }

    #[test]
    fn test_parse_share_point_anchor_item_without_parent_path_json() {
        // A document library root has no `path` or `id` in its `parentReference`.
        // `AnchorParentReference` exists so this still deserializes.
        let data = r#"{
            "id": "01ROOTROOTROOTROOTROOTROOTROOTROOTROOTROOT",
            "name": "Documents",
            "parentReference": {
                "driveType": "documentLibrary",
                "driveId": "b!xYzLongOpaqueDriveIdentifierValue0123456789"
            },
            "folder": {
                "childCount": 0
            }
        }"#;

        let item: SharePointAnchorItem = serde_json::from_str(data).unwrap();
        assert_eq!(item.name, "Documents");
        assert_eq!(
            item.parent_reference.drive_id,
            "b!xYzLongOpaqueDriveIdentifierValue0123456789"
        );
    }

    #[test]
    fn test_parse_share_point_list_response_json() {
        let data = r#"{
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#drives('b%21xYz')/items('01ABC')/children(id,name,lastModifiedDateTime,eTag,size,parentReference,folder,file)",
            "value": [
                {
                    "@odata.etag": "\"{3B131E1C-7D81-20AF-80D0-450D00000000},10\"",
                    "eTag": "\"{3B131E1C-7D81-20AF-80D0-450D00000000},10\"",
                    "id": "01ABCDEF0000000000000000000000000000000001",
                    "lastModifiedDateTime": "2025-02-23T11:45:26Z",
                    "name": "empty_folder",
                    "size": 0,
                    "parentReference": {
                        "driveType": "documentLibrary",
                        "driveId": "b!xYzLongOpaqueDriveIdentifierValue0123456789",
                        "id": "01ABCDEF23456789ABCDEF0123456789ABCDEF0123",
                        "name": "Reports",
                        "path": "/drives/b!xYzLongOpaqueDriveIdentifierValue0123456789/root:/Shared Documents/Reports",
                        "siteId": "5f1f11f3-a6b4-4414-aee0-215c774f80db"
                    },
                    "folder": {
                        "childCount": 0
                    }
                },
                {
                    "@odata.etag": "\"{3B131E1C-7D81-20AF-80D0-710000000000},6\"",
                    "eTag": "\"{3B131E1C-7D81-20AF-80D0-710000000000},6\"",
                    "id": "01ABCDEF0000000000000000000000000000000002",
                    "lastModifiedDateTime": "2025-02-16T19:48:39Z",
                    "name": "quarterly.xlsx",
                    "size": 10560537,
                    "parentReference": {
                        "driveType": "documentLibrary",
                        "driveId": "b!xYzLongOpaqueDriveIdentifierValue0123456789",
                        "id": "01ABCDEF23456789ABCDEF0123456789ABCDEF0123",
                        "name": "Reports",
                        "path": "/drives/b!xYzLongOpaqueDriveIdentifierValue0123456789/root:/Shared Documents/Reports",
                        "siteId": "5f1f11f3-a6b4-4414-aee0-215c774f80db"
                    },
                    "file": {
                        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    }
                }
            ]
        }"#;

        let response: GraphApiSharePointListResponse = serde_json::from_str(data).unwrap();
        assert_eq!(response.value.len(), 2);
        let item = &response.value[0];
        assert_eq!(item.name, "empty_folder");
        assert_eq!(item.last_modified_date_time, "2025-02-23T11:45:26Z");
        assert_eq!(item.size, 0);
        if let ItemType::Folder { folder, .. } = &item.item_type {
            assert_eq!(folder.child_count, 0);
        } else {
            panic!("item_type is not a folder");
        }

        let item = &response.value[1];
        if let ItemType::File { file, .. } = &item.item_type {
            assert_eq!(
                file.mime_type,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            );
        } else {
            panic!("item_type is not a file");
        }
    }

    /// A direct child of the document library root has a `parentReference`
    /// missing `path`/`id`, and can itself be missing `eTag` too — the same
    /// root-item quirk `test_parse_share_point_anchor_item_without_parent_path_json`
    /// covers for the `/shares` anchor lookup, but hit here via
    /// `GET .../children` when the operator itself is anchored at the
    /// library root.
    #[test]
    fn test_parse_share_point_list_response_for_root_child_without_parent_path_json() {
        let data = r#"{
            "value": [
                {
                    "id": "01ABCDEF0000000000000000000000000000000001",
                    "lastModifiedDateTime": "2025-02-23T11:45:26Z",
                    "name": "Reports",
                    "size": 0,
                    "parentReference": {
                        "driveType": "documentLibrary",
                        "driveId": "b!xYzLongOpaqueDriveIdentifierValue0123456789",
                        "siteId": "5f1f11f3-a6b4-4414-aee0-215c774f80db"
                    },
                    "folder": {
                        "childCount": 3
                    }
                }
            ]
        }"#;

        let response: GraphApiSharePointListResponse = serde_json::from_str(data).unwrap();
        assert_eq!(response.value.len(), 1);
        let item = &response.value[0];
        assert_eq!(item.name, "Reports");
        assert!(item.e_tag.is_none());
        assert!(item.parent_reference.path.is_none());
        assert!(item.parent_reference.id.is_none());
        assert_eq!(item.parent_reference.drive_id, "b!xYzLongOpaqueDriveIdentifierValue0123456789");
    }

    #[test]
    fn test_parse_share_point_list_response_with_next_link_json() {
        let response_json = r#"{
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/drives/b%21xYz/items/01ABC/children?$select=id%2cname&$top=2&$skiptoken=UGFnZWQ9VFJVRQ",
            "value": []
        }"#;

        let response: GraphApiSharePointListResponse = serde_json::from_str(response_json).unwrap();
        assert!(response.value.is_empty());
        assert!(response.next_link.is_some());
    }

    #[test]
    fn test_parse_share_point_file_with_version_json() {
        let data = r#"{
            "@odata.etag": "\"{3B131E1C-7D81-20AF-80D0-720000000000},2\"",
            "eTag": "\"{3B131E1C-7D81-20AF-80D0-720000000000},2\"",
            "id": "01ABCDEF0000000000000000000000000000000003",
            "lastModifiedDateTime": "2025-02-16T19:49:05Z",
            "name": "filename.txt",
            "size": 3,
            "parentReference": {
                "driveType": "documentLibrary",
                "driveId": "b!xYzLongOpaqueDriveIdentifierValue0123456789",
                "id": "01ABCDEF23456789ABCDEF0123456789ABCDEF0123",
                "name": "Reports",
                "path": "/drives/b!xYzLongOpaqueDriveIdentifierValue0123456789/root:/Shared Documents/Reports"
            },
            "file": {
                "mimeType": "text/plain"
            },
            "versions": [
                {
                    "id": "1.0",
                    "lastModifiedDateTime": "2025-02-16T19:49:05Z",
                    "size": 3
                }
            ]
        }"#;

        let item: SharePointItem = serde_json::from_str(data).unwrap();
        let versions = item.versions.expect("Versions present");
        assert_eq!("1.0", versions[0].id);
        assert_eq!("2025-02-16T19:49:05Z", versions[0].last_modified_date_time);
    }

    #[test]
    fn test_parse_share_point_monitor_status_json() {
        let data = r#"{
            "percentageComplete": 100.0,
            "resourceId": "01JP3NYHGSBJ7R42UN65HZ333HZFWQTGL4",
            "status": "completed"
        }"#;

        let response: SharePointMonitorStatus = serde_json::from_str(data).unwrap();
        assert_eq!(response.status, "completed");
    }

    #[test]
    fn test_parse_share_point_item_versions_json() {
        let data = r#"{
            "value": [
                {
                    "id": "2.0",
                    "lastModifiedDateTime": "2025-03-16T17:02:49Z",
                    "size": 74758
                },
                {
                    "id": "1.0",
                    "lastModifiedDateTime": "2025-03-12T21:59:54Z",
                    "size": 74756
                }
            ]
        }"#;

        let response: GraphApiSharePointVersionsResponse = serde_json::from_str(data).unwrap();
        assert_eq!(response.value.len(), 2);
        let version = &response.value[0];
        assert_eq!(version.id, "2.0");
        assert_eq!(version.size, 74758);
    }
}
