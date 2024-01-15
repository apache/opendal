use serde::{Deserialize, Serialize};
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

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct iCloudRoot {
    #[serde(default)]
    pub asset_quota: i64,
    #[serde(default)]
    pub date_created: String,
    #[serde(default)]
    pub direct_children_count: i64,
    pub docwsid: String,
    pub drivewsid: String,
    pub etag: String,
    #[serde(default)]
    pub file_count: i64,
    pub items: Vec<iCloudItem>,
    pub name: String,
    pub number_of_items: i64,
    pub status: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub zone: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct iCloudItem {
    #[serde(default)]
    pub asset_quota: Option<i64>,
    #[serde(default)]
    pub date_created: String,
    #[serde(default)]
    pub date_modified: String,
    #[serde(default)]
    pub direct_children_count: Option<i64>,
    pub docwsid: String,
    pub drivewsid: String,
    pub etag: String,
    #[serde(default)]
    pub file_count: Option<i64>,
    #[serde(rename = "item_id")]
    pub item_id: Option<String>,
    pub name: String,
    pub parent_id: String,
    #[serde(default)]
    pub size: u64,
    #[serde(rename = "type")]
    pub type_field: String,
    pub zone: String,
    #[serde(default)]
    pub max_depth: Option<String>,
    #[serde(default)]
    pub is_chained_to_parent: Option<bool>,
}

impl std::fmt::Display for iCloudRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "iCloudRoot \
        docwsid:{}\
        drivewsid:{}\
        items:{:?}\
        name:{}\
        type_field:{}\
        zone:{}\
        ",
            self.docwsid, self.drivewsid, self.items, self.name, self.type_field, self.zone
        )
    }
}
impl std::fmt::Display for iCloudItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "iCloudItem \
        docwsid:{}\
        drivewsid:{}\
        items_id:{:?}\
        name:{}\
        type_field:{}\
        zone:{}\
        ",
            self.docwsid, self.drivewsid, self.item_id, self.name, self.type_field, self.zone
        )
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct iCloudObject {
    #[serde(rename = "document_id")]
    pub document_id: String,
    #[serde(rename = "item_id")]
    pub item_id: String,
    #[serde(rename = "owner_dsid")]
    pub owner_dsid: i64,
    #[serde(rename = "data_token")]
    pub data_token: DataToken,
    #[serde(rename = "double_etag")]
    pub double_etag: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataToken {
    pub url: String,
    pub token: String,
    pub signature: String,
    #[serde(rename = "wrapping_key")]
    pub wrapping_key: String,
    #[serde(rename = "reference_signature")]
    pub reference_signature: String,
}

impl std::fmt::Display for iCloudObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "iCloudObject \
        document_id:{}\
        owner_dsid:{}\
        item_id:{:?}\
        double_etag:{}\
        data_token:{:?}\
        ",
            self.document_id, self.owner_dsid, self.item_id, self.double_etag, self.data_token
        )
    }
}

impl std::fmt::Display for DataToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "DataToken \
        url:{}\
        reference_signatured:{}\
        token:{:?}\
        wrapping_key:{}\
        reference_signature:{}\
        ",
            self.url,
            self.reference_signature,
            self.token,
            self.wrapping_key,
            self.reference_signature
        )
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct iCloudCreateFolder {
    pub destination_drivews_id: String,
    pub folders: Vec<iCloudItem>,
}

impl std::fmt::Display for iCloudCreateFolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "iCloudCreateFolder \
        destination_drivews_id:{}\
        items:{:?}",
            self.destination_drivews_id, self.folders
        )
    }
}

#[test]
fn test_parse_icloud_drive_root_json() {
    let data = r#"{
  "assetQuota": 19603579,
  "dateCreated": "2019-06-10T14:17:49Z",
  "directChildrenCount": 3,
  "docwsid": "root",
  "drivewsid": "FOLDER::com.apple.CloudDocs::root",
  "etag": "w7",
  "fileCount": 22,
  "items": [
    {
      "assetQuota": 19603579,
      "dateCreated": "2021-02-05T08:30:58Z",
      "directChildrenCount": 22,
      "docwsid": "1E013608-C669-43DB-AC14-3D7A4E0A0500",
      "drivewsid": "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500",
      "etag": "sn",
      "fileCount": 22,
      "item_id": "CJWdk48eEAAiEB4BNgjGaUPbrBQ9ek4KBQAoAQ",
      "name": "Downloads",
      "parentId": "FOLDER::com.apple.CloudDocs::root",
      "shareAliasCount": 0,
      "shareCount": 0,
      "type": "FOLDER",
      "zone": "com.apple.CloudDocs"
    },
    {
      "dateCreated": "2019-06-10T14:17:54Z",
      "docwsid": "documents",
      "drivewsid": "FOLDER::com.apple.Keynote::documents",
      "etag": "1v",
      "maxDepth": "ANY",
      "name": "Keynote",
      "parentId": "FOLDER::com.apple.CloudDocs::root",
      "type": "APP_LIBRARY",
      "zone": "com.apple.Keynote"
    },
    {
      "assetQuota": 0,
      "dateCreated": "2024-01-06T02:35:08Z",
      "directChildrenCount": 0,
      "docwsid": "21E4A15E-DA77-472A-BAC8-B0C35A91F237",
      "drivewsid": "FOLDER::com.apple.CloudDocs::21E4A15E-DA77-472A-BAC8-B0C35A91F237",
      "etag": "w8",
      "fileCount": 0,
      "isChainedToParent": true,
      "item_id": "CJWdk48eEAAiECHkoV7ad0cqusiww1qR8jcoAQ",
      "name": "opendal",
      "parentId": "FOLDER::com.apple.CloudDocs::root",
      "shareAliasCount": 0,
      "shareCount": 0,
      "type": "FOLDER",
      "zone": "com.apple.CloudDocs"
    }
  ],
  "name": "",
  "numberOfItems": 16,
  "shareAliasCount": 0,
  "shareCount": 0,
  "status": "OK",
  "type": "FOLDER",
  "zone": "com.apple.CloudDocs"
}"#;

    let response: iCloudRoot = serde_json::from_str(data).unwrap();
    assert_eq!(response.name, "");
    assert_eq!(response.type_field, "FOLDER");
    assert_eq!(response.zone, "com.apple.CloudDocs");
    assert_eq!(response.docwsid, "root");
    assert_eq!(response.drivewsid, "FOLDER::com.apple.CloudDocs::root");
    assert_eq!(response.etag, "w7");
    assert_eq!(response.file_count, 22);
}

#[test]
fn test_parse_icloud_drive_folder_file() {
    let data = r#"{
  "assetQuota": 19603579,
  "dateCreated": "2021-02-05T08:34:21Z",
  "directChildrenCount": 22,
  "docwsid": "1E013608-C669-43DB-AC14-3D7A4E0A0500",
  "drivewsid": "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500",
  "etag": "w9",
  "fileCount": 22,
  "items": [
    {
      "dateChanged": "2021-02-18T14:10:46Z",
      "dateCreated": "2021-02-10T07:01:28Z",
      "dateModified": "2021-02-10T07:01:28Z",
      "docwsid": "304F8B99-588B-4376-B7C8-12FCAE7A78D0",
      "drivewsid": "FILE::com.apple.CloudDocs::304F8B99-588B-4376-B7C8-12FCAE7A78D0",
      "etag": "5j::5i",
      "extension": "pdf",
      "item_id": "CJWdk48eEAAiEDBPi5lYi0N2t8gS_K56eNA",
      "lastOpenTime": "2021-02-10T07:17:43Z",
      "name": "1-10-longest-prefix-match-notes",
      "parentId": "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500",
      "size": 802970,
      "type": "FILE",
      "zone": "com.apple.CloudDocs"
    },
    {
      "dateChanged": "2021-02-18T14:10:46Z",
      "dateCreated": "2021-02-10T07:01:34Z",
      "dateModified": "2021-02-10T07:01:34Z",
      "docwsid": "9605331E-7BF3-41A0-A128-A68FFA377C50",
      "drivewsid": "FILE::com.apple.CloudDocs::9605331E-7BF3-41A0-A128-A68FFA377C50",
      "etag": "5b::5a",
      "extension": "pdf",
      "item_id": "CJWdk48eEAAiEJYFMx5780GgoSimj_o3fFA",
      "lastOpenTime": "2021-02-10T10:28:42Z",
      "name": "1-11-ARP-notes",
      "parentId": "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500",
      "size": 639483,
      "type": "FILE",
      "zone": "com.apple.CloudDocs"
    }
    ],
  "name": "Downloads",
  "numberOfItems": 22,
  "parentId": "FOLDER::com.apple.CloudDocs::root",
  "shareAliasCount": 0,
  "shareCount": 0,
  "status": "OK",
  "type": "FOLDER",
  "zone": "com.apple.CloudDocs"
}"#;

    let response = serde_json::from_str::<iCloudRoot>(data).unwrap();

    assert_eq!(response.name, "Downloads");
    assert_eq!(response.type_field, "FOLDER");
    assert_eq!(response.zone, "com.apple.CloudDocs");
    assert_eq!(response.docwsid, "1E013608-C669-43DB-AC14-3D7A4E0A0500");
    assert_eq!(
        response.drivewsid,
        "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500"
    );
    assert_eq!(response.etag, "w9");
    assert_eq!(response.file_count, 22);
}
