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

//! HTTP response messages

use serde::Deserialize;

use crate::*;

#[derive(Debug, Deserialize)]
pub(super) struct BooleanResp {
    pub boolean: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct FileStatusWrapper {
    pub file_status: FileStatus,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct FileStatusesWrapper {
    pub file_statuses: FileStatuses,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct FileStatuses {
    pub file_status: Vec<FileStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileStatus {
    pub length: u64,
    pub modification_time: i64,

    pub path_suffix: String,
    #[serde(rename = "type")]
    pub ty: FileStatusType,
}

impl TryFrom<FileStatus> for Metadata {
    type Error = Error;
    fn try_from(value: FileStatus) -> Result<Self> {
        let mut meta = match value.ty {
            FileStatusType::Directory => Metadata::new(EntryMode::DIR),
            FileStatusType::File => Metadata::new(EntryMode::FILE),
        };
        let till_now = time::Duration::milliseconds(value.modification_time);
        let last_modified = time::OffsetDateTime::UNIX_EPOCH
            .checked_add(till_now)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "last modification overflowed!"))?;
        meta.set_last_modified(last_modified)
            .set_content_length(value.length);
        Ok(meta)
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum FileStatusType {
    Directory,
    File,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::raw::oio::Page;
    use crate::services::webhdfs::pager::WebhdfsPager;
    use crate::EntryMode;

    #[test]
    fn test_file_status() {
        let json = r#"
{
  "FileStatus":
  {
    "accessTime"      : 0,
    "blockSize"       : 0,
    "group"           : "supergroup",
    "length"          : 0,
    "modificationTime": 1320173277227,
    "owner"           : "webuser",
    "pathSuffix"      : "",
    "permission"      : "777",
    "replication"     : 0,
    "type"            : "DIRECTORY"
  }
}
"#;
        let status: FileStatusWrapper = serde_json::from_str(json).expect("must success");
        assert_eq!(status.file_status.length, 0);
        assert_eq!(status.file_status.modification_time, 1320173277227);
        assert_eq!(status.file_status.path_suffix, "");
        assert_eq!(status.file_status.ty, FileStatusType::Directory);
    }

    #[tokio::test]
    async fn test_list_empty() {
        let json = r#"
    {
        "FileStatuses": {"FileStatus":[]}
    }
        "#;
        let file_statuses = serde_json::from_str::<FileStatusesWrapper>(json)
            .expect("must success")
            .file_statuses
            .file_status;
        assert!(file_statuses.is_empty());
    }

    #[tokio::test]
    async fn test_list_status() {
        let json = r#"
{
  "FileStatuses":
  {
    "FileStatus":
    [
      {
        "accessTime"      : 1320171722771,
        "blockSize"       : 33554432,
        "group"           : "supergroup",
        "length"          : 24930,
        "modificationTime": 1320171722771,
        "owner"           : "webuser",
        "pathSuffix"      : "a.patch",
        "permission"      : "644",
        "replication"     : 1,
        "type"            : "FILE"
      },
      {
        "accessTime"      : 0,
        "blockSize"       : 0,
        "group"           : "supergroup",
        "length"          : 0,
        "modificationTime": 1320895981256,
        "owner"           : "szetszwo",
        "pathSuffix"      : "bar",
        "permission"      : "711",
        "replication"     : 0,
        "type"            : "DIRECTORY"
      }
    ]
  }
}
            "#;

        let file_statuses = serde_json::from_str::<FileStatusesWrapper>(json)
            .expect("must success")
            .file_statuses
            .file_status;

        let mut pager = WebhdfsPager::new("listing/directory", file_statuses);
        let mut entries = vec![];
        while let Some(oes) = pager.next().await.expect("must success") {
            entries.extend(oes);
        }

        entries.sort_by(|a, b| a.path().cmp(b.path()));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path(), "listing/directory/a.patch");
        assert_eq!(entries[0].mode(), EntryMode::FILE);
        assert_eq!(entries[1].path(), "listing/directory/bar/");
        assert_eq!(entries[1].mode(), EntryMode::DIR);
    }
}
