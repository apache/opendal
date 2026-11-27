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

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Buf;
use serde::Deserialize;

use super::core::{
    B2Core, ErrorContext, File, ListFileNamesResponse, ListFileVersionsRequest,
    ListFileVersionsResponse, parse_error, parse_file_info,
};
use opendal_core::raw::*;
use opendal_core::*;

pub type B2Listers = TwoWays<oio::PageLister<B2Lister>, B2VersionLister>;

pub struct B2Lister {
    core: Arc<B2Core>,
    ctx: OperationContext,

    path: String,
    delimiter: Option<&'static str>,
    limit: Option<usize>,

    /// B2 starts listing **after** this specified key
    start_after: Option<String>,
}

impl B2Lister {
    pub fn new(
        core: Arc<B2Core>,
        ctx: OperationContext,
        path: &str,
        recursive: bool,
        limit: Option<usize>,
        start_after: Option<&str>,
    ) -> Self {
        let delimiter = if recursive { None } else { Some("/") };
        Self {
            core,
            ctx,

            path: path.to_string(),
            delimiter,
            limit,
            start_after: start_after.map(String::from),
        }
    }
}

impl oio::PageList for B2Lister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .list_file_names(
                &self.ctx,
                Some(&self.path),
                self.delimiter,
                self.limit,
                if ctx.token.is_empty() {
                    self.start_after
                        .as_ref()
                        .map(|v| build_absolute_path(&self.core.root, v))
                } else {
                    Some(ctx.token.clone())
                },
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListFileNames")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let output: ListFileNamesResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        match output.next_file_name {
            Some(token) if !token.is_empty() => {
                ctx.token = token;
            }
            _ => {
                ctx.done = true;
                ctx.token.clear();
            }
        }

        for file in output.files {
            if let Some(start_after) = self.start_after.clone()
                && build_absolute_path(&self.core.root, &start_after) == file.file_name
            {
                continue;
            }
            let metadata = parse_file_info(&file).build();
            let path = if file.file_name == self.core.root {
                "/".to_string()
            } else {
                build_relative_path(&self.core.root, &file.file_name)
            };

            ctx.entries.push_back(oio::Entry::new(&path, metadata));
        }

        Ok(())
    }
}

pub struct B2VersionLister {
    core: Arc<B2Core>,
    ctx: OperationContext,
    prefix: String,
    start_after: Option<String>,
    delimiter: Option<&'static str>,
    limit: Option<usize>,
    include_versions: bool,
    include_deleted: bool,
    cursor: VersionCursor,
    last_version_name: Option<String>,
    entries: VecDeque<oio::Entry>,
}

#[derive(PartialEq, Eq)]
enum VersionCursor {
    Start,
    Next {
        file_name: String,
        file_id: Option<String>,
    },
    Done,
}

#[derive(Deserialize)]
#[serde(
    tag = "action",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum ListedFileVersion {
    #[serde(rename = "upload", alias = "copy")]
    Data {
        file_id: String,
        #[serde(flatten)]
        file: File,
    },
    Hide {
        file_name: String,
        file_id: String,
        #[serde(deserialize_with = "deserialize_upload_timestamp")]
        upload_timestamp: Timestamp,
    },
    Start,
    Folder {
        file_name: String,
    },
}

fn deserialize_upload_timestamp<'de, D>(deserializer: D) -> std::result::Result<Timestamp, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let millis = i64::deserialize(deserializer)?;
    Timestamp::from_millisecond(millis).map_err(serde::de::Error::custom)
}

impl B2VersionLister {
    pub fn new(core: Arc<B2Core>, ctx: OperationContext, path: &str, args: &OpList) -> Self {
        Self {
            prefix: build_absolute_path(&core.root, path),
            start_after: args
                .start_after()
                .map(|key| build_absolute_path(&core.root, key)),
            delimiter: (!args.recursive()).then_some("/"),
            limit: args.limit(),
            include_versions: args.versions(),
            include_deleted: args.deleted(),
            core,
            ctx,
            cursor: VersionCursor::Start,
            last_version_name: None,
            entries: VecDeque::new(),
        }
    }

    fn push_entry(&mut self, file_name: &str, metadata: Metadata) {
        // B2's startFileName is inclusive, even when the name spans pages.
        if self.start_after.as_deref() == Some(file_name) {
            return;
        }
        let path = build_relative_path(&self.core.root, file_name);
        self.entries.push_back(oio::Entry::with(path, metadata));
    }
}

impl oio::List for B2VersionLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        while self.entries.is_empty() {
            let (start_file_name, start_file_id) = match &self.cursor {
                VersionCursor::Start => (self.start_after.as_deref(), None),
                VersionCursor::Next { file_name, file_id } => {
                    (Some(file_name.as_str()), file_id.as_deref())
                }
                VersionCursor::Done => return Ok(None),
            };
            let request = ListFileVersionsRequest {
                bucket_id: &self.core.bucket_id,
                prefix: &self.prefix,
                max_file_count: self.limit,
                start_file_name,
                start_file_id,
                delimiter: self.delimiter,
            };
            let page: ListFileVersionsResponse<ListedFileVersion> = self
                .core
                .list_file_versions(&self.ctx, request, Operation::List)
                .await?;

            let cursor = match page.next_file_name.filter(|name| !name.is_empty()) {
                Some(file_name) => VersionCursor::Next {
                    file_name,
                    file_id: page.next_file_id.filter(|id| !id.is_empty()),
                },
                None => VersionCursor::Done,
            };
            if cursor == self.cursor {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "B2 version listing did not advance",
                ));
            }
            self.cursor = cursor;

            for version in page.files {
                // B2 orders versions of each name newest first, including
                // hide markers and unfinished uploads. Only data versions
                // and hide markers determine the current version.
                match version {
                    ListedFileVersion::Data { file_id, file } => {
                        let is_current = self.last_version_name.as_ref() != Some(&file.file_name);
                        if self.include_versions || is_current {
                            let mut metadata = parse_file_info(&file);
                            metadata.version(&file_id).is_current(Some(is_current));
                            self.push_entry(&file.file_name, metadata.build());
                        }
                        self.last_version_name = Some(file.file_name);
                    }
                    ListedFileVersion::Hide {
                        file_name,
                        file_id,
                        upload_timestamp,
                    } => {
                        if self.include_deleted {
                            let is_current = self.last_version_name.as_ref() != Some(&file_name);
                            let mut metadata = if file_name.ends_with('/') {
                                MetadataBuilder::dir()
                            } else {
                                MetadataBuilder::file(0)
                            };
                            metadata
                                .version(&file_id)
                                .is_deleted(true)
                                .is_current(Some(is_current))
                                .last_modified(upload_timestamp);
                            self.push_entry(&file_name, metadata.build());
                        }
                        self.last_version_name = Some(file_name);
                    }
                    ListedFileVersion::Start => {}
                    ListedFileVersion::Folder { file_name } => {
                        self.push_entry(&file_name, MetadataBuilder::dir().build());
                    }
                }
            }
        }
        Ok(self.entries.pop_front())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::stream;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::raw::oio::List as _;

    use super::*;
    use crate::core::tests::test_core;

    struct ScriptedListTransport {
        pages: Mutex<Vec<String>>,
        requests: Arc<Mutex<Vec<Request<Buffer>>>>,
    }

    impl HttpTransport for ScriptedListTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let buf = if req.uri().path() == "/b2api/v4/b2_authorize_account" {
                Buffer::from(
                    r#"{"authorizationToken":"token","apiInfo":{"storageApi":{"apiUrl":"https://api.example","downloadUrl":"https://download.example"}}}"#,
                )
            } else {
                self.requests.lock().unwrap().push(req);
                let mut pages = self.pages.lock().unwrap();
                assert!(!pages.is_empty(), "unexpected extra list page request");
                Buffer::from(pages.remove(0))
            };
            let len = buf.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn test_lister_stops_on_empty_next_file_name() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedListTransport {
            pages: Mutex::new(vec![
                r#"{
                    "files": [
                        {"fileId": "4_z1", "fileName": "a.txt", "contentLength": 1}
                    ],
                    "nextFileName": ""
                }"#
                .to_string(),
            ]),
            requests: requests.clone(),
        };
        let core = test_core("/");
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));
        let mut lister = oio::PageLister::new(B2Lister::new(core, ctx, "", false, None, None));

        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            entries.push(entry);
        }

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path(), "a.txt");
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_lister_normalizes_root_entry_to_slash() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedListTransport {
            pages: Mutex::new(vec![
                r#"{
                    "files": [
                        {"fileId": "4_z0", "fileName": "sub/", "contentLength": 0},
                        {"fileId": "4_z1", "fileName": "sub/a.txt", "contentLength": 5},
                        {"fileId": null, "fileName": "sub/dir/", "contentLength": 0}
                    ],
                    "nextFileName": null
                }"#
                .to_string(),
            ]),
            requests,
        };
        let core = test_core("/sub/");
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));
        let mut lister = oio::PageLister::new(B2Lister::new(core, ctx, "", false, None, None));

        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            entries.push((entry.path().to_string(), entry.metadata().mode()));
        }

        assert_eq!(
            entries,
            vec![
                ("/".to_string(), EntryMode::DIR),
                ("a.txt".to_string(), EntryMode::FILE),
                ("dir/".to_string(), EntryMode::DIR),
            ]
        );
    }

    #[tokio::test]
    async fn test_lister_paginates_non_empty_next_file_name() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = ScriptedListTransport {
            pages: Mutex::new(vec![
                r#"{
                    "files": [{"fileId": "4_z1", "fileName": "a.txt", "contentLength": 1}],
                    "nextFileName": "b.txt"
                }"#
                .to_string(),
                r#"{
                    "files": [{"fileId": "4_z2", "fileName": "b.txt", "contentLength": 2}],
                    "nextFileName": ""
                }"#
                .to_string(),
            ]),
            requests: requests.clone(),
        };
        let core = test_core("/");
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));
        let mut lister = oio::PageLister::new(B2Lister::new(core, ctx, "", false, None, None));

        let mut paths = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            paths.push(entry.path().to_string());
        }

        assert_eq!(paths, vec!["a.txt", "b.txt"]);
        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert!(
            recorded[1]
                .uri()
                .query()
                .unwrap()
                .contains("startFileName=b.txt")
        );
    }

    #[tokio::test]
    async fn test_version_listing_filters_data_and_hides_across_native_pages() -> Result<()> {
        let pages = [
            r#"{"files":[{"action":"start","fileName":"sub/a","fileId":"unfinished"}],"nextFileName":"sub/a","nextFileId":"current"}"#,
            r#"{"files":[{"action":"upload","fileName":"sub/a","fileId":"current","contentLength":3}],"nextFileName":"sub/a","nextFileId":"old-hide"}"#,
            r#"{"files":[{"action":"hide","fileName":"sub/a","fileId":"old-hide","uploadTimestamp":1700000000123}],"nextFileName":"sub/a","nextFileId":"old"}"#,
            r#"{"files":[{"action":"upload","fileName":"sub/a","fileId":"old","contentLength":1}],"nextFileName":"sub/b","nextFileId":"current-hide"}"#,
            r#"{"files":[{"action":"hide","fileName":"sub/b","fileId":"current-hide","uploadTimestamp":1700000000456}],"nextFileName":"sub/b","nextFileId":"hidden"}"#,
            r#"{"files":[{"action":"upload","fileName":"sub/b","fileId":"hidden","contentLength":2}],"nextFileName":"sub/dir/","nextFileId":null}"#,
            r#"{"files":[{"action":"folder","fileName":"sub/dir/","fileId":null}],"nextFileName":"sub/z","nextFileId":"last"}"#,
            r#"{"files":[{"action":"upload","fileName":"sub/z","fileId":"last","contentLength":1}],"nextFileName":null,"nextFileId":null}"#,
        ];
        let current = ("a", Some("current"), Some(true), false);
        let old = ("a", Some("old"), Some(false), false);
        let old_hide = ("a", Some("old-hide"), Some(false), true);
        let current_hide = ("b", Some("current-hide"), Some(true), true);
        let hidden = ("b", Some("hidden"), Some(false), false);
        let folder = ("dir/", None, None, false);
        let last = ("z", Some("last"), Some(true), false);
        for (versions, deleted, expected) in [
            (true, false, vec![current, old, hidden, folder, last]),
            (
                false,
                true,
                vec![current, old_hide, current_hide, folder, last],
            ),
            (
                true,
                true,
                vec![current, old_hide, old, current_hide, hidden, folder, last],
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let transport = ScriptedListTransport {
                pages: Mutex::new(pages.map(str::to_string).into()),
                requests: requests.clone(),
            };
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
            let op = Operator::new(
                crate::B2::default()
                    .root("sub")
                    .bucket("version-list-tests")
                    .bucket_id("bucket-id")
                    .application_key_id("test-key")
                    .application_key("test-secret"),
            )?
            .with_context(ctx);

            let entries = op
                .list_with("/")
                .limit(1)
                .versions(versions)
                .deleted(deleted)
                .await?;
            let actual: Vec<_> = entries
                .iter()
                .map(|entry| {
                    let metadata = entry.metadata();
                    if metadata.is_deleted() {
                        let timestamp = if entry.path() == "a" {
                            "2023-11-14T22:13:20.123Z"
                        } else {
                            "2023-11-14T22:13:20.456Z"
                        };
                        assert_eq!(metadata.last_modified(), Some(timestamp.parse().unwrap()));
                        assert!(metadata.is_file());
                        assert_eq!(metadata.content_length(), 0);
                    }
                    (
                        entry.path(),
                        metadata.version(),
                        metadata.is_current(),
                        metadata.is_deleted(),
                    )
                })
                .collect();
            assert_eq!(actual, expected, "versions={versions}, deleted={deleted}");

            let recorded = requests.lock().unwrap();
            let bodies: Vec<serde_json::Value> = recorded
                .iter()
                .map(|request| {
                    assert_eq!(request.uri().path(), "/b2api/v2/b2_list_file_versions");
                    serde_json::from_slice(&request.body().to_bytes()).unwrap()
                })
                .collect();
            let markers: Vec<_> = bodies
                .iter()
                .map(|body| {
                    assert_eq!(body["delimiter"], "/");
                    assert_eq!(body["maxFileCount"], 1);
                    (body["startFileName"].as_str(), body["startFileId"].as_str())
                })
                .collect();
            assert_eq!(
                markers,
                [
                    (None, None),
                    (Some("sub/a"), Some("current")),
                    (Some("sub/a"), Some("old-hide")),
                    (Some("sub/a"), Some("old")),
                    (Some("sub/b"), Some("current-hide")),
                    (Some("sub/b"), Some("hidden")),
                    (Some("sub/dir/"), None),
                    (Some("sub/z"), Some("last")),
                ]
            );
        }
        Ok(())
    }
}
