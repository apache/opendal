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

use std::sync::Arc;

use bytes::Buf;

use super::core::parse_error;
use super::core::*;
use opendal_core::raw::*;
use opendal_core::*;

pub struct DropboxLister {
    core: Arc<DropboxCore>,
    ctx: OperationContext,
    path: String,
    recursive: bool,
    limit: Option<usize>,
}

impl DropboxLister {
    pub fn new(
        core: Arc<DropboxCore>,
        ctx: OperationContext,
        path: String,
        recursive: bool,
        limit: Option<usize>,
    ) -> Self {
        Self {
            core,
            ctx,
            path,
            recursive,
            limit,
        }
    }
}

impl oio::PageList for DropboxLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        // The token is set when obtaining entries and returning `has_more` flag.
        // When the token exists, we should retrieve more entries using the Dropbox continue API.
        // Refer: https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder-continue
        let response = if !ctx.token.is_empty() {
            self.core
                .dropbox_list_continue(&self.ctx, &ctx.token)
                .await?
        } else {
            self.core
                .dropbox_list(&self.ctx, &self.path, self.recursive, self.limit)
                .await?
        };

        let status_code = response.status();

        if !status_code.is_success() {
            let error = parse_error(
                ErrorContext::new(if ctx.token.is_empty() {
                    ServiceOperation("ListFolder")
                } else {
                    ServiceOperation("ListFolderContinue")
                }),
                response,
            );

            let result = match error.kind() {
                ErrorKind::NotFound => Ok(()),
                _ => Err(error),
            };

            ctx.done = true;
            return result;
        }

        let bytes = response.into_body();
        let decoded_response: DropboxListResponse =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        for entry in decoded_response.entries {
            let entry_mode = match entry.tag.as_str() {
                "file" => EntryMode::FILE,
                "folder" => EntryMode::DIR,
                _ => EntryMode::Unknown,
            };

            let mut name = entry.name;
            let mut meta = match entry_mode {
                EntryMode::FILE => entry
                    .size
                    .map_or_else(MetadataBuilder::unknown, MetadataBuilder::file),
                EntryMode::DIR => MetadataBuilder::dir(),
                EntryMode::Unknown => MetadataBuilder::unknown(),
            };

            // Dropbox will return folder names that do not end with '/'.
            if entry_mode == EntryMode::DIR && !name.ends_with('/') {
                name.push('/');
            }

            // The behavior here aligns with Dropbox's stat function.
            if entry_mode == EntryMode::FILE {
                let date_utc_last_modified = entry.client_modified.parse::<Timestamp>()?;
                meta.last_modified(date_utc_last_modified);
            }

            ctx.entries.push_back(oio::Entry::with(name, meta.build()));
        }

        if decoded_response.has_more {
            ctx.token = decoded_response.cursor;
            ctx.done = false;
        } else {
            ctx.done = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use asyncband::mutex::Mutex;
    use http::Request;
    use http::Response;
    use http::StatusCode;

    use super::*;
    use opendal_core::raw::oio::PageList;

    struct FixedJsonTransport {
        body: String,
    }

    impl HttpTransport for FixedJsonTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let buf = Buffer::from(self.body.clone());
            let len = buf.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(futures::stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    fn listed_paths(entries: &std::collections::VecDeque<oio::Entry>) -> Vec<String> {
        let mut paths = entries
            .iter()
            .map(|entry| entry.path().to_string())
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    async fn list_paths(root: &str, path: &str, body: &str) -> Vec<String> {
        let ctx = OperationContext::from_parts(
            HttpTransporter::new(FixedJsonTransport {
                body: body.to_string(),
            }),
            Executor::default(),
        );
        let core = Arc::new(DropboxCore {
            info: ServiceInfo::new("dropbox", root, ""),
            capability: Capability::default(),
            root: root.to_string(),
            signer: Arc::new(Mutex::new(DropboxSigner {
                access_token: "test-token".to_string(),
                expires_in: Timestamp::from_second(4_000_000_000).unwrap(),
                ..DropboxSigner::default()
            })),
        });
        let lister = DropboxLister::new(core, ctx, path.to_string(), true, None);
        let mut page = oio::PageContext {
            done: false,
            token: String::new(),
            entries: std::collections::VecDeque::new(),
        };
        lister.next_page(&mut page).await.unwrap();
        listed_paths(&page.entries)
    }

    #[tokio::test]
    async fn test_list_keeps_directory_prefix_and_lists_itself() {
        let body = r#"{
            "entries": [
                {
                    ".tag": "folder",
                    "name": "sub",
                    "path_display": "/dir/sub",
                    "path_lower": "/dir/sub",
                    "id": "id:folder"
                },
                {
                    ".tag": "file",
                    "name": "a.txt",
                    "path_display": "/dir/a.txt",
                    "path_lower": "/dir/a.txt",
                    "client_modified": "2020-01-01T00:00:00Z",
                    "size": 3,
                    "id": "id:file"
                },
                {
                    ".tag": "file",
                    "name": "nested.txt",
                    "path_display": "/dir/sub/nested.txt",
                    "path_lower": "/dir/sub/nested.txt",
                    "client_modified": "2020-01-01T00:00:00Z",
                    "size": 4,
                    "id": "id:nested"
                }
            ],
            "cursor": "cur",
            "has_more": false
        }"#;

        assert_eq!(
            list_paths("/", "dir/", body).await,
            vec!["dir/", "dir/a.txt", "dir/sub/", "dir/sub/nested.txt",]
        );
    }
}
