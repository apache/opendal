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
use http::StatusCode;

use super::core::parse_error;
use super::core::{ErrorContext, WebhdfsCore};
use super::message::*;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

pub struct WebhdfsLister {
    core: Arc<WebhdfsCore>,
    ctx: OperationContext,
    path: String,
}

impl WebhdfsLister {
    pub fn new(core: Arc<WebhdfsCore>, ctx: OperationContext, path: &str) -> Self {
        Self {
            core,
            ctx,
            path: path.to_string(),
        }
    }
}

impl oio::PageList for WebhdfsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let file_status = if self.core.disable_list_batch {
            let resp = self.core.webhdfs_list_status(&self.ctx, &self.path).await?;
            match resp.status() {
                StatusCode::OK => {
                    ctx.done = true;
                    ctx.entries.push_back(oio::Entry::new(
                        format!("{}/", self.path).as_str(),
                        MetadataBuilder::dir().build(),
                    ));

                    let bs = resp.into_body();
                    serde_json::from_reader::<_, FileStatusesWrapper>(bs.reader())
                        .map_err(new_json_deserialize_error)?
                        .file_statuses
                        .file_status
                }
                StatusCode::NOT_FOUND => {
                    ctx.done = true;
                    return Ok(());
                }
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("ListStatus")),
                        resp,
                    ));
                }
            }
        } else {
            let resp = self
                .core
                .webhdfs_list_status_batch(&self.ctx, &self.path, &ctx.token)
                .await?;
            match resp.status() {
                StatusCode::OK => {
                    let bs = resp.into_body();
                    let res: DirectoryListingWrapper =
                        serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                    let directory_listing = res.directory_listing;
                    let file_statuses = directory_listing.partial_listing.file_statuses.file_status;

                    if directory_listing.remaining_entries == 0 {
                        ctx.entries.push_back(oio::Entry::new(
                            format!("{}/", self.path).as_str(),
                            MetadataBuilder::dir().build(),
                        ));

                        ctx.done = true;
                    } else {
                        let Some(last) = file_statuses.last() else {
                            return Err(Error::new(
                                ErrorKind::Unexpected,
                                "webhdfs LISTSTATUS_BATCH returned empty page with remaining entries",
                            ));
                        };
                        if last.path_suffix.is_empty() || last.path_suffix == ctx.token {
                            return Err(Error::new(
                                ErrorKind::Unexpected,
                                "webhdfs LISTSTATUS_BATCH did not advance startAfter token",
                            ));
                        }
                        ctx.token.clone_from(&last.path_suffix);
                    }

                    file_statuses
                }
                StatusCode::NOT_FOUND => {
                    ctx.done = true;
                    return Ok(());
                }
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("ListStatusBatch")),
                        resp,
                    ));
                }
            }
        };

        for status in file_status {
            let mut path = if self.path.is_empty() {
                status.path_suffix.to_string()
            } else {
                format!("{}/{}", self.path, status.path_suffix)
            };

            let meta = match status.ty {
                FileStatusType::Directory => MetadataBuilder::dir().build(),
                FileStatusType::File => {
                    let mut metadata = MetadataBuilder::file(status.length);
                    metadata.last_modified(Timestamp::from_millisecond(status.modification_time)?);
                    metadata.build()
                }
                FileStatusType::Symlink | FileStatusType::Unknown => {
                    MetadataBuilder::unknown().build()
                }
            };

            if meta.mode().is_file() {
                path = path.trim_end_matches('/').to_string();
            }
            if meta.mode().is_dir() {
                path += "/"
            }
            let entry = oio::Entry::new(&path, meta);
            ctx.entries.push_back(entry);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use asyncband::once::OnceCell;
    use http::{Request, Response};
    use oio::PageList;

    use super::*;

    struct StaticTransport(String);

    impl HttpTransport for StaticTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Buffer::from(self.0.clone());
            let size = body.len() as u64;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(futures::stream::iter([Ok(body)]), Some(size)))
                .unwrap())
        }
    }

    fn lister_with_body(body: &str) -> WebhdfsLister {
        let core = Arc::new(WebhdfsCore {
            info: ServiceInfo::new("webhdfs", "/", ""),
            capability: Capability {
                list: true,
                ..Default::default()
            },
            root: "/".to_string(),
            endpoint: "http://127.0.0.1:9870".to_string(),
            user_name: None,
            delegation: None,
            root_checker: OnceCell::new(),
            atomic_write_dir: None,
            disable_list_batch: false,
        });
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(StaticTransport(body.to_string())));
        WebhdfsLister::new(core, ctx, "dir")
    }

    #[tokio::test]
    async fn next_page_errors_on_empty_batch_with_remaining_entries() {
        let lister = lister_with_body(
            r#"{
                "DirectoryListing": {
                    "partialListing": { "FileStatuses": { "FileStatus": [] } },
                    "remainingEntries": 1
                }
            }"#,
        );
        let mut page_ctx = oio::PageContext {
            done: false,
            token: String::new(),
            entries: VecDeque::new(),
        };

        let err = lister.next_page(&mut page_ctx).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn next_page_errors_when_batch_token_does_not_advance() {
        let lister = lister_with_body(
            r#"{
                "DirectoryListing": {
                    "partialListing": {
                        "FileStatuses": {
                            "FileStatus": [{
                                "length": 10,
                                "modificationTime": 1700000000000,
                                "pathSuffix": "same",
                                "type": "FILE"
                            }]
                        }
                    },
                    "remainingEntries": 2
                }
            }"#,
        );
        let mut page_ctx = oio::PageContext {
            done: false,
            token: "same".to_string(),
            entries: VecDeque::new(),
        };

        let err = lister.next_page(&mut page_ctx).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn next_page_errors_on_empty_last_path_suffix_with_remaining_entries() {
        let lister = lister_with_body(
            r#"{
                "DirectoryListing": {
                    "partialListing": {
                        "FileStatuses": {
                            "FileStatus": [{
                                "length": 10,
                                "modificationTime": 1700000000000,
                                "pathSuffix": "",
                                "type": "FILE"
                            }]
                        }
                    },
                    "remainingEntries": 1
                }
            }"#,
        );
        let mut page_ctx = oio::PageContext {
            done: false,
            token: String::new(),
            entries: VecDeque::new(),
        };

        let err = lister.next_page(&mut page_ctx).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn next_page_maps_symlink_and_finishes_when_remaining_is_zero() -> Result<()> {
        let lister = lister_with_body(
            r#"{
                "DirectoryListing": {
                    "partialListing": {
                        "FileStatuses": {
                            "FileStatus": [
                                {
                                    "length": 0,
                                    "modificationTime": 1700000000000,
                                    "pathSuffix": "link",
                                    "type": "SYMLINK"
                                },
                                {
                                    "length": 42,
                                    "modificationTime": 1700000000000,
                                    "pathSuffix": "file.txt",
                                    "type": "FILE"
                                }
                            ]
                        }
                    },
                    "remainingEntries": 0
                }
            }"#,
        );
        let mut page_ctx = oio::PageContext {
            done: false,
            token: String::new(),
            entries: VecDeque::new(),
        };

        lister.next_page(&mut page_ctx).await?;
        assert!(page_ctx.done);
        assert_eq!(page_ctx.entries.len(), 3);

        let dir_entry = page_ctx.entries.pop_front().unwrap();
        assert_eq!(dir_entry.path(), "dir/");
        assert_eq!(dir_entry.metadata().mode(), EntryMode::DIR);

        let symlink_entry = page_ctx.entries.pop_front().unwrap();
        assert_eq!(symlink_entry.path(), "dir/link");
        assert_eq!(symlink_entry.metadata().mode(), EntryMode::Unknown);

        let file_entry = page_ctx.entries.pop_front().unwrap();
        assert_eq!(file_entry.path(), "dir/file.txt");
        assert_eq!(file_entry.metadata().mode(), EntryMode::FILE);

        Ok(())
    }
}
