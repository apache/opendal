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
use serde::Deserialize;

use super::core::parse_error;
use super::core::{DbfsCore, ErrorContext};
use opendal_core::raw::*;
use opendal_core::*;

pub struct DbfsLister {
    core: Arc<DbfsCore>,
    ctx: OperationContext,
    path: String,
}

impl DbfsLister {
    pub fn new(core: Arc<DbfsCore>, ctx: OperationContext, path: String) -> Self {
        Self { core, ctx, path }
    }
}

impl oio::PageList for DbfsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let response = self.core.dbfs_list(&self.ctx, &self.path).await?;

        let status_code = response.status();
        if !status_code.is_success() {
            if status_code == StatusCode::NOT_FOUND {
                ctx.done = true;
                return Ok(());
            }
            let error = parse_error(ErrorContext::new(ServiceOperation("List")), response);
            return Err(error);
        }

        let bytes = response.into_body();
        let decoded_response: DbfsOutputList =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        ctx.done = true;

        for status in decoded_response.files {
            let path = build_relative_path(&self.core.root, &status.path);
            let entry: oio::Entry = match status.is_dir {
                true => {
                    let normalized_path = format!("{path}/");
                    let mut meta = MetadataBuilder::dir();
                    meta.last_modified(Timestamp::from_millisecond(status.modification_time)?);
                    oio::Entry::new(&normalized_path, meta.build())
                }
                false => {
                    let mut meta = MetadataBuilder::file(status.file_size as u64);
                    meta.last_modified(Timestamp::from_millisecond(status.modification_time)?);
                    oio::Entry::new(&path, meta.build())
                }
            };
            ctx.entries.push_back(entry);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct DbfsOutputList {
    #[serde(default)]
    files: Vec<DbfsStatus>,
}

#[derive(Debug, Deserialize)]
struct DbfsStatus {
    path: String,
    is_dir: bool,
    file_size: i64,
    modification_time: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use opendal_core::raw::oio::PageList;

    use super::*;
    use crate::core::test_support::RecordingTransport;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn next_page_handles_empty_json_object_for_empty_dir() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let transport = RecordingTransport::new([(StatusCode::OK, "{}")]);
            let ctx = transport.context();
            let core = Arc::new(DbfsCore {
                root: "/".to_string(),
                endpoint: "https://example.databricks.com".to_string(),
                token: "tok".to_string(),
            });

            let lister = DbfsLister::new(core, ctx, "/".to_string());
            let mut page_ctx = oio::PageContext {
                done: false,
                token: String::new(),
                entries: VecDeque::new(),
            };
            lister.next_page(&mut page_ctx).await.unwrap();

            assert!(page_ctx.done);
            assert!(page_ctx.entries.is_empty());

            let reqs = transport.requests();
            assert_eq!(reqs.len(), 1);
            assert_eq!(
                reqs[0].uri,
                "https://example.databricks.com/api/2.0/dbfs/list?path=/"
            );
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn next_page_parses_files_and_dirs() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let body = r#"{
                "files": [
                    {"path": "/root/sub", "is_dir": true, "file_size": 0, "modification_time": 1700000000000},
                    {"path": "/root/a.txt", "is_dir": false, "file_size": 42, "modification_time": 1700000001000}
                ]
            }"#;
            let transport = RecordingTransport::new([(StatusCode::OK, body)]);
            let ctx = transport.context();
            let core = Arc::new(DbfsCore {
                root: "/root/".to_string(),
                endpoint: "https://example.databricks.com".to_string(),
                token: "tok".to_string(),
            });

            let lister = DbfsLister::new(core, ctx, "/".to_string());
            let mut page_ctx = oio::PageContext {
                done: false,
                token: String::new(),
                entries: VecDeque::new(),
            };
            lister.next_page(&mut page_ctx).await.unwrap();

            assert!(page_ctx.done);
            assert_eq!(page_ctx.entries.len(), 2);
            assert_eq!(page_ctx.entries[0].path(), "sub/");
            assert_eq!(page_ctx.entries[0].metadata().mode(), EntryMode::DIR);
            assert_eq!(page_ctx.entries[1].path(), "a.txt");
            assert_eq!(page_ctx.entries[1].metadata().mode(), EntryMode::FILE);
            assert_eq!(page_ctx.entries[1].metadata().content_length(), 42);
        })
        .await
        .expect("test timed out");
    }
}
