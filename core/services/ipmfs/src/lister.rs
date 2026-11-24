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
use super::core::{ErrorContext, IpmfsCore};
use opendal_core::EntryMode;
use opendal_core::ErrorKind;
use opendal_core::MetadataBuilder;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::*;

pub struct IpmfsLister {
    core: Arc<IpmfsCore>,
    ctx: OperationContext,
    path: String,
}

impl IpmfsLister {
    pub fn new(core: Arc<IpmfsCore>, ctx: OperationContext, path: &str) -> Self {
        Self {
            core,
            ctx,
            path: path.to_string(),
        }
    }
}

impl oio::PageList for IpmfsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self.core.ipmfs_ls(&self.ctx, &self.path).await?;

        if resp.status() != StatusCode::OK {
            let err = parse_error(ErrorContext::new(ServiceOperation("FilesLs")), resp);
            if matches!(err.kind(), ErrorKind::NotFound) {
                // treat as empty listing
                ctx.done = true;
                return Ok(());
            }
            return Err(err);
        }

        let bs = resp.into_body();
        let entries_body: IpfsLsResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let trimmed = self.path.trim_matches('/');
        let prefix = if trimmed.is_empty() {
            String::new()
        } else {
            format!("{trimmed}/")
        };

        // Add current directory entry when processing the first page
        if ctx.token.is_empty() && !ctx.done {
            let path = if prefix.is_empty() { "/" } else { &prefix };
            ctx.entries
                .push_back(oio::Entry::new(path, MetadataBuilder::dir().build()));
        }

        // Mark dir stream has been consumed.
        ctx.done = true;

        for object in entries_body.entries.unwrap_or_default() {
            let (path, meta) = match object.mode() {
                EntryMode::FILE => (
                    format!("{prefix}{}", object.name),
                    MetadataBuilder::file(object.size).build(),
                ),
                EntryMode::DIR => (
                    format!("{prefix}{}/", object.name),
                    MetadataBuilder::dir().build(),
                ),
                EntryMode::Unknown => (
                    format!("{prefix}{}", object.name),
                    MetadataBuilder::unknown().build(),
                ),
            };

            ctx.entries.push_back(oio::Entry::new(&path, meta));
        }

        Ok(())
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponseEntry {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Type")]
    file_type: i64,
    #[serde(rename = "Size")]
    size: u64,
}

impl IpfsLsResponseEntry {
    /// ref: <https://github.com/ipfs/specs/blob/main/UNIXFS.md#data-format>
    ///
    /// ```protobuf
    /// enum DataType {
    ///     Raw = 0;
    ///     Directory = 1;
    ///     File = 2;
    ///     Metadata = 3;
    ///     Symlink = 4;
    ///     HAMTShard = 5;
    /// }
    /// ```
    fn mode(&self) -> EntryMode {
        match &self.file_type {
            1 | 5 => EntryMode::DIR,
            0 | 2 => EntryMode::FILE,
            _ => EntryMode::Unknown,
        }
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponse {
    #[serde(rename = "Entries")]
    entries: Option<Vec<IpfsLsResponseEntry>>,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response};
    use oio::PageList;
    use opendal_core::*;

    use super::*;
    use crate::IPMFS_SCHEME;
    use crate::backend::IpmfsBuilder;

    struct StaticLsTransport {
        status: StatusCode,
        body: String,
    }

    impl HttpTransport for StaticLsTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Buffer::from(self.body.clone());
            let len = body.len() as u64;
            Ok(Response::builder()
                .status(self.status)
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(len)))
                .unwrap())
        }
    }

    fn make_lister(root: &str, path: &str, status: StatusCode, body: &str) -> IpmfsLister {
        let core = Arc::new(IpmfsCore {
            info: ServiceInfo::new(IPMFS_SCHEME, root, ""),
            capability: Capability {
                list: true,
                ..Default::default()
            },
            root: root.to_string(),
            endpoint: "http://127.0.0.1:5001".to_string(),
        });
        let ctx =
            OperationContext::new().with_http_transport(HttpTransporter::new(StaticLsTransport {
                status,
                body: body.to_string(),
            }));
        IpmfsLister::new(core, ctx, path)
    }

    #[tokio::test]
    async fn test_list_entry_modes_and_metadata() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let body = r#"{
                "Entries": [
                    {"Name": "raw_file", "Type": 0, "Size": 12},
                    {"Name": "subdir", "Type": 1, "Size": 0},
                    {"Name": "unixfs_file", "Type": 2, "Size": 34},
                    {"Name": "meta_node", "Type": 3, "Size": 5},
                    {"Name": "symlink_entry", "Type": 4, "Size": 7},
                    {"Name": "hamt_dir", "Type": 5, "Size": 0},
                    {"Name": "future_type", "Type": 99, "Size": 9}
                ]
            }"#;

            let op =
                Operator::new(IpmfsBuilder::default().endpoint("http://127.0.0.1:5001"))?
                    .with_context(OperationContext::new().with_http_transport(
                        HttpTransporter::new(StaticLsTransport {
                            status: StatusCode::OK,
                            body: body.to_string(),
                        }),
                    ));

            let entries = op.list("/").await?;
            let got: Vec<(&str, EntryMode, u64)> = entries
                .iter()
                .map(|e| (e.path(), e.metadata().mode(), e.metadata().content_length()))
                .collect();

            assert_eq!(
                got,
                vec![
                    ("/", EntryMode::DIR, 0),
                    ("raw_file", EntryMode::FILE, 12),
                    ("subdir/", EntryMode::DIR, 0),
                    ("unixfs_file", EntryMode::FILE, 34),
                    ("meta_node", EntryMode::Unknown, 0),
                    ("symlink_entry", EntryMode::Unknown, 0),
                    ("hamt_dir/", EntryMode::DIR, 0),
                    ("future_type", EntryMode::Unknown, 0),
                ]
            );
            Ok(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_list_non_trailing_slash_path() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let body = r#"{
                "Entries": [
                    {"Name": "a", "Type": 0, "Size": 5},
                    {"Name": "nested", "Type": 1, "Size": 0}
                ]
            }"#;

            for root in ["/", "/mfs/"] {
                for input_path in ["sub", "sub/"] {
                    let lister = make_lister(root, input_path, StatusCode::OK, body);
                    let mut page_ctx = oio::PageContext {
                        done: false,
                        token: String::new(),
                        entries: VecDeque::new(),
                    };

                    lister.next_page(&mut page_ctx).await?;
                    assert!(page_ctx.done);

                    let got: Vec<(String, EntryMode, u64)> = page_ctx
                        .entries
                        .iter()
                        .map(|e| {
                            (
                                e.path().to_string(),
                                e.mode(),
                                e.metadata().content_length(),
                            )
                        })
                        .collect();

                    assert_eq!(
                        got,
                        vec![
                            ("sub/".to_string(), EntryMode::DIR, 0),
                            ("sub/a".to_string(), EntryMode::FILE, 5),
                            ("sub/nested/".to_string(), EntryMode::DIR, 0),
                        ],
                        "root={root} input_path={input_path}"
                    );
                }
            }

            Ok(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_list_not_found_returns_empty() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let lister = make_lister(
                "/",
                "missing",
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"Message":"file does not exist","Code":0,"Type":"error"}"#,
            );
            let mut page_ctx = oio::PageContext {
                done: false,
                token: String::new(),
                entries: VecDeque::new(),
            };

            lister.next_page(&mut page_ctx).await?;
            assert!(page_ctx.done);
            assert!(page_ctx.entries.is_empty());
            Ok(())
        })
        .await
        .expect("test timed out")
    }
}
