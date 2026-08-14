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

use super::core::MetainformationResponse;
use super::core::YandexDiskCore;
use super::core::parse_error;
use super::core::parse_info;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::oio::Entry;
use opendal_core::raw::*;

pub struct YandexDiskLister {
    core: Arc<YandexDiskCore>,
    ctx: OperationContext,

    path: String,
    limit: Option<usize>,
}

impl YandexDiskLister {
    pub(super) fn new(
        core: Arc<YandexDiskCore>,
        ctx: OperationContext,
        path: &str,
        limit: Option<usize>,
    ) -> Self {
        YandexDiskLister {
            core,
            ctx,
            path: path.to_string(),
            limit,
        }
    }
}

impl oio::PageList for YandexDiskLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let offset = if ctx.token.is_empty() {
            None
        } else {
            Some(ctx.token.clone())
        };

        let resp = self
            .core
            .metainformation(&self.ctx, &self.path, self.limit, offset)
            .await?;

        if resp.status() == http::StatusCode::NOT_FOUND {
            ctx.done = true;
            return Ok(());
        }

        match resp.status() {
            http::StatusCode::OK => {
                let body = resp.into_body();

                let resp: MetainformationResponse =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                consume_page(&self.core.root, resp, ctx)
            }
            http::StatusCode::NOT_FOUND => {
                ctx.done = true;
                Ok(())
            }
            _ => Err(parse_error(resp)),
        }
    }
}

/// Turn one metainformation response into an entry.
///
/// Returns `None` for a response whose path does not carry the `disk:` prefix, matching what the
/// listing loop did with such a path before.
fn build_entry(root: &str, mf: MetainformationResponse) -> Result<Option<Entry>> {
    let Some(rel) = mf
        .path
        .strip_prefix("disk:")
        .map(|p| build_rel_path(root, p))
    else {
        return Ok(None);
    };

    let md = parse_info(mf)?;

    let path = if md.mode().is_dir() {
        format!("{rel}/")
    } else {
        rel
    };

    Ok(Some(Entry::new(&path, md)))
}

/// Fold one metainformation response into the page context.
///
/// A folder answers with `_embedded`, whose items are the entries. A **file** has no `_embedded`
/// at all -- the response is that file's own metainformation -- and listing a file path is a
/// documented contract: `test_list_prefix` writes a file and lists its exact path, expecting one
/// `FILE` entry back.
///
/// That branch used to fall through without touching `done` or pushing anything, and `done` is
/// exactly what `PageLister::next` loops on, so the same request was re-issued for ever.
fn consume_page(
    root: &str,
    mut resp: MetainformationResponse,
    ctx: &mut oio::PageContext,
) -> Result<()> {
    let Some(embedded) = resp.embedded.take() else {
        if let Some(entry) = build_entry(root, resp)? {
            ctx.entries.push_back(entry);
        }
        ctx.done = true;
        return Ok(());
    };

    let n = embedded.items.len();

    for mf in embedded.items {
        if let Some(entry) = build_entry(root, mf)? {
            ctx.entries.push_back(entry);
        }
    }

    let current_len = ctx.token.parse::<usize>().unwrap_or(0) + n;

    if current_len >= embedded.total {
        ctx.done = true;
    }

    ctx.token = current_len.to_string();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::EntryMode;
    use std::collections::VecDeque;

    use super::super::core::Embedded;

    fn ctx() -> oio::PageContext {
        oio::PageContext {
            done: false,
            token: String::new(),
            entries: VecDeque::new(),
        }
    }

    fn info(ty: &str, path: &str) -> MetainformationResponse {
        MetainformationResponse {
            ty: ty.to_string(),
            path: path.to_string(),
            modified: "2024-01-01T00:00:00Z".to_string(),
            md5: None,
            mime_type: None,
            size: Some(7),
            embedded: None,
        }
    }

    #[test]
    fn a_file_response_yields_the_file_and_ends_the_listing() {
        // A file carries no `_embedded`. Leaving `done` false here is what made `PageLister`
        // re-issue the identical request for ever.
        let mut c = ctx();

        consume_page("/", info("file", "disk:/a.txt"), &mut c).expect("consume");

        assert!(
            c.done,
            "a response without `_embedded` must end the listing"
        );
        assert_eq!(c.entries.len(), 1);
        assert_eq!(c.entries[0].path(), "a.txt");
        assert_eq!(c.entries[0].metadata().mode(), EntryMode::FILE);
    }

    #[test]
    fn a_complete_folder_page_ends_the_listing() {
        let mut c = ctx();
        let mut folder = info("dir", "disk:/d");
        folder.embedded = Some(Embedded {
            total: 2,
            items: vec![info("file", "disk:/d/a.txt"), info("dir", "disk:/d/sub")],
        });

        consume_page("/", folder, &mut c).expect("consume");

        assert!(c.done);
        assert_eq!(c.entries[0].path(), "d/a.txt");
        assert_eq!(c.entries[1].path(), "d/sub/");
        assert_eq!(c.token, "2");
    }

    #[test]
    fn a_partial_folder_page_advances_the_token() {
        let mut c = ctx();
        let mut folder = info("dir", "disk:/d");
        folder.embedded = Some(Embedded {
            total: 5,
            items: vec![info("file", "disk:/d/a.txt")],
        });

        consume_page("/", folder, &mut c).expect("consume");

        assert!(!c.done, "more items remain, so the listing must continue");
        assert_eq!(c.token, "1");
    }
}
