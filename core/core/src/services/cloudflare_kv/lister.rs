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

use super::core::CloudflareKvCore;
use super::error::parse_error;
use crate::raw::*;
use crate::services::cloudflare_kv::model::{CfKvListKey, CfKvListResponse};
use crate::*;

pub struct CloudflareKvLister {
    core: Arc<CloudflareKvCore>,

    path: String,
    limit: Option<usize>,
    recursive: bool,
}

impl CloudflareKvLister {
    pub fn new(
        core: Arc<CloudflareKvCore>,
        path: &str,
        recursive: bool,
        limit: Option<usize>,
    ) -> Self {
        Self {
            core,

            path: path.to_string(),
            limit,
            recursive,
        }
    }

    fn build_entry_for_item(&self, item: &CfKvListKey, root: &str) -> Result<oio::Entry> {
        let metadata = item.metadata.clone();
        let mut name = item.name.clone();

        if metadata.is_dir && !name.ends_with('/') {
            name += "/";
        }

        let mut name = name.replace(root.trim_start_matches('/'), "");

        // If it is the root directory, it needs to be processed as /
        if name.is_empty() {
            name = "/".to_string();
        }

        let entry_metadata = if name.ends_with('/') {
            Metadata::new(EntryMode::DIR)
                .with_etag(build_tmp_path_of(&name))
                .with_content_length(0)
        } else {
            Metadata::new(EntryMode::FILE)
                .with_etag(metadata.etag)
                .with_content_length(metadata.content_length as u64)
                .with_last_modified(metadata.last_modified.parse::<Timestamp>()?)
        };

        Ok(oio::Entry::new(&name, entry_metadata))
    }

    fn handle_non_recursive_file_list(
        &self,
        ctx: &mut oio::PageContext,
        result: &[CfKvListKey],
        root: &str,
    ) -> Result<()> {
        if let Some(item) = result.iter().find(|item| item.name == self.path) {
            let entry = self.build_entry_for_item(item, root)?;
            ctx.entries.push_back(entry);
        } else if !result.is_empty() {
            let path_name = self.path.replace(root.trim_start_matches('/'), "");
            let entry = oio::Entry::new(
                &format!("{path_name}/"),
                Metadata::new(EntryMode::DIR)
                    .with_etag(build_tmp_path_of(&path_name))
                    .with_content_length(0),
            );
            ctx.entries.push_back(entry);
        }
        ctx.done = true;
        Ok(())
    }
}

impl oio::PageList for CloudflareKvLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let new_path = self.path.trim_end_matches('/');
        let resp = self
            .core
            .list(new_path, self.limit, Some(ctx.token.clone()))
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();
        let res: CfKvListResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        if !res.success {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "oss list this key failed for reason we don't know",
            ));
        }

        let (token, done) = res
            .result_info
            .and_then(|info| info.cursor)
            .map_or((String::new(), true), |cursor| {
                (cursor.clone(), cursor.is_empty())
            });

        ctx.token = token;
        ctx.done = done;

        if let Some(result) = res.result {
            let root = self.core.info.root().to_string();

            if !self.path.ends_with('/') && !self.recursive {
                self.handle_non_recursive_file_list(ctx, &result, &root)?;
                return Ok(());
            }

            for item in result {
                let mut name = item.name.clone();
                if item.metadata.is_dir && !name.ends_with('/') {
                    name += "/";
                }

                // For non-recursive listing, filter out entries not in the current directory.
                if !self.recursive {
                    if let Some(relative_path) = name.strip_prefix(&self.path) {
                        if relative_path.trim_end_matches('/').contains('/') {
                            continue;
                        }
                    } else if self.path != name {
                        continue;
                    }
                }

                let entry = self.build_entry_for_item(&item, &root)?;
                ctx.entries.push_back(entry);
            }
        }

        Ok(())
    }
}
