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

use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

use super::core::SharePointCore;
use super::core::parse_error;
use super::graph_model::GraphApiSharePointListResponse;
use super::graph_model::ItemType;

pub struct SharePointLister {
    core: Arc<SharePointCore>,
    ctx: OperationContext,
    capability: Capability,
    path: String,
    op: OpList,
}

impl SharePointLister {
    pub(crate) fn new(
        path: String,
        core: Arc<SharePointCore>,
        ctx: OperationContext,
        capability: Capability,
        args: &OpList,
    ) -> Self {
        Self {
            core,
            ctx,
            capability,
            path,
            op: args.clone(),
        }
    }

    /// The prefix that every child of the listed directory is reported under.
    ///
    /// Entries are named relative to the operator root, and every item in a
    /// `children` response is a direct child of the directory being listed, so
    /// the listed path is the prefix.
    ///
    /// This deliberately does not derive paths from `parentReference.path`. On
    /// SharePoint that field is relative to the *drive* root
    /// (`/drives/{drive-id}/root:/...`), while this service is anchored to an
    /// arbitrary folder inside the drive, so the two are not interchangeable.
    fn entry_prefix(&self) -> String {
        if self.path == "/" || self.path.is_empty() {
            String::new()
        } else if self.path.ends_with('/') {
            self.path.clone()
        } else {
            format!("{}/", self.path)
        }
    }
}

impl oio::PageList for SharePointLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let response = if ctx.token.is_empty() {
            self.core
                .sharepoint_list(&self.ctx, &self.path, self.op.limit())
                .await?
        } else {
            self.core
                .sharepoint_get_next_list_page(&self.ctx, &ctx.token)
                .await?
        };

        let status_code = response.status();
        if !status_code.is_success() {
            if status_code == http::StatusCode::NOT_FOUND {
                ctx.done = true;
                return Ok(());
            }
            return Err(parse_error(response));
        }

        let bytes = response.into_body();
        let decoded_response: GraphApiSharePointListResponse =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        let list_with_versions = self.capability.list_with_versions;

        // Include the current directory itself when handling the first page of the listing.
        if ctx.token.is_empty() && !ctx.done {
            // TODO: when listing a directory directly, we could reuse the stat result,
            // cache the result when listing nested directory
            let path = if self.path == "/" {
                "".to_string()
            } else {
                self.path.clone()
            };

            let meta = self
                .core
                .sharepoint_stat(&self.ctx, &path, OpStat::default())
                .await?;

            // skip `list_with_versions` intentionally because a folder doesn't have versions

            let entry = oio::Entry::new(&path, meta);
            ctx.entries.push_back(entry);
        }

        if let Some(next_link) = decoded_response.next_link {
            ctx.token = next_link;
        } else {
            ctx.done = true;
        }

        let prefix = self.entry_prefix();

        for drive_item in decoded_response.value {
            let mut path = format!("{prefix}{}", drive_item.name);

            let entry_mode = match drive_item.item_type {
                ItemType::Folder { .. } => EntryMode::DIR,
                ItemType::File { .. } => EntryMode::FILE,
            };

            // Add the trailing `/` because Graph returns a directory with the name
            if entry_mode == EntryMode::DIR {
                path.push('/');
            }

            let mut meta = Metadata::new(entry_mode)
                .with_etag(drive_item.e_tag)
                .with_content_length(drive_item.size.max(0) as u64);
            let last_modified = drive_item.last_modified_date_time.parse::<Timestamp>()?;
            meta.set_last_modified(last_modified);

            // When listing a directory with `$expand=versions`, Graph returns 400 "Operation not supported".
            // Thus, `list_with_versions` induces N+1 requests. This N+1 is intentional.
            // N+1 is horrendous but we can't do any better without Graph's API support.
            // When Graph supports listing with versions API, remove this.
            if list_with_versions {
                let versions = self.core.sharepoint_list_versions(&self.ctx, &path).await?;
                if let Some(version) = versions.first() {
                    meta.set_version(&version.id);
                }
            }

            let entry = oio::Entry::new(&path, meta);
            ctx.entries.push_back(entry)
        }

        Ok(())
    }
}
