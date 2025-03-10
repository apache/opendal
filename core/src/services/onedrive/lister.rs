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

use super::core::OneDriveCore;
use super::error::parse_error;
use super::graph_model::GraphApiOneDriveListResponse;
use super::graph_model::ItemType;
use crate::raw::oio;
use crate::raw::*;
use crate::*;

pub struct OneDriveLister {
    core: Arc<OneDriveCore>,
    path: String,
}

impl OneDriveLister {
    const DRIVE_ROOT_PREFIX: &'static str = "/drive/root:";

    pub(crate) fn new(path: String, core: Arc<OneDriveCore>) -> Self {
        Self { core, path }
    }
}

impl oio::PageList for OneDriveLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let request_url = if ctx.token.is_empty() {
            format!(
                "{}:/children",
                OneDriveCore::onedrive_item_url(&self.core.root, &self.path)
            )
        } else {
            ctx.token.clone()
        };

        let response = self.core.onedrive_get_next_list_page(&request_url).await?;

        let status_code = response.status();
        if !status_code.is_success() {
            if status_code == http::StatusCode::NOT_FOUND {
                ctx.done = true;
                return Ok(());
            }
            let error = parse_error(response);
            return Err(error);
        }

        let bytes = response.into_body();
        let decoded_response: GraphApiOneDriveListResponse =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        // Include the current directory itself when handling the first page of the listing.
        if ctx.token.is_empty() && !ctx.done {
            let path = self.path.clone(); //build_rooted_abs_path(&self.core.root, &self.path);
            let full_path = build_rooted_abs_path(&self.core.root, &self.path);
            let trimmed_full_path = full_path.strip_suffix('/').unwrap_or(&full_path);
            // TODO: when listing a directory directly, we could reuse the stat result,
            // cache the result when listing nested directory
            let meta = self.core.onedrive_stat(trimmed_full_path).await?;
            let entry = oio::Entry::new(&path, meta);
            ctx.entries.push_back(entry);
        }

        if let Some(next_link) = decoded_response.next_link {
            ctx.token = next_link;
        } else {
            ctx.done = true;
        }

        for drive_item in decoded_response.value {
            let name = drive_item.name;
            let parent_path = drive_item.parent_reference.path;
            let parent_path = parent_path
                .strip_prefix(Self::DRIVE_ROOT_PREFIX)
                .unwrap_or("");

            let path = format!("{}/{}", parent_path, name);
            let mut normalized_path = build_rel_path(self.core.root.as_str(), path.as_str());
            let entry_mode = match drive_item.item_type {
                ItemType::Folder { .. } => EntryMode::DIR,
                ItemType::File { .. } => EntryMode::FILE,
            };

            // Add the trailing `/` because OneDrive returns a directory with the name
            if entry_mode == EntryMode::DIR {
                normalized_path.push('/');
            }

            let mut meta = Metadata::new(entry_mode).with_etag(drive_item.e_tag);
            let last_modified =
                parse_datetime_from_rfc3339(drive_item.last_modified_date_time.as_str())?;
            meta.set_last_modified(last_modified);
            let content_length = if drive_item.size < 0 {
                0
            } else {
                drive_item.size as u64
            };
            meta.set_content_length(content_length);

            let entry = oio::Entry::new(&normalized_path, meta);
            ctx.entries.push_back(entry)
        }

        Ok(())
    }
}
