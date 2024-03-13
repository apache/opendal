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

use super::backend::OnedriveBackend;
use super::error::parse_error;
use super::graph_model::GraphApiOnedriveListResponse;
use super::graph_model::ItemType;
use crate::raw::oio;
use crate::raw::*;
use crate::*;

pub struct OnedriveLister {
    root: String,
    path: String,
    backend: OnedriveBackend,
}

impl OnedriveLister {
    const DRIVE_ROOT_PREFIX: &'static str = "/drive/root:";

    pub(crate) fn new(root: String, path: String, backend: OnedriveBackend) -> Self {
        Self {
            root,
            path,
            backend,
        }
    }
}

impl oio::PageList for OnedriveLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let request_url = if ctx.token.is_empty() {
            let path = build_rooted_abs_path(&self.root, &self.path);
            let url: String = if path == "." || path == "/" {
                "https://graph.microsoft.com/v1.0/me/drive/root/children".to_string()
            } else {
                // According to OneDrive API examples, the path should not end with a slash.
                // Reference: <https://learn.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_list_children?view=odsp-graph-online>
                let path = path.strip_suffix('/').unwrap_or("");
                format!(
                    "https://graph.microsoft.com/v1.0/me/drive/root:{}:/children",
                    percent_encode_path(path),
                )
            };
            url
        } else {
            ctx.token.clone()
        };

        let resp = self
            .backend
            .onedrive_get_next_list_page(&request_url)
            .await?;

        let status_code = resp.status();
        if !status_code.is_success() {
            if status_code == http::StatusCode::NOT_FOUND {
                ctx.done = true;
                return Ok(());
            }
            let error = parse_error(resp).await?;
            return Err(error);
        }

        let bytes = resp.into_body().bytes().await?;
        let decoded_response = serde_json::from_slice::<GraphApiOnedriveListResponse>(&bytes)
            .map_err(new_json_deserialize_error)?;

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

            let normalized_path = build_rel_path(&self.root, &path);

            let entry: oio::Entry = match drive_item.item_type {
                ItemType::Folder { .. } => {
                    let normalized_path = format!("{}/", normalized_path);
                    oio::Entry::new(&normalized_path, Metadata::new(EntryMode::DIR))
                }
                ItemType::File { .. } => {
                    oio::Entry::new(&normalized_path, Metadata::new(EntryMode::FILE))
                }
            };

            ctx.entries.push_back(entry)
        }

        Ok(())
    }
}
