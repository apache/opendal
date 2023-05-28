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

use async_trait::async_trait;
use http::Response;

use super::backend::OnedriveBackend;
use super::error::parse_error;
use super::graph_model::GraphApiOnedriveListResponse;
use super::graph_model::ItemType;
use crate::raw::build_rel_path;
use crate::raw::build_rooted_abs_path;
use crate::raw::new_json_deserialize_error;
use crate::raw::oio::{self};
use crate::raw::percent_encode_path;
use crate::raw::IncomingAsyncBody;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct OnedrivePager {
    root: String,
    path: String,
    backend: OnedriveBackend,
    next_link: Option<String>,
    done: bool,
}

impl OnedrivePager {
    const DRIVE_ROOT_PREFIX: &'static str = "/drive/root:";

    pub(crate) fn new(root: String, path: String, backend: OnedriveBackend) -> Self {
        Self {
            root,
            path,
            backend,
            next_link: None,
            done: false,
        }
    }
}

#[async_trait]
impl oio::Page for OnedrivePager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.done {
            return Ok(None);
        }
        let response = self.onedrive_get().await?;

        let status_code = response.status();
        if !status_code.is_success() {
            if status_code == http::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let error = parse_error(response).await?;
            return Err(error);
        }

        let bytes = response.into_body().bytes().await?;
        let decoded_response = serde_json::from_slice::<GraphApiOnedriveListResponse>(&bytes)
            .map_err(new_json_deserialize_error)?;

        if let Some(next_link) = decoded_response.next_link {
            self.next_link = Some(next_link);
        } else {
            self.done = true;
        }

        let entries: Vec<oio::Entry> = decoded_response
            .value
            .into_iter()
            .map(|drive_item| {
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
                entry
            })
            .collect();

        Ok(Some(entries))
    }
}

impl OnedrivePager {
    async fn onedrive_get(&mut self) -> Result<Response<IncomingAsyncBody>> {
        let request_url = if let Some(next_link) = &self.next_link {
            let next_link_clone = next_link.clone();
            self.next_link = None;
            next_link_clone
        } else {
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
        };

        self.backend.onedrive_get_next_list_page(&request_url).await
    }
}
