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
use http::StatusCode;

use super::backend::WebhdfsBackend;
use super::error::parse_error;
use super::message::DirectoryListingWrapper;
use super::message::FileStatus;
use super::message::FileStatusType;
use crate::raw::*;
use crate::*;

pub struct WebhdfsPager {
    backend: WebhdfsBackend,
    path: String,
    statuses: Vec<FileStatus>,
    batch_start_after: Option<String>,
    remaining_entries: u32,
}

impl WebhdfsPager {
    pub fn new(backend: WebhdfsBackend, path: &str, statuses: Vec<FileStatus>) -> Self {
        Self {
            backend,
            path: path.to_string(),
            batch_start_after: statuses.last().map(|f| f.path_suffix.clone()),
            statuses,
            remaining_entries: 0,
        }
    }

    pub(super) fn set_remaining_entries(&mut self, remaining_entries: u32) {
        self.remaining_entries = remaining_entries;
    }
}

#[async_trait]
impl oio::Page for WebhdfsPager {
    /// Returns the next page of entries.
    ///
    /// Note: default list status with batch, calling next will query for next batch if `remaining_entries` > 0.
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.statuses.is_empty() && self.remaining_entries == 0 {
            return Ok(None);
        }

        return match self.backend.disable_list_batch {
            true => self.webhdfs_get_next_list_statuses(),
            false => {
                let args = OpList::with_start_after(
                    OpList::default(),
                    &self.batch_start_after.clone().unwrap(),
                );
                let req = self
                    .backend
                    .webhdfs_list_status_batch_request(&self.path, &args)?;
                let resp = self.backend.client.send(req).await?;

                match resp.status() {
                    StatusCode::OK => {
                        let bs = resp.into_body().bytes().await?;
                        let directory_listing =
                            serde_json::from_slice::<DirectoryListingWrapper>(&bs)
                                .map_err(new_json_deserialize_error)?;
                        let file_statuses = directory_listing
                            .directory_listing
                            .partial_listing
                            .file_statuses
                            .file_status;
                        self.remaining_entries =
                            directory_listing.directory_listing.remaining_entries;
                        self.batch_start_after =
                            file_statuses.last().map(|f| f.path_suffix.clone());
                        self.statuses.extend(file_statuses);
                        self.webhdfs_get_next_list_statuses()
                    }
                    StatusCode::NOT_FOUND => self.webhdfs_get_next_list_statuses(),
                    _ => Err(parse_error(resp).await?),
                }
            }
        };
    }
}

impl WebhdfsPager {
    /// Returns the next page of entries.
    fn webhdfs_get_next_list_statuses(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let mut entries = Vec::with_capacity(self.statuses.len());

        while let Some(status) = self.statuses.pop() {
            let mut path = if self.path.is_empty() {
                status.path_suffix.to_string()
            } else {
                format!("{}/{}", self.path, status.path_suffix)
            };

            let meta = match status.ty {
                FileStatusType::Directory => Metadata::new(EntryMode::DIR),
                FileStatusType::File => Metadata::new(EntryMode::FILE)
                    .with_content_length(status.length)
                    .with_last_modified(parse_datetime_from_from_timestamp_millis(
                        status.modification_time,
                    )?),
            };

            if meta.mode().is_file() {
                path = path.trim_end_matches('/').to_string();
            }
            if meta.mode().is_dir() {
                path += "/"
            }
            let entry = oio::Entry::new(&path, meta);
            entries.push(entry);
        }
        Ok(Some(entries))
    }
}
