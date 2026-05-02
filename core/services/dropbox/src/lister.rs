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

use super::core::*;
use super::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub struct DropboxLister {
    core: Arc<DropboxCore>,
    list_path: String,
    recursive: bool,
    limit: Option<usize>,
    self_entry: Option<oio::Entry>,
    filter_prefix: Option<String>,
    fetch_entries: bool,
}

impl DropboxLister {
    pub fn new(
        core: Arc<DropboxCore>,
        list_path: String,
        recursive: bool,
        limit: Option<usize>,
        self_entry: Option<oio::Entry>,
        filter_prefix: Option<String>,
        fetch_entries: bool,
    ) -> Self {
        Self {
            core,
            list_path,
            recursive,
            limit,
            self_entry,
            filter_prefix,
            fetch_entries,
        }
    }

    fn build_entry(&self, entry: DropboxMetadataResponse) -> Result<oio::Entry> {
        let path = entry.entry_path(self.core.root.as_str());
        let metadata = entry.parse_metadata()?;
        Ok(oio::Entry::new(&path, metadata))
    }
}

impl oio::PageList for DropboxLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        if ctx.token.is_empty() {
            if let Some(entry) = self.self_entry.clone() {
                ctx.entries.push_back(entry);
            }

            if !self.fetch_entries {
                ctx.done = true;
                return Ok(());
            }
        }

        // The token is set when obtaining entries and returning `has_more` flag.
        // When the token exists, we should retrieve more entries using the Dropbox continue API.
        // Refer: https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder-continue
        let response = if !ctx.token.is_empty() {
            self.core.dropbox_list_continue(&ctx.token).await?
        } else {
            self.core
                .dropbox_list(&self.list_path, self.recursive, self.limit)
                .await?
        };

        let status_code = response.status();

        if !status_code.is_success() {
            let error = parse_error(response);

            let result = match error.kind() {
                ErrorKind::NotFound => Ok(()),
                _ => Err(error),
            };

            ctx.done = true;
            return result;
        }

        let bytes = response.into_body();
        let decoded_response: DropboxListResponse =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        for entry in decoded_response.entries {
            let entry = self.build_entry(entry)?;

            if let Some(prefix) = self.filter_prefix.as_deref() {
                if !entry.path().starts_with(prefix) {
                    continue;
                }
            }
            if let Some(self_entry) = self.self_entry.as_ref() {
                if self_entry.path() == entry.path() {
                    continue;
                }
            }

            ctx.entries.push_back(entry);
        }

        if decoded_response.has_more {
            ctx.token = decoded_response.cursor;
            ctx.done = false;
        } else {
            ctx.done = true;
        }
        Ok(())
    }
}
