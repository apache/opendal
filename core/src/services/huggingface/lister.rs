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

use super::core::HuggingfaceCore;
use super::core::HuggingfaceStatus;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct HuggingfaceLister {
    core: Arc<HuggingfaceCore>,
    path: String,
    recursive: bool,
}

impl HuggingfaceLister {
    pub fn new(core: Arc<HuggingfaceCore>, path: String, recursive: bool) -> Self {
        Self {
            core,
            path,
            recursive,
        }
    }
}

impl oio::PageList for HuggingfaceLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let response = self.core.hf_list(&self.path, self.recursive).await?;

        let status_code = response.status();
        if !status_code.is_success() {
            let error = parse_error(response);
            return Err(error);
        }

        let bytes = response.into_body();
        let decoded_response: Vec<HuggingfaceStatus> =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        ctx.done = true;

        for status in decoded_response {
            let entry_type = match status.type_.as_str() {
                "directory" => EntryMode::DIR,
                "file" => EntryMode::FILE,
                _ => EntryMode::Unknown,
            };

            let mut meta = Metadata::new(entry_type);

            if let Some(commit_info) = status.last_commit.as_ref() {
                meta.set_last_modified(parse_datetime_from_rfc3339(commit_info.date.as_str())?);
            }

            if entry_type == EntryMode::FILE {
                meta.set_content_length(status.size);
            }

            let path = if entry_type == EntryMode::DIR {
                format!("{}/", &status.path)
            } else {
                status.path.clone()
            };

            ctx.entries.push_back(oio::Entry::new(
                &build_rel_path(&self.core.root, &path),
                meta,
            ));
        }

        Ok(())
    }
}
