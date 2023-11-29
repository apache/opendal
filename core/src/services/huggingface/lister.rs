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

use async_trait::async_trait;

use super::core::HuggingfaceCore;
use super::error::parse_error;
use super::message::HuggingfaceStatus;
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

#[async_trait]
impl oio::PageList for HuggingfaceLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let response = self.core.hf_list(&self.path, self.recursive).await?;

        let status_code = response.status();
        if !status_code.is_success() {
            let error = parse_error(response).await?;
            return Err(error);
        }

        let bytes = response.into_body().bytes().await?;
        let decoded_response = serde_json::from_slice::<Vec<HuggingfaceStatus>>(&bytes)
            .map_err(new_json_deserialize_error)?;

        ctx.done = true;

        for status in decoded_response {
            let entry: oio::Entry = match status.type_.as_str() {
                "directory" => {
                    let normalized_path = format!("{}/", &status.path);
                    let mut meta = Metadata::new(EntryMode::DIR);
                    if let Some(commit_info) = status.last_commit.as_ref() {
                        meta.set_last_modified(parse_datetime_from_rfc3339(
                            commit_info.date.as_str(),
                        )?);
                    }
                    oio::Entry::new(&normalized_path, meta)
                }
                "file" => {
                    let mut meta = Metadata::new(EntryMode::FILE);
                    if let Some(commit_info) = status.last_commit.as_ref() {
                        meta.set_last_modified(parse_datetime_from_rfc3339(
                            commit_info.date.as_str(),
                        )?);
                    }
                    meta.set_content_length(status.size);
                    oio::Entry::new(&status.path, meta)
                }
                _ => {
                    let mut meta = Metadata::new(EntryMode::Unknown);
                    if let Some(commit_info) = status.last_commit.as_ref() {
                        meta.set_last_modified(parse_datetime_from_rfc3339(
                            commit_info.date.as_str(),
                        )?);
                    }
                    oio::Entry::new(&status.path, meta)
                }
            };
            ctx.entries.push_back(entry);
        }
        Ok(())
    }
}
