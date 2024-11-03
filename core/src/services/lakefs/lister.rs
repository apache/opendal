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
use chrono::TimeZone;
use chrono::Utc;

use super::core::LakefsCore;
use super::core::LakefsListResponse;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct LakefsLister {
    core: Arc<LakefsCore>,
    path: String,
    delimiter: &'static str,
    amount: Option<usize>,
    after: Option<String>,
}

impl LakefsLister {
    pub fn new(
        core: Arc<LakefsCore>,
        path: String,
        amount: Option<usize>,
        after: Option<&str>,
        recursive: bool,
    ) -> Self {
        let delimiter = if recursive { "" } else { "/" };
        Self {
            core,
            path,
            delimiter,
            amount,
            after: after.map(String::from),
        }
    }
}

impl oio::PageList for LakefsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let response = self
            .core
            .list_objects(
                &self.path,
                self.delimiter,
                &self.amount,
                // start after should only be set for the first page.
                if ctx.token.is_empty() {
                    self.after.clone()
                } else {
                    None
                },
            )
            .await?;

        let status_code = response.status();
        if !status_code.is_success() {
            let error = parse_error(response);
            return Err(error);
        }

        let bytes = response.into_body();

        let decoded_response: LakefsListResponse =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        ctx.done = true;

        for status in decoded_response.results {
            let entry_type = match status.path_type.as_str() {
                "common_prefix" => EntryMode::DIR,
                "object" => EntryMode::FILE,
                _ => EntryMode::Unknown,
            };

            let mut meta = Metadata::new(entry_type);

            if status.mtime != 0 {
                meta.set_last_modified(Utc.timestamp_opt(status.mtime, 0).unwrap());
            }

            if entry_type == EntryMode::FILE {
                if let Some(size_bytes) = status.size_bytes {
                    meta.set_content_length(size_bytes);
                }
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
