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






use crate::raw::*;
use crate::services::dbfs::core::DbfsCore;
use crate::*;

pub struct DbfsLister {
    core: Arc<DbfsCore>,
    path: String,
}

impl DbfsLister {
    pub fn new(core: Arc<DbfsCore>, path: String) -> Self {
        Self { core, path }
    }
}

impl oio::PageList for DbfsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let Some(decoded_response) = self.core.dbfs_list(&self.path).await? else {
            ctx.done = true;
            return Ok(());
        };

        for status in decoded_response.files {
            let entry: oio::Entry = match status.is_dir {
                true => {
                    let normalized_path = format!("{}/", &status.path);
                    let mut meta = Metadata::new(EntryMode::DIR);
                    meta.set_last_modified(parse_datetime_from_from_timestamp_millis(
                        status.modification_time,
                    )?);
                    oio::Entry::new(&normalized_path, meta)
                }
                false => {
                    let mut meta = Metadata::new(EntryMode::FILE);
                    meta.set_last_modified(parse_datetime_from_from_timestamp_millis(
                        status.modification_time,
                    )?);
                    meta.set_content_length(status.file_size as u64);
                    oio::Entry::new(&status.path, meta)
                }
            };
            ctx.entries.push_back(entry);
        }
        ctx.done = true;
        Ok(())
    }
}
