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

use super::core::AzfileCore;
use crate::raw::*;
use crate::*;

pub struct AzfileLister {
    core: Arc<AzfileCore>,
    path: String,
    limit: Option<usize>,
}

impl AzfileLister {
    pub fn new(core: Arc<AzfileCore>, path: String, limit: Option<usize>) -> Self {
        Self { core, path, limit }
    }
}

impl oio::PageList for AzfileLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let Some(results) = self
            .core
            .azfile_list(&self.path, &self.limit, &ctx.token)
            .await?
        else {
            ctx.done = true;
            return Ok(());
        };

        if results.next_marker.is_empty() {
            ctx.done = true;
        } else {
            ctx.token = results.next_marker;
        }

        for file in results.entries.file {
            let meta = Metadata::new(EntryMode::FILE)
                .with_etag(file.properties.etag)
                .with_content_length(file.properties.content_length.unwrap_or(0))
                .with_last_modified(parse_datetime_from_rfc2822(&file.properties.last_modified)?);
            let path = self.path.clone().trim_start_matches('/').to_string() + &file.name;
            ctx.entries.push_back(oio::Entry::new(&path, meta));
        }

        for dir in results.entries.directory {
            let meta = Metadata::new(EntryMode::DIR)
                .with_etag(dir.properties.etag)
                .with_last_modified(parse_datetime_from_rfc2822(&dir.properties.last_modified)?);
            let path = self.path.clone().trim_start_matches('/').to_string() + &dir.name + "/";
            ctx.entries.push_back(oio::Entry::new(&path, meta));
        }

        Ok(())
    }
}
