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

use super::core::GithubCore;
use crate::raw::oio::Entry;
use crate::raw::*;
use crate::*;

pub struct GithubLister {
    core: Arc<GithubCore>,
    path: String,
}

impl GithubLister {
    pub fn new(core: Arc<GithubCore>, path: &str) -> Self {
        Self {
            core,

            path: path.to_string(),
        }
    }
}

impl oio::PageList for GithubLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let entries = self.core.list(&self.path).await?;

        for entry in entries {
            let path = build_rel_path(&self.core.root, &entry.path);
            let entry = if entry.type_field == "dir" {
                let path = format!("{}/", path);
                Entry::new(&path, Metadata::new(EntryMode::DIR))
            } else {
                if path.ends_with(".gitkeep") {
                    continue;
                }
                let m = Metadata::new(EntryMode::FILE)
                    .with_content_length(entry.size)
                    .with_etag(entry.sha);
                Entry::new(&path, m)
            };

            ctx.entries.push_back(entry);
        }

        ctx.done = true;

        Ok(())
    }
}
