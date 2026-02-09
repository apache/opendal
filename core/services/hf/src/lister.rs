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

use super::core::HfCore;
use opendal_core::raw::*;
use opendal_core::*;

pub struct HfLister {
    core: Arc<HfCore>,
    path: String,
    recursive: bool,
}

impl HfLister {
    pub fn new(core: Arc<HfCore>, path: String, recursive: bool) -> Self {
        Self {
            core,
            path,
            recursive,
        }
    }
}

impl oio::PageList for HfLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let cursor = if ctx.token.is_empty() {
            None
        } else {
            Some(ctx.token.as_str())
        };
        let response = self
            .core
            .file_tree(&self.path, self.recursive, cursor)
            .await?;

        if let Some(next_cursor) = response.next_cursor {
            ctx.token = next_cursor;
        } else {
            ctx.done = true;
        }

        for info in response.files {
            let meta = info.metadata()?;
            let path = if meta.mode() == EntryMode::DIR {
                format!("{}/", &info.path)
            } else {
                info.path.clone()
            };
            ctx.entries.push_back(oio::Entry::new(
                &build_rel_path(&self.core.root, &path),
                meta,
            ));
        }

        Ok(())
    }
}
