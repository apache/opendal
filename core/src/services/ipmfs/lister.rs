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

use super::backend::IpmfsBackend;
use crate::raw::*;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct IpmfsLister {
    backend: Arc<IpmfsBackend>,
    root: String,
    path: String,
}

impl IpmfsLister {
    pub fn new(backend: Arc<IpmfsBackend>, root: &str, path: &str) -> Self {
        Self {
            backend,
            root: root.to_string(),
            path: path.to_string(),
        }
    }
}

impl oio::PageList for IpmfsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let output = self.backend.ipmfs_ls(&self.path).await?;

        // Mark dir stream has been consumed.
        ctx.done = true;

        for object in output.entries.unwrap_or_default() {
            let path = match object.mode() {
                EntryMode::FILE => format!("{}{}", &self.path, object.name),
                EntryMode::DIR => format!("{}{}/", &self.path, object.name),
                EntryMode::Unknown => unreachable!(),
            };

            let path = build_rel_path(&self.root, &path);

            ctx.entries.push_back(oio::Entry::new(
                &path,
                Metadata::new(object.mode()).with_content_length(object.size),
            ));
        }

        Ok(())
    }
}
