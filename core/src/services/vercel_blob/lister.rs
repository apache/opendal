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

use super::core::parse_blob;
use super::core::VercelBlobCore;
use crate::raw::oio::Entry;
use crate::raw::*;
use crate::Result;

pub struct VercelBlobLister {
    core: Arc<VercelBlobCore>,

    path: String,
    limit: Option<usize>,
}

impl VercelBlobLister {
    pub(super) fn new(core: Arc<VercelBlobCore>, path: &str, limit: Option<usize>) -> Self {
        VercelBlobLister {
            core,
            path: path.to_string(),
            limit,
        }
    }
}

impl oio::PageList for VercelBlobLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let p = build_abs_path(&self.core.root, &self.path);

        let resp = self.core.list(&p, self.limit).await?;

        ctx.done = !resp.has_more;

        if let Some(cursor) = resp.cursor {
            ctx.token = cursor;
        }

        for blob in resp.blobs {
            let path = build_rel_path(&self.core.root, &blob.pathname);

            if path == self.path {
                continue;
            }

            let md = parse_blob(&blob)?;

            ctx.entries.push_back(Entry::new(&path, md));
        }

        Ok(())
    }
}
