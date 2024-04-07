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



use super::core::*;

use crate::raw::*;
use crate::*;

pub struct WebdavLister {
    core: Arc<WebdavCore>,

    path: String,
    args: OpList,
}

impl WebdavLister {
    pub fn new(core: Arc<WebdavCore>, path: &str, args: OpList) -> Self {
        Self {
            core,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::PageList for WebdavLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let Some((is_jfrog_artifactory, result)) =
            self.core.webdav_list(&self.path, &self.args).await?
        else {
            ctx.done = true;
            return Ok(());
        };

        for res in result.response {
            let mut path = res
                .href
                .strip_prefix(&self.core.server_path)
                .unwrap_or(&res.href)
                .to_string();

            let meta = parse_propstat(&res.propstat)?;

            // Append `/` to path if it's a dir
            if !path.ends_with('/') && meta.is_dir() {
                path += "/"
            }

            // Ignore the root path itself.
            if self.core.root == path {
                continue;
            }

            let normalized_path = build_rel_path(&self.core.root, &path);
            let decoded_path = percent_decode_path(&normalized_path);

            if normalized_path == self.path || decoded_path == self.path {
                // WebDAV server may return the current path as an entry.
                continue;
            }

            // HACKS! HACKS! HACKS!
            //
            // jfrog artifactory will generate a virtual checksum file for each file.
            // The checksum file can't be stated, but can be listed and read.
            // We ignore the checksum files to avoid listing unexpected files.
            if is_jfrog_artifactory && meta.content_type() == Some("application/x-checksum") {
                continue;
            }

            ctx.entries.push_back(oio::Entry::new(&decoded_path, meta))
        }
        ctx.done = true;

        Ok(())
    }
}
