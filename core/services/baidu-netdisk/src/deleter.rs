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

use crate::core::BaiduNetdiskCore;
use crate::lister::BaiduNetdiskLister;
use opendal_core::{raw::*, *};
use std::sync::Arc;

pub struct BaiduNetdiskDeleter {
    pub core: Arc<BaiduNetdiskCore>,
    pub ctx: OperationContext,
}
impl oio::OneShotDelete for BaiduNetdiskDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let paths = if args.recursive() && !path.ends_with('/') {
            let mut lister = oio::PageLister::new(BaiduNetdiskLister {
                core: self.core.clone(),
                ctx: self.ctx.clone(),
                path: get_parent(&path).into(),
                limit: 1000,
            });
            let mut paths = Vec::new();
            // Collect every matching sibling before deleting: deleting while
            // paginating would shift Baidu's offsets and skip later matches.
            // Directory matches are removed with their contents by filemanager.
            while let Some(entry) = oio::List::next(&mut lister).await? {
                if entry.path().starts_with(&path) {
                    paths.push(self.core.absolute_path(entry.path()));
                }
            }
            paths
        } else {
            // The path sent to filemanager has no trailing slash. Check directory
            // paths so deleting "file/" preserves an existing "file".
            if path.ends_with('/') {
                match self.core.stat(&self.ctx, &path).await {
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
            vec![self.core.absolute_path(&path)]
        };
        for path in paths {
            match self
                .core
                .manage(&self.ctx, "delete", serde_json::json!([path]))
                .await
            {
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                result => result?,
            }
        }
        Ok(())
    }
}
