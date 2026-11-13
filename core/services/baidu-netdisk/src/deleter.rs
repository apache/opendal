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

use crate::core::{BaiduNetdiskCore, FILE_API, FileList};
use crate::lister::BaiduNetdiskLister;
use http::Request;
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
            let file = match self.core.stat(&self.ctx, &path).await {
                Ok(file) => file,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e),
            };
            if !args.recursive() && file.isdir == 1 {
                // Baidu always deletes directories recursively. Check for children
                // before issuing a non-recursive delete, including paths without '/'.
                let query = form_urlencoded::Serializer::new(String::new())
                    .append_pair("method", "list")
                    .append_pair("dir", &self.core.absolute_path(&path))
                    .append_pair("start", "0")
                    .append_pair("limit", "1")
                    .finish();
                let req = Request::get(format!("{FILE_API}?{query}"))
                    .body(Buffer::new())
                    .map_err(new_request_build_error)?;
                let value = match self.core.send(&self.ctx, req).await {
                    Ok(value) => value,
                    Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
                    Err(e) => return Err(e),
                };
                let children: FileList =
                    serde_json::from_value(value).map_err(new_json_deserialize_error)?;
                if !children.list.is_empty() {
                    return Err(Error::new(
                        ErrorKind::IsADirectory,
                        "cannot delete a non-empty directory without recursive=true",
                    ));
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
