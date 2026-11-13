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
use http::Request;
use opendal_core::{raw::*, *};
use std::sync::Arc;

pub struct BaiduNetdiskLister {
    pub core: Arc<BaiduNetdiskCore>,
    pub ctx: OperationContext,
    pub path: String,
    pub limit: usize,
}
impl oio::PageList for BaiduNetdiskLister {
    async fn next_page(&self, page: &mut oio::PageContext) -> Result<()> {
        let mut entries = Vec::new();
        if page.token.is_empty() {
            let file = match self.core.stat(&self.ctx, &self.path).await {
                Ok(file) => file,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    page.done = true;
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
            let mut path = self.path.clone();
            if file.isdir == 1 && !path.ends_with('/') {
                path.push('/');
            }
            entries.push(oio::Entry::new(&path, file.metadata()?));
            if !self.path.ends_with('/') || file.isdir != 1 {
                page.entries.extend(entries);
                page.done = true;
                return Ok(());
            }
        }
        let start = if page.token.is_empty() {
            0
        } else {
            page.token.parse::<usize>().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "invalid list offset").set_source(e)
            })?
        };
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("method", "list")
            .append_pair("dir", &self.core.absolute_path(&self.path))
            .append_pair("start", &start.to_string())
            .append_pair("limit", &self.limit.to_string())
            .append_pair("order", "name")
            .finish();
        let req = Request::get(format!("{FILE_API}?{query}"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let value = match self.core.send(&self.ctx, req).await {
            Ok(v) => v,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                page.done = true;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let result: FileList = serde_json::from_value(value).map_err(new_json_deserialize_error)?;
        let count = result.list.len();
        for file in result.list {
            let mut path = build_rel_path(&self.core.root, &file.path);
            if file.isdir == 1 {
                path.push('/');
            }
            entries.push(oio::Entry::new(&path, file.metadata()?));
        }
        page.entries.extend(entries);
        page.done = count < self.limit;
        page.token = (start + count).to_string();
        Ok(())
    }
}
