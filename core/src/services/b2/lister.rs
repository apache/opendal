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

use async_trait::async_trait;
use bytes::Buf;

use super::core::parse_file_info;
use super::core::B2Core;
use super::core::ListFileNamesResponse;
use crate::raw::*;
use crate::services::b2::error::parse_error;
use crate::*;

pub struct B2Lister {
    core: Arc<B2Core>,

    path: String,
    delimiter: Option<&'static str>,
    limit: Option<usize>,

    /// B2 starts listing **after** this specified key
    start_after: Option<String>,
}

impl B2Lister {
    pub fn new(
        core: Arc<B2Core>,
        path: &str,
        recursive: bool,
        limit: Option<usize>,
        start_after: Option<&str>,
    ) -> Self {
        let delimiter = if recursive { None } else { Some("/") };
        Self {
            core,

            path: path.to_string(),
            delimiter,
            limit,
            start_after: start_after.map(String::from),
        }
    }
}

#[async_trait]
impl oio::PageList for B2Lister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .list_file_names(
                Some(&self.path),
                self.delimiter,
                self.limit,
                self.start_after.clone(),
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp).await?);
        }

        let bs = resp.into_body().bytes().await?;

        let output: ListFileNamesResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        ctx.done = output.next_file_name.is_none();

        for file in output.files {
            if let Some(start_after) = self.start_after.clone() {
                if build_abs_path(&self.core.root, &start_after) == file.file_name {
                    continue;
                }
            }
            if file.file_name == build_abs_path(&self.core.root, &self.path) {
                continue;
            }
            let file_name = file.file_name.clone();
            let metadata = parse_file_info(&file);

            ctx.entries.push_back(oio::Entry::new(
                &build_rel_path(&self.core.root, &file_name),
                metadata,
            ))
        }

        Ok(())
    }
}
