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

use bytes::Buf;

use super::core::CloudflareKvCore;
use super::error::parse_error;
use crate::raw::*;
use crate::services::cloudflare_kv::model::CfKvListResponse;
use crate::*;

pub struct CloudflareKvLister {
    core: Arc<CloudflareKvCore>,

    path: String,
    limit: Option<usize>,
}

impl CloudflareKvLister {
    pub fn new(core: Arc<CloudflareKvCore>, path: &str, limit: Option<usize>) -> Self {
        Self {
            core,

            path: path.to_string(),
            limit,
        }
    }
}

impl oio::PageList for CloudflareKvLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let new_path = self.path.trim_end_matches('/');
        let resp = self
            .core
            .list(new_path, self.limit, Some(ctx.token.clone()))
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();
        let res: CfKvListResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        if !res.success {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "oss list this key failed for reason we don't know",
            ));
        }

        let (token, done) = res
            .result_info
            .and_then(|info| info.cursor)
            .map_or((String::new(), true), |cursor| {
                (cursor.clone(), cursor.is_empty())
            });

        ctx.token = token;
        ctx.done = done;

        if let Some(result) = res.result {
            for item in result {
                let metadata = item.metadata;

                let mut name = item.name;
                if metadata.is_dir && !name.ends_with('/') {
                    name += "/";
                }
                let root = self.core.info.root().as_ref().to_string();
                name = name.replace(root.trim_start_matches('/'), "");

                let de = oio::Entry::new(
                    &name,
                    Metadata::new(EntryMode::from_path(&name))
                        .with_etag(metadata.etag)
                        .with_content_length(metadata.content_length as u64)
                        .with_last_modified(parse_datetime_from_rfc3339(&metadata.last_modified)?),
                );
                ctx.entries.push_back(de);
            }
        }

        Ok(())
    }
}
