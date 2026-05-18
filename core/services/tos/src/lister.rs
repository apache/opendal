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

use crate::core::*;
use crate::error::parse_error;
use opendal_core::EntryMode;
use opendal_core::Metadata;
use opendal_core::Result;
use opendal_core::raw::*;

pub struct TosLister {
    core: Arc<TosCore>,

    path: String,
    args: OpList,

    delimiter: &'static str,
    start_after: Option<String>,
}

impl TosLister {
    pub fn new(core: Arc<TosCore>, path: &str, args: OpList) -> Self {
        let delimiter = if args.recursive() { "" } else { "/" };
        let start_after = args.start_after().map(ToString::to_string);

        Self {
            core,

            path: path.to_string(),
            args,

            delimiter,
            start_after,
        }
    }
}

impl oio::PageList for TosLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .tos_list_objects_v2(
                &self.path,
                &ctx.token,
                self.delimiter,
                self.args.limit(),
                if ctx.token.is_empty() {
                    self.start_after.clone()
                } else {
                    None
                },
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();
        let output: ListObjectsOutputV2 =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        ctx.done = !output.is_truncated;
        ctx.token = output.next_continuation_token.clone().unwrap_or_default();

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.core.root, &prefix.prefix),
                Metadata::new(EntryMode::DIR),
            );

            ctx.entries.push_back(de);
        }

        for object in output.contents {
            let mut path = build_rel_path(&self.core.root, &object.key);
            if path.is_empty() {
                path = "/".to_string();
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));
            meta.set_is_current(true);
            if let Some(etag) = &object.etag {
                meta.set_etag(etag);
                meta.set_content_md5(etag.trim_matches('"'));
            }
            meta.set_content_length(object.size);
            meta.set_last_modified(object.last_modified.parse::<Timestamp>()?);

            let de = oio::Entry::with(path, meta);
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}
