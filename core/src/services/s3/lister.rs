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
use quick_xml::de;

use super::core::ListObjectsOutput;
use super::core::S3Core;
use super::error::parse_error;
use crate::raw::*;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct S3Lister {
    core: Arc<S3Core>,

    path: String,
    delimiter: &'static str,
    limit: Option<usize>,

    /// Amazon S3 starts listing **after** this specified key
    start_after: Option<String>,
}

impl S3Lister {
    pub fn new(
        core: Arc<S3Core>,
        path: &str,
        recursive: bool,
        limit: Option<usize>,
        start_after: Option<&str>,
    ) -> Self {
        let delimiter = if recursive { "" } else { "/" };
        Self {
            core,

            path: path.to_string(),
            delimiter,
            limit,
            start_after: start_after.map(String::from),
        }
    }
}

impl oio::PageList for S3Lister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .s3_list_objects(
                &self.path,
                &ctx.token,
                self.delimiter,
                self.limit,
                // State after should only be set for the first page.
                if ctx.token.is_empty() {
                    self.start_after.clone()
                } else {
                    None
                },
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp).await?);
        }

        let bs = resp.into_body().bytes().await?;

        let output: ListObjectsOutput =
            de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        // Try our best to check whether this list is done.
        //
        // - Check `is_truncated`
        // - Check `next_continuation_token`
        // - Check the length of `common_prefixes` and `contents` (very rarely case)
        ctx.done = if let Some(is_truncated) = output.is_truncated {
            !is_truncated
        } else if let Some(next_continuation_token) = output.next_continuation_token.as_ref() {
            next_continuation_token.is_empty()
        } else {
            output.common_prefixes.is_empty() && output.contents.is_empty()
        };
        ctx.token = output.next_continuation_token.clone().unwrap_or_default();

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.core.root, &prefix.prefix),
                Metadata::new(EntryMode::DIR),
            );

            ctx.entries.push_back(de);
        }

        for object in output.contents {
            let path = build_rel_path(&self.core.root, &object.key);

            // s3 could return the dir itself in contents.
            if path == self.path || path.is_empty() {
                continue;
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));

            if let Some(etag) = &object.etag {
                meta.set_etag(etag);
                meta.set_content_md5(etag.trim_matches('"'));
            }
            meta.set_content_length(object.size);

            // object.last_modified provides more precious time that contains
            // nanosecond, let's trim them.
            meta.set_last_modified(parse_datetime_from_rfc3339(object.last_modified.as_str())?);

            let de = oio::Entry::with(path, meta);
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}
