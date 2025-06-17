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
use serde_json;

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// GcsLister takes over task of listing objects and
/// helps walking directory
pub struct GcsLister {
    core: Arc<GcsCore>,

    path: String,
    delimiter: &'static str,
    limit: Option<usize>,
    args: OpList,

    /// Filter results to objects whose names are lexicographically
    /// **equal to or after** startOffset
    start_after: Option<String>,
}

impl GcsLister {
    /// Generate a new directory walker
    pub fn new(core: Arc<GcsCore>, path: &str, args: OpList) -> Self {
        let delimiter = if args.recursive() { "" } else { "/" };
        let start_after = args.start_after().map(String::from);
        Self {
            core,

            path: path.to_string(),
            delimiter,
            limit: args.limit(),
            args,
            start_after,
        }
    }
}

impl oio::PageList for GcsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .gcs_list_objects(
                &self.path,
                &ctx.token,
                self.delimiter,
                self.limit,
                if ctx.token.is_empty() {
                    self.start_after.clone()
                } else {
                    None
                },
                self.args.versions() || self.args.deleted(),
            )
            .await?;

        if !resp.status().is_success() {
            return Err(parse_error(resp));
        }
        let bytes = resp.into_body();

        let output: ListResponse =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        if let Some(token) = &output.next_page_token {
            ctx.token.clone_from(token);
        } else {
            ctx.done = true;
        }

        for prefix in output.prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.core.root, &prefix),
                Metadata::new(EntryMode::DIR),
            );

            ctx.entries.push_back(de);
        }

        for (index, object) in output.items.iter().enumerate() {
            // exclude the inclusive start_after itself
            let mut path = build_rel_path(&self.core.root, &object.name);
            if path.is_empty() {
                path = "/".to_string();
            }
            if self.start_after.as_ref() == Some(&path) {
                continue;
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));

            // set metadata fields
            meta.set_content_md5(object.md5_hash.as_str());
            meta.set_etag(object.etag.as_str());
            meta.set_version(object.generation.as_str());

            let size = object.size.parse().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse u64 from list response").set_source(e)
            })?;
            meta.set_content_length(size);
            if !object.content_type.is_empty() {
                meta.set_content_type(&object.content_type);
            }

            meta.set_last_modified(parse_datetime_from_rfc3339(object.updated.as_str())?);

            let (mut is_latest, mut is_deleted) = (false, false);
            // ref: https://cloud.google.com/storage/docs/json_api/v1/objects/list
            // if versions is true, lists all versions of an object as distinct results in order of increasing generation number.
            // so we need to check if the next item is not the same object, and the object is not deleted.
            // then it is the current version.
            if (index == output.items.len() - 1 || output.items[index + 1].name != object.name)
                && object.time_deleted.is_none()
            {
                meta.set_is_current(true);
                is_latest = true;
            }
            if object.time_deleted.is_some() {
                meta.set_is_deleted(true);
                is_deleted = true;
            }

            let de = oio::Entry::with(path, meta);

            // if deleted is true, we need to include all deleted versions of an object.
            //
            // if versions is true, we need to include all versions of an object.
            // if versions is false, we only include the latest version of an object.
            if (self.args.deleted() && is_deleted) || (self.args.versions() || is_latest) {
                ctx.entries.push_back(de);
            }
        }

        Ok(())
    }
}
