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

use super::core::*;
use super::error::parse_error;
use crate::raw::oio::PageContext;
use crate::raw::*;
use crate::EntryMode;
use crate::Error;
use crate::Metadata;
use crate::Result;

pub type CosListers = TwoWays<oio::PageLister<CosLister>, oio::PageLister<CosObjectVersionsLister>>;

pub struct CosLister {
    core: Arc<CosCore>,
    path: String,
    delimiter: &'static str,
    limit: Option<usize>,
}

impl CosLister {
    pub fn new(core: Arc<CosCore>, path: &str, recursive: bool, limit: Option<usize>) -> Self {
        let delimiter = if recursive { "" } else { "/" };
        Self {
            core,
            path: path.to_string(),
            delimiter,
            limit,
        }
    }
}

impl oio::PageList for CosLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .cos_list_objects(&self.path, &ctx.token, self.delimiter, self.limit)
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();

        let output: ListObjectsOutput =
            de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        // Try our best to check whether this list is done.
        //
        // - Check `next_marker`
        ctx.done = match output.next_marker.as_ref() {
            None => true,
            Some(next_marker) => next_marker.is_empty(),
        };
        ctx.token = output.next_marker.clone().unwrap_or_default();

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

            let meta = Metadata::new(EntryMode::from_path(&path)).with_content_length(object.size);

            let de = oio::Entry::with(path, meta);
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

/// refer: https://cloud.tencent.com/document/product/436/35521
pub struct CosObjectVersionsLister {
    core: Arc<CosCore>,

    prefix: String,
    args: OpList,

    delimiter: &'static str,
    abs_start_after: Option<String>,
}

impl CosObjectVersionsLister {
    pub fn new(core: Arc<CosCore>, path: &str, args: OpList) -> Self {
        let delimiter = if args.recursive() { "" } else { "/" };
        let abs_start_after = args
            .start_after()
            .map(|start_after| build_abs_path(&core.root, start_after));

        Self {
            core,
            prefix: path.to_string(),
            args,
            delimiter,
            abs_start_after,
        }
    }
}

impl oio::PageList for CosObjectVersionsLister {
    async fn next_page(&self, ctx: &mut PageContext) -> Result<()> {
        let markers = ctx.token.rsplit_once(" ");
        let (key_marker, version_id_marker) = if let Some(data) = markers {
            data
        } else if let Some(start_after) = &self.abs_start_after {
            (start_after.as_str(), "")
        } else {
            ("", "")
        };

        let resp = self
            .core
            .cos_list_object_versions(
                &self.prefix,
                self.delimiter,
                self.args.limit(),
                key_marker,
                version_id_marker,
            )
            .await?;
        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp));
        }

        let body = resp.into_body();
        let output: ListObjectVersionsOutput = de::from_reader(body.reader())
            .map_err(new_xml_deserialize_error)
            // Allow Cos list to retry on XML deserialization errors.
            //
            // This is because the Cos list API may return incomplete XML data under high load.
            // We are confident that our XML decoding logic is correct. When this error occurs,
            // we allow retries to obtain the correct data.
            .map_err(Error::set_temporary)?;

        ctx.done = if let Some(is_truncated) = output.is_truncated {
            !is_truncated
        } else {
            false
        };
        ctx.token = format!(
            "{} {}",
            output.next_key_marker.unwrap_or_default(),
            output.next_version_id_marker.unwrap_or_default()
        );

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.core.root, &prefix.prefix),
                Metadata::new(EntryMode::DIR),
            );
            ctx.entries.push_back(de);
        }

        for version_object in output.version {
            // `list` must be additive, so we need to include the latest version object
            // even if `versions` is not enabled.
            //
            // Here we skip all non-latest version objects if `versions` is not enabled.
            if !(self.args.versions() || version_object.is_latest) {
                continue;
            }

            let mut path = build_rel_path(&self.core.root, &version_object.key);
            if path.is_empty() {
                path = "/".to_owned();
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));
            meta.set_version(&version_object.version_id);
            meta.set_is_current(version_object.is_latest);
            meta.set_content_length(version_object.size);
            meta.set_last_modified(parse_datetime_from_rfc3339(
                version_object.last_modified.as_str(),
            )?);
            if let Some(etag) = version_object.etag {
                meta.set_etag(&etag);
                meta.set_content_md5(etag.trim_matches('"'));
            }

            let entry = oio::Entry::new(&path, meta);
            ctx.entries.push_back(entry);
        }

        if self.args.deleted() {
            for delete_marker in output.delete_marker {
                let mut path = build_rel_path(&self.core.root, &delete_marker.key);
                if path.is_empty() {
                    path = "/".to_owned();
                }

                let mut meta = Metadata::new(EntryMode::FILE);
                meta.set_version(&delete_marker.version_id);
                meta.set_is_deleted(true);
                meta.set_is_current(delete_marker.is_latest);
                meta.set_last_modified(parse_datetime_from_rfc3339(
                    delete_marker.last_modified.as_str(),
                )?);

                let entry = oio::Entry::new(&path, meta);
                ctx.entries.push_back(entry);
            }
        }

        Ok(())
    }
}
