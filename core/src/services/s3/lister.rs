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

use super::core::S3Core;
use super::core::{ListObjectVersionsOutput, ListObjectsOutput};
use super::error::parse_error;
use crate::raw::oio::PageContext;
use crate::raw::*;
use crate::EntryMode;
use crate::Error;
use crate::Metadata;
use crate::Result;
use bytes::Buf;
use quick_xml::de;

pub type S3Listers = TwoWays<oio::PageLister<S3Lister>, oio::PageLister<S3ObjectVersionsLister>>;

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
                // start after should only be set for the first page.
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

        let output: ListObjectsOutput = de::from_reader(bs.reader())
            .map_err(new_xml_deserialize_error)
            // Allow S3 list to retry on XML deserialization errors.
            //
            // This is because the S3 list API may return incomplete XML data under high load.
            // We are confident that our XML decoding logic is correct. When this error occurs,
            // we allow retries to obtain the correct data.
            .map_err(Error::set_temporary)?;

        // Try our best to check whether this list is done.
        //
        // - Check `is_truncated`
        // - Check `next_continuation_token`
        // - Check the length of `common_prefixes` and `contents` (very rare case)
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
            let mut path = build_rel_path(&self.core.root, &object.key);
            if path.is_empty() {
                path = "/".to_string();
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));

            if let Some(etag) = &object.etag {
                meta.set_etag(etag);
                meta.set_content_md5(etag.trim_matches('"'));
            }
            meta.set_content_length(object.size);

            // object.last_modified provides more precise time that contains
            // nanosecond, let's trim them.
            meta.set_last_modified(parse_datetime_from_rfc3339(object.last_modified.as_str())?);

            let de = oio::Entry::with(path, meta);
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

// refer: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
pub struct S3ObjectVersionsLister {
    core: Arc<S3Core>,

    prefix: String,
    delimiter: &'static str,
    limit: Option<usize>,
    start_after: String,
    abs_start_after: String,
}

impl S3ObjectVersionsLister {
    pub fn new(
        core: Arc<S3Core>,
        path: &str,
        recursive: bool,
        limit: Option<usize>,
        start_after: Option<&str>,
    ) -> Self {
        let delimiter = if recursive { "" } else { "/" };
        let start_after = start_after.unwrap_or_default().to_owned();
        let abs_start_after = build_abs_path(core.root.as_str(), start_after.as_str());

        Self {
            core,
            prefix: path.to_string(),
            delimiter,
            limit,
            start_after,
            abs_start_after,
        }
    }
}

impl oio::PageList for S3ObjectVersionsLister {
    async fn next_page(&self, ctx: &mut PageContext) -> Result<()> {
        let markers = ctx.token.rsplit_once(" ");
        let (key_marker, version_id_marker) = if let Some(data) = markers {
            data
        } else if !self.start_after.is_empty() {
            (self.abs_start_after.as_str(), "")
        } else {
            ("", "")
        };

        let resp = self
            .core
            .s3_list_object_versions(
                &self.prefix,
                self.delimiter,
                self.limit,
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
            // Allow S3 list to retry on XML deserialization errors.
            //
            // This is because the S3 list API may return incomplete XML data under high load.
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
            let mut path = build_rel_path(&self.core.root, &version_object.key);
            if path.is_empty() {
                path = "/".to_owned();
            }

            let mut meta = Metadata::new(EntryMode::from_path(&path));
            meta.set_version(&version_object.version_id);
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

        Ok(())
    }
}
