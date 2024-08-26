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
use chrono::Utc;

use self::oio::Entry;
use super::core::AliyunDriveCore;
use super::core::AliyunDriveFileList;
use crate::raw::*;
use crate::EntryMode;
use crate::Error;
use crate::ErrorKind;
use crate::Metadata;
use crate::Result;

pub struct AliyunDriveLister {
    core: Arc<AliyunDriveCore>,

    parent: Option<AliyunDriveParent>,
    limit: Option<usize>,
}

pub struct AliyunDriveParent {
    pub file_id: String,
    pub path: String,
    pub updated_at: String,
}

impl AliyunDriveLister {
    pub fn new(
        core: Arc<AliyunDriveCore>,
        parent: Option<AliyunDriveParent>,
        limit: Option<usize>,
    ) -> Self {
        AliyunDriveLister {
            core,
            parent,
            limit,
        }
    }
}

impl oio::PageList for AliyunDriveLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let Some(parent) = &self.parent else {
            ctx.done = true;
            return Ok(());
        };

        let offset = if ctx.token.is_empty() {
            // Push self into the list result.
            ctx.entries.push_back(Entry::new(
                &parent.path,
                Metadata::new(EntryMode::DIR).with_last_modified(
                    parent
                        .updated_at
                        .parse::<chrono::DateTime<Utc>>()
                        .map_err(|e| {
                            Error::new(ErrorKind::Unexpected, "parse last modified time")
                                .set_source(e)
                        })?,
                ),
            ));
            None
        } else {
            Some(ctx.token.clone())
        };

        let res = self.core.list(&parent.file_id, self.limit, offset).await;
        let res = match res {
            Err(err) if err.kind() == ErrorKind::NotFound => {
                ctx.done = true;
                None
            }
            Err(err) => return Err(err),
            Ok(res) => Some(res),
        };

        let Some(res) = res else {
            return Ok(());
        };

        let result: AliyunDriveFileList =
            serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;

        for item in result.items {
            let (path, mut md) = if item.path_type == "folder" {
                let path = format!("{}{}/", &parent.path.trim_start_matches('/'), &item.name);
                (path, Metadata::new(EntryMode::DIR))
            } else {
                let path = format!("{}{}", &parent.path.trim_start_matches('/'), &item.name);
                (path, Metadata::new(EntryMode::FILE))
            };

            md = md.with_last_modified(item.updated_at.parse::<chrono::DateTime<Utc>>().map_err(
                |e| Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e),
            )?);
            if let Some(v) = item.size {
                md = md.with_content_length(v);
            }
            if let Some(v) = item.content_type {
                md = md.with_content_type(v);
            }

            ctx.entries.push_back(Entry::new(&path, md));
        }

        let next_marker = result.next_marker.unwrap_or_default();
        if next_marker.is_empty() {
            ctx.done = true;
        } else {
            ctx.token = next_marker;
        }

        Ok(())
    }
}
