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
use serde::Deserialize;
use serde_json::de;

use super::core::AzdlsCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct AzdlsLister {
    core: Arc<AzdlsCore>,

    path: String,
    limit: Option<usize>,
}

impl AzdlsLister {
    pub fn new(core: Arc<AzdlsCore>, path: String, limit: Option<usize>) -> Self {
        Self { core, path, limit }
    }
}

impl oio::PageList for AzdlsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let Some((token, output)) = self
            .core
            .azdls_list(&self.path, &ctx.token, self.limit)
            .await?
        else {
            ctx.done = true;
            return Ok(());
        };

        // Check whether this list is done.
        ctx.token = token;
        if !ctx.token.is_empty() {
            ctx.done = true;
        }

        for object in output.paths {
            // Azdls will return `"true"` and `"false"` for is_directory.
            let mode = if &object.is_directory == "true" {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            };

            let meta = Metadata::new(mode)
                // Keep fit with ETag header.
                .with_etag(format!("\"{}\"", &object.etag))
                .with_content_length(object.content_length.parse().map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "content length is not valid integer")
                        .set_source(err)
                })?)
                .with_last_modified(parse_datetime_from_rfc2822(&object.last_modified)?);

            let mut path = build_rel_path(&self.core.root, &object.name);
            if mode == EntryMode::DIR {
                path += "/"
            };

            let de = oio::Entry::new(&path, meta);

            ctx.entries.push_back(de);
        }

        Ok(())
    }
}
