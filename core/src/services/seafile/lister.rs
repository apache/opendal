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
use http::header;
use http::Request;
use http::StatusCode;
use serde::Deserialize;

use super::core::SeafileCore;
use super::error::parse_error;
use crate::raw::oio::Entry;
use crate::raw::*;
use crate::*;

pub struct SeafileLister {
    core: Arc<SeafileCore>,

    path: String,
}

impl SeafileLister {
    pub(super) fn new(core: Arc<SeafileCore>, path: &str) -> Self {
        SeafileLister {
            core,
            path: path.to_string(),
        }
    }
}

impl oio::PageList for SeafileLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let path = build_rooted_abs_path(&self.core.root, &self.path);

        let auth_info = self.core.get_auth_info().await?;

        let url = format!(
            "{}/api2/repos/{}/dir/?p={}",
            self.core.endpoint,
            auth_info.repo_id,
            percent_encode_path(&path)
        );

        let req = Request::get(url);

        let req = req
            .header(header::AUTHORIZATION, format!("Token {}", auth_info.token))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let resp_body = resp.into_body();
                let infos: Vec<Info> = serde_json::from_reader(resp_body.reader())
                    .map_err(new_json_deserialize_error)?;

                // add path itself
                ctx.entries.push_back(Entry::new(
                    self.path.as_str(),
                    Metadata::new(EntryMode::DIR),
                ));

                for info in infos {
                    if !info.name.is_empty() {
                        let rel_path =
                            build_rel_path(&self.core.root, &format!("{}{}", path, info.name));

                        let entry = if info.type_field == "file" {
                            let meta = Metadata::new(EntryMode::FILE)
                                .with_last_modified(parse_datetime_from_from_timestamp(info.mtime)?)
                                .with_content_length(info.size.unwrap_or(0));
                            Entry::new(&rel_path, meta)
                        } else {
                            let path = format!("{}/", rel_path);
                            Entry::new(&path, Metadata::new(EntryMode::DIR))
                        };

                        ctx.entries.push_back(entry);
                    }
                }

                ctx.done = true;

                Ok(())
            }
            // return nothing when not exist
            StatusCode::NOT_FOUND => {
                ctx.done = true;
                Ok(())
            }
            _ => Err(parse_error(resp)),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Info {
    #[serde(rename = "type")]
    pub type_field: String,
    pub mtime: i64,
    pub size: Option<u64>,
    pub name: String,
}
