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

use bytes::Buf;
use std::sync::Arc;

use http::StatusCode;

use super::core::parse_info;
use super::core::ChainsafeCore;
use super::core::Info;
use super::error::parse_error;
use crate::raw::oio::Entry;
use crate::raw::*;
use crate::*;

pub struct ChainsafeLister {
    core: Arc<ChainsafeCore>,
    path: String,
}

impl ChainsafeLister {
    pub fn new(core: Arc<ChainsafeCore>, path: &str) -> Self {
        Self {
            core,

            path: path.to_string(),
        }
    }
}

impl oio::PageList for ChainsafeLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self.core.list_objects(&self.path).await?;

        match resp.status() {
            StatusCode::OK => {
                let bs = resp.into_body();

                let output: Vec<Info> =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                for info in output {
                    let mut path = build_abs_path(&normalize_root(&self.path), &info.name);

                    let md = parse_info(info);

                    if md.mode() == EntryMode::DIR {
                        path = format!("{}/", path);
                    }

                    ctx.entries.push_back(Entry::new(&path, md));
                }

                ctx.done = true;

                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
