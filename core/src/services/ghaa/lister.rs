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

use async_trait::async_trait;
use http::StatusCode;
use std::sync::Arc;

use super::core::GhaaArtifact;
use super::core::GhaaCore;
use super::core::GhaaListResponse;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct GhaaLister {
    core: Arc<GhaaCore>,
    path: String,
    limit: Option<usize>,
}

impl GhaaLister {
    pub fn new(core: Arc<GhaaCore>, path: String, limit: Option<usize>) -> Self {
        Self { core, path, limit }
    }
}

#[async_trait]
impl oio::PageList for GhaaLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        if ctx.token.is_empty() {
            ctx.token = 1.to_string();
        }
        // GitHub action not support artifact name with Forward slash `/`

        // There are two cases:
        // - "/" means user want to list all the artifacts
        // - "/artifact_name" means user want to list the corresponding artifact

        let path = self.path.trim_start_matches('/');
        let resp = self
            .core
            .list_all_artifacts_under_workflow(&ctx.token, &self.limit)
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let slc = resp.into_body().bytes().await?;
                let parsed_body: GhaaListResponse =
                    serde_json::from_slice(&slc).map_err(new_json_deserialize_error)?;

                for artifact in &parsed_body.artifacts {
                    if path.is_empty() || path == artifact.name {
                        ctx.entries.push_back(parse_artifact_to_entry(artifact));
                    }
                }

                // parsed_body.artifacts is empty means we have reached the end of the list
                if parsed_body.artifacts.is_empty() {
                    ctx.done = true;
                } else {
                    ctx.token = (ctx.token.parse::<u64>().unwrap() + 1).to_string();
                }

                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}

fn parse_artifact_to_entry(artifact: &GhaaArtifact) -> oio::Entry {
    let meta = Metadata::new(EntryMode::FILE)
        .with_content_length(artifact.size_in_bytes)
        .with_last_modified(
            parse_datetime_from_rfc3339(&artifact.updated_at)
                .expect("parse datetime from rfc3339 failed"),
        );
    oio::Entry::new(artifact.name.as_str(), meta)
}
