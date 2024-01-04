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
use chrono::DateTime;
use chrono::Utc;
use http::StatusCode;
use serde::Deserialize;
use std::sync::Arc;

use super::core::GhaaCore;
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
        let resp = if self.path == "/" {
            self.core
                .list_artifacts_under_repo(&ctx.token, &self.limit)
                .await?
        } else {
            self.core
                .list_artifacts_under_workflow(&self.path, &ctx.token, &self.limit)
                .await?
        };

        match resp.status() {
            StatusCode::OK => {
                let slc = resp.into_body().bytes().await?;
                let parsed_body: GhaaListResponse =
                    serde_json::from_slice(&slc).map_err(new_json_deserialize_error)?;

                for artifact in parsed_body.artifacts {
                    ctx.entries.push_back(parse_artifact_to_entry(artifact));
                }

                // parsed_body.artifacts is empty means we have reached the end of the list
                if ctx.entries.is_empty() {
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

fn parse_artifact_to_entry(artifact: GhaaArtifact) -> oio::Entry {
    let meta = Metadata::new(EntryMode::FILE)
        .with_etag(artifact.id.to_string())
        .with_content_length(artifact.size_in_bytes as u64)
        .with_last_modified(
            DateTime::parse_from_rfc3339(&artifact.updated_at)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap(),
        );
    oio::Entry::new(artifact.workflow_run.id.to_string().as_str(), meta)
}

#[derive(Debug, Deserialize)]
struct GhaaListResponse {
    artifacts: Vec<GhaaArtifact>,
}

#[derive(Debug, Deserialize)]
struct GhaaArtifact {
    id: u64,
    size_in_bytes: usize,
    updated_at: String,
    workflow_run: Workflow,
}

#[derive(Debug, Deserialize)]
struct Workflow {
    id: u64,
}
