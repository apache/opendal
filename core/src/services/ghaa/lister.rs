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
        let resp = self
            .core
            .list_artifacts_under_workflow(&self.path, &ctx.token, &self.limit)
            .await?;

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
        .with_content_length(artifact.size_in_bytes as u64)
        .with_last_modified(
            parse_datetime_from_rfc3339(&artifact.updated_at)
                .expect("parse datetime from rfc3339 failed"),
        );
    oio::Entry::new(artifact.workflow_run.id.to_string().as_str(), meta)
}

#[derive(Debug, Deserialize)]
struct GhaaListResponse {
    artifacts: Vec<GhaaArtifact>,
}

#[derive(Debug, Deserialize)]
struct GhaaArtifact {
    size_in_bytes: usize,
    updated_at: String,
    workflow_run: Workflow,
}

#[derive(Debug, Deserialize)]
struct Workflow {
    id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_response() {
        let resp = r#"{
            "total_count": 2,
            "artifacts": [
              {
                "id": 11,
                "node_id": "MDg6QXJ0aWZhY3QxMQ==",
                "name": "Rails",
                "size_in_bytes": 556,
                "url": "https://api.github.com/repos/octo-org/octo-docs/actions/artifacts/11",
                "archive_download_url": "https://api.github.com/repos/octo-org/octo-docs/actions/artifacts/11/zip",
                "expired": false,
                "created_at": "2020-01-10T14:59:22Z",
                "expires_at": "2020-03-21T14:59:22Z",
                "updated_at": "2020-02-21T14:59:22Z",
                "workflow_run": {
                  "id": 2332938,
                  "repository_id": 1296269,
                  "head_repository_id": 1296269,
                  "head_branch": "main",
                  "head_sha": "328faa0536e6fef19753d9d91dc96a9931694ce3"
                }
              },
              {
                "id": 13,
                "node_id": "MDg6QXJ0aWZhY3QxMw==",
                "name": "Test output",
                "size_in_bytes": 453,
                "url": "https://api.github.com/repos/octo-org/octo-docs/actions/artifacts/13",
                "archive_download_url": "https://api.github.com/repos/octo-org/octo-docs/actions/artifacts/13/zip",
                "expired": false,
                "created_at": "2020-01-10T14:59:22Z",
                "expires_at": "2020-03-21T14:59:22Z",
                "updated_at": "2020-02-21T14:59:22Z",
                "workflow_run": {
                  "id": 2332942,
                  "repository_id": 1296269,
                  "head_repository_id": 1296269,
                  "head_branch": "main",
                  "head_sha": "178f4f6090b3fccad4a65b3e83d076a622d59652"
                }
              }
            ]
        }"#;

        let parsed_body: GhaaListResponse = serde_json::from_str(resp)
            .map_err(new_json_deserialize_error)
            .unwrap();

        assert_eq!(parsed_body.artifacts.len(), 2);
        assert_eq!(parsed_body.artifacts[0].size_in_bytes, 556);
        assert_eq!(parsed_body.artifacts[0].workflow_run.id, 2332938);

        assert_eq!(parsed_body.artifacts[1].size_in_bytes, 453);
        assert_eq!(parsed_body.artifacts[1].workflow_run.id, 2332942);
    }
}
