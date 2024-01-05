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

use std::fmt::{Debug, Formatter};

use crate::raw::HttpClient;
use http::header::ACCEPT;
use http::header::AUTHORIZATION;
use http::header::USER_AGENT;
use http::Request;
use http::Response;
use madsim::net::rpc::Deserialize;

use crate::raw::*;
use crate::services::GhaaConfig;
use crate::*;

/// VERSION is the compiled version of OpenDAL.
pub const VERSION: &str = raw::VERSION;

const HEADER_GITHUB_API_VERSION: &str = "X-GitHub-Api-Version";

/// The default endpoint suffix for ghaa.
const DEFAULT_GHAA_ENDPOINT_SUFFIX: &str = "https://api.github.com/repos";

pub struct GhaaCore {
    pub config: GhaaConfig,
    pub client: HttpClient,
}

impl Debug for GhaaCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GhaaCore")
            .field("owner", &self.config.owner)
            .field("repo", &self.config.repo)
            .finish_non_exhaustive()
    }
}
impl GhaaCore {
    pub async fn download_artifact(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!(
            "{}/{}/{}/actions/artifacts/{}/zip",
            DEFAULT_GHAA_ENDPOINT_SUFFIX, self.config.owner, self.config.repo, path
        );

        let mut req = Request::get(&url);

        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.config.token {
            req = req.header(AUTHORIZATION, format!("Bearer {}", auth))
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn stat_artifact(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!(
            "{}/{}/{}/actions/artifacts/{}",
            DEFAULT_GHAA_ENDPOINT_SUFFIX, self.config.owner, self.config.repo, path
        );

        let mut req = Request::get(&url);
        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.config.token {
            req = req.header(AUTHORIZATION, format!("Bearer {}", auth))
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn delete_artifact(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!(
            "{}/{}/{}/actions/artifacts/{}",
            DEFAULT_GHAA_ENDPOINT_SUFFIX, self.config.owner, self.config.repo, path
        );

        let mut req = Request::delete(&url);
        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.config.token {
            req = req.header(AUTHORIZATION, format!("Bearer {}", auth))
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn list_artifacts_under_workflow(
        &self,
        path: &str,
        page: &String,
        limit: &Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        // GitHub only allows 100 items per page at most.
        // https://docs.github.com/en/rest/actions/artifacts?apiVersion=2022-11-28#list-workflow-run-artifacts
        let path = path.trim_end_matches('/');
        let url: String = format!(
            "{}/{}/{}/actions/runs/{}/artifacts?pre_page={}&page={}",
            DEFAULT_GHAA_ENDPOINT_SUFFIX,
            self.config.owner,
            self.config.repo,
            path,
            limit.unwrap_or(100),
            page
        );

        let mut req = Request::get(&url);
        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.config.token {
            req = req.header(AUTHORIZATION, format!("Bearer {}", auth))
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct GhaaStatResponse {
    pub(crate) size_in_bytes: u64,
    pub(crate) updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stat_response() {
        let resp = r#"{
            "id": 11,
            "node_id": "MDg6QXJ0aWZhY3QxMQ==",
            "name": "Rails",
            "size_in_bytes": 556,
            "url": "https://api.github.com/repos/octo-org/octo-docs/actions/artifacts/11",
            "archive_download_url": "https://api.github.com/repos/octo-org/octo-docs/actions/artifacts/11/zip",
            "expired": false,
            "created_at": "2020-01-10T14:59:22Z",
            "expires_at": "2020-01-21T14:59:22Z",
            "updated_at": "2020-01-21T14:59:22Z",
            "workflow_run": {
              "id": 2332938,
              "repository_id": 1296269,
              "head_repository_id": 1296269,
              "head_branch": "main",
              "head_sha": "328faa0536e6fef19753d9d91dc96a9931694ce3"
            }
        }"#;

        let parsed_body: GhaaStatResponse = serde_json::from_str(resp)
            .map_err(new_json_deserialize_error)
            .unwrap();

        assert_eq!(parsed_body.size_in_bytes, 556);
        assert_eq!(parsed_body.updated_at, "2020-01-21T14:59:22Z");
    }
}
