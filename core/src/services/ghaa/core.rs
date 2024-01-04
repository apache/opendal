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

use crate::raw::*;
use crate::services::ghaa::error::parse_error;
use crate::*;

/// VERSION is the compiled version of OpenDAL.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const HEADER_GITHUB_API_VERSION: &str = "X-GitHub-Api-Version";

/// The default endpoint suffix for ghaa.
const DEFAULT_GHAA_ENDPOINT_SUFFIX: &str = "https://api.github.com/repos";

pub struct GhaaCore {
    pub owner: String,
    pub repo: String,
    pub token: Option<String>,
    pub client: HttpClient,
}

impl Debug for GhaaCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GhaaCore")
            .field("owner", &self.owner)
            .field("repo", &self.repo)
            .finish_non_exhaustive()
    }
}
impl GhaaCore {
    pub async fn download_artifact(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!(
            "{}/{}/{}/actions/artifacts/{}/zip",
            DEFAULT_GHAA_ENDPOINT_SUFFIX, self.owner, self.repo, path
        );

        let mut req = Request::get(&url);

        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.token {
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
            DEFAULT_GHAA_ENDPOINT_SUFFIX, self.owner, self.repo, path
        );

        let mut req = Request::get(&url);
        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.token {
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
            DEFAULT_GHAA_ENDPOINT_SUFFIX, self.owner, self.repo, path
        );

        let mut req = Request::delete(&url);
        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.token {
            req = req.header(AUTHORIZATION, format!("Bearer {}", auth))
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn list_artifacts_under_repo(
        &self,
        page: &String,
        limit: &Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        // GitHub only allows 100 items per page at most.
        // https://docs.github.com/en/rest/actions/artifacts?apiVersion=2022-11-28#list-artifacts-for-a-repository
        let url: String = format!(
            "{}/{}/{}/actions/artifacts?per_page={}&page={}",
            DEFAULT_GHAA_ENDPOINT_SUFFIX,
            self.owner,
            self.repo,
            limit.unwrap_or(100),
            page
        );
        println!("{}", url);
        let mut req = Request::get(&url);
        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.token {
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
        let path = path.trim_end_matches("/");
        let url: String = format!(
            "{}/{}/{}/actions/runs/{}/artifacts?pre_page={}&page={}",
            DEFAULT_GHAA_ENDPOINT_SUFFIX,
            self.owner,
            self.repo,
            path,
            limit.unwrap_or(100),
            page
        );

        let mut req = Request::get(&url);
        req = req.header(USER_AGENT, format!("opendal/{VERSION} (service ghaa)"));
        req = req.header(HEADER_GITHUB_API_VERSION, "2022-11-28");
        req = req.header(ACCEPT, "application/vnd.github.v3+json");

        if let Some(auth) = &self.token {
            req = req.header(AUTHORIZATION, format!("Bearer {}", auth))
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}
