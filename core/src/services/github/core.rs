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

use base64::Engine;
use bytes::Buf;
use bytes::Bytes;
use http::header;
use http::request;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// Core of [github contents](https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#create-or-update-file-contents) services support.
#[derive(Clone)]
pub struct GithubCore {
    pub info: Arc<AccessorInfo>,
    /// The root of this core.
    pub root: String,
    /// Github access_token.
    pub token: Option<String>,
    /// Github repo owner.
    pub owner: String,
    /// Github repo name.
    pub repo: String,
}

impl Debug for GithubCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("owner", &self.owner)
            .field("repo", &self.repo)
            .finish_non_exhaustive()
    }
}

impl GithubCore {
    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }

    pub fn sign(&self, req: request::Builder) -> Result<request::Builder> {
        let mut req = req
            .header(header::USER_AGENT, format!("opendal-{}", VERSION))
            .header("X-GitHub-Api-Version", "2022-11-28");

        // Github access_token is optional.
        if let Some(token) = &self.token {
            req = req.header(
                header::AUTHORIZATION,
                format_authorization_by_bearer(token)?,
            )
        }

        Ok(req)
    }
}

impl GithubCore {
    pub async fn get_file_sha(&self, path: &str) -> Result<Option<String>> {
        // if the token is not set, we should not try to get the sha of the file.
        if self.token.is_none() {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "Github access_token is not set",
            ));
        }

        let resp = self.stat(path).await?;

        match resp.status() {
            StatusCode::OK => {
                let body = resp.into_body();
                let resp: Entry =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                Ok(Some(resp.sha))
            }
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn stat(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}",
            self.owner,
            self.repo,
            percent_encode_path(&path)
        );

        let req = Request::get(url);

        let req = self.sign(req)?;

        let req = req
            .header("Accept", "application/vnd.github.object+json")
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn get(&self, path: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}",
            self.owner,
            self.repo,
            percent_encode_path(&path)
        );

        let req = Request::get(url);

        let req = self.sign(req)?;

        let req = req
            .header(header::ACCEPT, "application/vnd.github.raw+json")
            .header(header::RANGE, range.to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.info.http_client().fetch(req).await
    }

    pub async fn upload(&self, path: &str, bs: Buffer) -> Result<Response<Buffer>> {
        let sha = self.get_file_sha(path).await?;

        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}",
            self.owner,
            self.repo,
            percent_encode_path(&path)
        );

        let req = Request::put(url);

        let req = self.sign(req)?;

        let mut req_body = CreateOrUpdateContentsRequest {
            message: format!("Write {} at {} via opendal", path, chrono::Local::now()),
            content: base64::engine::general_purpose::STANDARD.encode(bs.to_bytes()),
            sha: None,
        };

        if let Some(sha) = sha {
            req_body.sha = Some(sha);
        }

        let req_body = serde_json::to_vec(&req_body).map_err(new_json_serialize_error)?;

        let req = req
            .header("Accept", "application/vnd.github+json")
            .body(Buffer::from(req_body))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        // If path is a directory, we should delete path/.gitkeep
        let formatted_path = format!("{}.gitkeep", path);
        let p = if path.ends_with('/') {
            formatted_path.as_str()
        } else {
            path
        };

        let Some(sha) = self.get_file_sha(p).await? else {
            return Ok(());
        };

        let path = build_abs_path(&self.root, p);

        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}",
            self.owner,
            self.repo,
            percent_encode_path(&path)
        );

        let req = Request::delete(url);

        let req = self.sign(req)?;

        let req_body = DeleteContentsRequest {
            message: format!("Delete {} at {} via opendal", path, chrono::Local::now()),
            sha,
        };

        let req_body = serde_json::to_vec(&req_body).map_err(new_json_serialize_error)?;

        let req = req
            .header("Accept", "application/vnd.github.object+json")
            .body(Buffer::from(Bytes::from(req_body)))
            .map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        match resp.status() {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn list(&self, path: &str) -> Result<ListResponse> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}",
            self.owner,
            self.repo,
            percent_encode_path(&path)
        );

        let req = Request::get(url);

        let req = self.sign(req)?;

        let req = req
            .header("Accept", "application/vnd.github.object+json")
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        match resp.status() {
            StatusCode::OK => {
                let body = resp.into_body();
                let resp: ListResponse =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                Ok(resp)
            }
            StatusCode::NOT_FOUND => Ok(ListResponse::default()),
            _ => Err(parse_error(resp)),
        }
    }

    /// We use git_url to call github's Tree based API.
    pub async fn list_with_recursive(&self, git_url: &str) -> Result<Vec<Tree>> {
        let url = format!("{}?recursive=true", git_url);

        let req = Request::get(url);

        let req = self.sign(req)?;

        let req = req
            .header("Accept", "application/vnd.github.object+json")
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        match resp.status() {
            StatusCode::OK => {
                let body = resp.into_body();
                let resp: ListTreeResponse =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                Ok(resp.tree)
            }
            _ => Err(parse_error(resp)),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize)]
pub struct CreateOrUpdateContentsRequest {
    pub message: String,
    pub content: String,
    pub sha: Option<String>,
}

#[derive(Default, Debug, Clone, Serialize)]
pub struct DeleteContentsRequest {
    pub message: String,
    pub sha: String,
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct ListTreeResponse {
    pub tree: Vec<Tree>,
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct Tree {
    pub path: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub size: Option<u64>,
    pub sha: String,
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct ListResponse {
    pub git_url: String,
    pub entries: Vec<Entry>,
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct Entry {
    pub path: String,
    pub sha: String,
    pub size: u64,
    #[serde(rename = "type")]
    pub type_field: String,
}
