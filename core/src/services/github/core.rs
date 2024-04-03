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

use std::fmt::Debug;
use std::fmt::Formatter;

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

use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// Core of [github contents](https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#create-or-update-file-contents) services support.
#[derive(Clone)]
pub struct GithubCore {
    /// The root of this core.
    pub root: String,
    /// Github access_token.
    pub token: Option<String>,
    /// Github repo owner.
    pub owner: String,
    /// Github repo name.
    pub repo: String,

    pub client: HttpClient,
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
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<oio::Buffer>> {
        self.client.send(req).await
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
        // if the token is not set, we shhould not try to get the sha of the file.
        if self.token.is_none() {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "Github access_token is not set",
            ));
        }

        let resp = self.stat(path).await?;

        match resp.status() {
            StatusCode::OK => {
                let headers = resp.headers();

                let sha = parse_etag(headers)?;

                let Some(sha) = sha else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "No ETag found in response headers",
                    ));
                };

                Ok(Some(sha.trim_matches('"').to_string()))
            }
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(parse_error(resp).await?),
        }
    }

    pub async fn stat(&self, path: &str) -> Result<Response<oio::Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}",
            self.owner,
            self.repo,
            percent_encode_path(&path)
        );

        let req = Request::head(url);

        let req = self.sign(req)?;

        let req = req
            .header("Accept", "application/vnd.github.raw+json")
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn get(&self, path: &str, range: BytesRange) -> Result<Response<oio::Buffer>> {
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
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn upload(&self, path: &str, bs: Bytes) -> Result<Response<oio::Buffer>> {
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
            content: base64::engine::general_purpose::STANDARD.encode(&bs),
            sha: None,
        };

        if let Some(sha) = sha {
            req_body.sha = Some(sha);
        }

        let req_body = serde_json::to_vec(&req_body).map_err(new_json_serialize_error)?;

        let req = req
            .header("Accept", "application/vnd.github+json")
            .body(AsyncBody::Bytes(Bytes::from(req_body)))
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let Some(sha) = self.get_file_sha(path).await? else {
            return Ok(());
        };

        let path = build_abs_path(&self.root, path);

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
            .body(AsyncBody::Bytes(Bytes::from(req_body)))
            .map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        match resp.status() {
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(resp).await?),
        }
    }

    pub async fn list(&self, path: &str) -> Result<Vec<Entry>> {
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
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        match resp.status() {
            StatusCode::OK => {
                let body = resp.into_body();
                let resp: ListResponse =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                Ok(resp.entries)
            }
            _ => Err(parse_error(resp).await?),
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
pub struct ListResponse {
    pub entries: Vec<Entry>,
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct Entry {
    pub name: String,
    pub path: String,
    pub sha: String,
    pub size: u64,
    pub url: String,
    pub html_url: String,
    pub git_url: String,
    pub download_url: Option<String>,
    #[serde(rename = "type")]
    pub type_field: String,
    pub content: Option<String>,
    pub encoding: Option<String>,
    #[serde(rename = "_links")]
    pub links: Links,
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct Links {
    #[serde(rename = "self")]
    pub self_field: String,
    pub git: String,
    pub html: String,
}
