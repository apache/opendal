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
use bytes::Bytes;
use http::header;
use http::request;
use http::Request;
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

        let meta = self.stat(path).await?;
        match meta {
            Some(meta) => Ok(meta.etag().map(|v| v.trim_matches('"').to_string())),
            None => Ok(None),
        }
    }

    pub async fn stat(&self, path: &str) -> Result<Option<Metadata>> {
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
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => parse_into_metadata(&path, &parts.headers).map(Some),
            StatusCode::NOT_FOUND => Ok(None),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn get(&self, path: &str, range: BytesRange, buf: &mut oio::WritableBuf) -> Result<usize> {
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
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => body.read(buf).await,
            StatusCode::RANGE_NOT_SATISFIABLE => {
                body.consume().await?;
                Ok(0)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn upload(&self, path: &str, bs: Bytes) -> Result<()> {
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
            .body(RequestBody::Bytes(Bytes::from(req_body)))
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK | StatusCode::CREATED => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
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
            .body(RequestBody::Bytes(Bytes::from(req_body)))
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => Ok(()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
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
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let resp: ListResponse = body.to_json().await?;
                Ok(resp.entries)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
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
