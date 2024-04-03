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

use bytes::Buf;
use http::header;
use http::request;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

#[derive(Clone)]
pub struct YandexDiskCore {
    /// The root of this core.
    pub root: String,
    /// Yandex Disk oauth access_token.
    pub access_token: String,

    pub client: HttpClient,
}

impl Debug for YandexDiskCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl YandexDiskCore {
    #[inline]
    pub fn sign(&self, req: request::Builder) -> request::Builder {
        req.header(
            header::AUTHORIZATION,
            format!("OAuth {}", self.access_token),
        )
    }
}

impl YandexDiskCore {
    /// Get upload url.
    pub async fn get_upload_url(&self, path: &str) -> Result<String> {
        let path = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources/upload?path={}&overwrite=true",
            percent_encode_path(&path)
        );

        let req = Request::get(url);

        let req = self.sign(req);

        // Set body
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: GetUploadUrlResponse = body.to_json().await?;
                Ok(resp.href)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn get_download_url(&self, path: &str) -> Result<String> {
        let path = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources/download?path={}&overwrite=true",
            percent_encode_path(&path)
        );

        let req = Request::get(url);

        let req = self.sign(req);

        // Set body
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: GetUploadUrlResponse = body.to_json().await?;
                Ok(resp.href)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn ensure_dir_exists(&self, path: &str) -> Result<()> {
        let path = build_abs_path(&self.root, path);

        let paths = path.split('/').collect::<Vec<&str>>();

        for i in 0..paths.len() - 1 {
            let path = paths[..i + 1].join("/");
            self.create_dir(&path).await?;
        }
        Ok(())
    }

    pub async fn create_dir(&self, path: &str) -> Result<()> {
        let url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources?path=/{}",
            percent_encode_path(path),
        );

        let req = Request::put(url);

        let req = self.sign(req);

        // Set body
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED | StatusCode::CONFLICT => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn copy(&self, from: &str, to: &str) -> Result<()> {
        let from = build_rooted_abs_path(&self.root, from);
        let to = build_rooted_abs_path(&self.root, to);

        let url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources/copy?from={}&path={}&overwrite=true",
            percent_encode_path(&from),
            percent_encode_path(&to)
        );

        let req = Request::post(url);

        let req = self.sign(req);

        // Set body
        let req = req
            .body(RequestBody::Empty)
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

    pub async fn move_object(&self, from: &str, to: &str) -> Result<()> {
        let from = build_rooted_abs_path(&self.root, from);
        let to = build_rooted_abs_path(&self.root, to);

        let url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources/move?from={}&path={}&overwrite=true",
            percent_encode_path(&from),
            percent_encode_path(&to)
        );

        let req = Request::post(url);

        let req = self.sign(req);

        // Set body
        let req = req
            .body(RequestBody::Empty)
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
        let path = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources?path={}&permanently=true",
            percent_encode_path(&path),
        );

        let req = Request::delete(url);

        let req = self.sign(req);

        // Set body
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK |
            StatusCode::NO_CONTENT |
            // Yandex Disk deleting a non-empty folder can take an unknown amount of time,
            // So the API responds with the code 202 Accepted (the deletion process has started).
            StatusCode::ACCEPTED|
            // Allow 404 when deleting a non-existing object
            StatusCode::NOT_FOUND=> Ok(()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn metainformation(
        &self,
        path: &str,
        limit: Option<usize>,
        offset: Option<String>,
    ) -> Result<MetainformationResponse> {
        let path = build_rooted_abs_path(&self.root, path);

        let mut url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources?path={}",
            percent_encode_path(&path),
        );

        if let Some(limit) = limit {
            url = format!("{}&limit={}", url, limit);
        }

        if let Some(offset) = offset {
            url = format!("{}&offset={}", url, offset);
        }

        let req = Request::get(url);

        let req = self.sign(req);

        // Set body
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let mf: MetainformationResponse = body.to_json().await?;
                Ok(mf)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }
}

pub(super) fn parse_info(mf: MetainformationResponse) -> Result<Metadata> {
    let mode = if mf.ty == "file" {
        EntryMode::FILE
    } else {
        EntryMode::DIR
    };

    let mut m = Metadata::new(mode);

    m.set_last_modified(parse_datetime_from_rfc3339(&mf.modified)?);

    if let Some(md5) = mf.md5 {
        m.set_content_md5(&md5);
    }

    if let Some(mime_type) = mf.mime_type {
        m.set_content_type(&mime_type);
    }

    if let Some(size) = mf.size {
        m.set_content_length(size);
    }

    Ok(m)
}

#[derive(Debug, Deserialize)]
pub struct GetUploadUrlResponse {
    pub href: String,
}

#[derive(Debug, Deserialize)]
pub struct MetainformationResponse {
    #[serde(rename = "type")]
    pub ty: String,
    pub name: String,
    pub path: String,
    pub modified: String,
    pub md5: Option<String>,
    pub mime_type: Option<String>,
    pub size: Option<u64>,
    #[serde(rename = "_embedded")]
    pub embedded: Option<Embedded>,
}

#[derive(Debug, Deserialize)]
pub struct Embedded {
    pub total: usize,
    pub items: Vec<MetainformationResponse>,
}
