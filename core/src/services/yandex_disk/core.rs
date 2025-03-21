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
use http::header;
use http::request;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

#[derive(Clone)]
pub struct YandexDiskCore {
    pub info: Arc<AccessorInfo>,
    /// The root of this core.
    pub root: String,
    /// Yandex Disk oauth access_token.
    pub access_token: String,
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
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }

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
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bytes = resp.into_body();

                let resp: GetUploadUrlResponse =
                    serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

                Ok(resp.href)
            }
            _ => Err(parse_error(resp)),
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
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bytes = resp.into_body();

                let resp: GetUploadUrlResponse =
                    serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

                Ok(resp.href)
            }
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn ensure_dir_exists(&self, path: &str) -> Result<()> {
        let path = build_abs_path(&self.root, path);

        let paths = path.split('/').collect::<Vec<&str>>();

        for i in 0..paths.len() - 1 {
            let path = paths[..i + 1].join("/");
            let resp = self.create_dir(&path).await?;

            let status = resp.status();

            match status {
                StatusCode::CREATED | StatusCode::CONFLICT => {}
                _ => return Err(parse_error(resp)),
            }
        }
        Ok(())
    }

    pub async fn create_dir(&self, path: &str) -> Result<Response<Buffer>> {
        let url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources?path=/{}",
            percent_encode_path(path),
        );

        let req = Request::put(url);

        let req = self.sign(req);

        // Set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn copy(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
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
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn move_object(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
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
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn delete(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "https://cloud-api.yandex.net/v1/disk/resources?path={}&permanently=true",
            percent_encode_path(&path),
        );

        let req = Request::delete(url);

        let req = self.sign(req);

        // Set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn metainformation(
        &self,
        path: &str,
        limit: Option<usize>,
        offset: Option<String>,
    ) -> Result<Response<Buffer>> {
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
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
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
