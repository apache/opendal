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
use http::StatusCode;
use http::{header, Request};
use serde::Deserialize;
use serde::Serialize;

use super::error::parse_error;
use crate::raw::oio::WritableBuf;
use crate::raw::*;
use crate::*;

#[derive(Debug, Serialize)]
struct CreateFileRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    recursive: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateDirRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    recursive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    allow_exists: Option<bool>,
}

/// Metadata of alluxio object
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FileInfo {
    /// The path of the object
    pub path: String,
    /// The last modification time of the object
    pub last_modification_time_ms: i64,
    /// Whether the object is a folder
    pub folder: bool,
    /// The length of the object in bytes
    pub length: u64,
}

impl TryFrom<FileInfo> for Metadata {
    type Error = Error;

    fn try_from(file_info: FileInfo) -> Result<Metadata> {
        let mut metadata = if file_info.folder {
            Metadata::new(EntryMode::DIR)
        } else {
            Metadata::new(EntryMode::FILE)
        };
        metadata
            .set_content_length(file_info.length)
            .set_last_modified(parse_datetime_from_from_timestamp_millis(
                file_info.last_modification_time_ms,
            )?);
        Ok(metadata)
    }
}

/// Alluxio core
#[derive(Clone)]
pub struct AlluxioCore {
    /// root of this backend.
    pub root: String,
    /// endpoint of alluxio
    pub endpoint: String,
    /// prefix of alluxio
    pub client: HttpClient,
}

impl Debug for AlluxioCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl AlluxioCore {
    pub async fn create_dir(&self, path: &str) -> Result<()> {
        let path = build_rooted_abs_path(&self.root, path);

        let r = CreateDirRequest {
            recursive: Some(true),
            allow_exists: Some(true),
        };

        let body = serde_json::to_vec(&r).map_err(new_json_serialize_error)?;
        let body = bytes::Bytes::from(body);

        let mut req = Request::post(format!(
            "{}/api/v1/paths/{}/create-directory",
            self.endpoint,
            percent_encode_path(&path)
        ));

        req = req.header("Content-Type", "application/json");

        let req = req
            .body(RequestBody::Bytes(body))
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn create_file(&self, path: &str) -> Result<u64> {
        let path = build_rooted_abs_path(&self.root, path);

        let r = CreateFileRequest {
            recursive: Some(true),
        };

        let body = serde_json::to_vec(&r).map_err(new_json_serialize_error)?;
        let body = bytes::Bytes::from(body);
        let mut req = Request::post(format!(
            "{}/api/v1/paths/{}/create-file",
            self.endpoint,
            percent_encode_path(&path)
        ));

        req = req.header(header::CONTENT_TYPE, "application/json");

        let req = req
            .body(RequestBody::Bytes(body))
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let body = body.to_bytes().await?;
                let steam_id: u64 =
                    serde_json::from_reader(body.reader()).map_err(new_json_serialize_error)?;
                Ok(steam_id)
            }
            _ => {
                let body = body.to_bytes().await?;
                Err(parse_error(parts, body)?)
            }
        }
    }

    pub(super) async fn open_file(&self, path: &str) -> Result<u64> {
        let path = build_rooted_abs_path(&self.root, path);

        let req = Request::post(format!(
            "{}/api/v1/paths/{}/open-file",
            self.endpoint,
            percent_encode_path(&path)
        ));
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let steam_id: u64 = body.to_json().await?;
                Ok(steam_id)
            }
            _ => {
                let body = body.to_bytes().await?;
                Err(parse_error(parts, body)?)
            }
        }
    }

    pub(super) async fn delete(&self, path: &str) -> Result<()> {
        let path = build_rooted_abs_path(&self.root, path);

        let req = Request::post(format!(
            "{}/api/v1/paths/{}/delete",
            self.endpoint,
            percent_encode_path(&path)
        ));
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let body = body.to_bytes().await?;
                let err = parse_error(parts, body)?;
                if err.kind() == ErrorKind::NotFound {
                    return Ok(());
                }
                Err(err)
            }
        }
    }

    pub(super) async fn rename(&self, path: &str, dst: &str) -> Result<()> {
        let path = build_rooted_abs_path(&self.root, path);
        let dst = build_rooted_abs_path(&self.root, dst);

        let req = Request::post(format!(
            "{}/api/v1/paths/{}/rename?dst={}",
            self.endpoint,
            percent_encode_path(&path),
            percent_encode_path(&dst)
        ));

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub(super) async fn get_status(&self, path: &str) -> Result<FileInfo> {
        let path = build_rooted_abs_path(&self.root, path);

        let req = Request::post(format!(
            "{}/api/v1/paths/{}/get-status",
            self.endpoint,
            percent_encode_path(&path)
        ));

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let file_info: FileInfo = body.to_json().await?;
                Ok(file_info)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub(super) async fn list_status(&self, path: &str) -> Result<Vec<FileInfo>> {
        let path = build_rooted_abs_path(&self.root, path);

        let req = Request::post(format!(
            "{}/api/v1/paths/{}/list-status",
            self.endpoint,
            percent_encode_path(&path)
        ));

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let file_infos: Vec<FileInfo> = body.to_json().await?;
                Ok(file_infos)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs).await?)
            }
        }
    }

    /// TODO: we should implement range support correctly.
    ///
    /// Please refer to [alluxio-py](https://github.com/Alluxio/alluxio-py/blob/main/alluxio/const.py#L18)
    pub async fn read(&self, stream_id: u64, _: BytesRange, buf: WritableBuf) -> Result<usize> {
        let req = Request::post(format!(
            "{}/api/v1/streams/{}/read",
            self.endpoint, stream_id,
        ));

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        if !parts.status.is_success() {
            let bs = body.to_bytes().await?;
            return Err(parse_error(parts, bs)?);
        }

        body.read(buf).await
    }

    pub(super) async fn write(&self, stream_id: u64, body: RequestBody) -> Result<usize> {
        let req = Request::post(format!(
            "{}/api/v1/streams/{}/write",
            self.endpoint, stream_id
        ));
        let req = req.body(body).map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let size: usize = body.to_json().await?;
                Ok(size)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub(super) async fn close(&self, stream_id: u64) -> Result<()> {
        let req = Request::post(format!(
            "{}/api/v1/streams/{}/close",
            self.endpoint, stream_id
        ));
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs).await?)
            }
        }
    }
}
