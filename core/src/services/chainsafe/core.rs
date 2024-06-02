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

use bytes::Bytes;
use http::header;
use http::Request;
use http::Response;
use serde::Deserialize;
use serde_json::json;

use crate::raw::*;
use crate::*;

/// Core of [chainsafe](https://storage.chainsafe.io/) services support.
#[derive(Clone)]
pub struct ChainsafeCore {
    /// The root of this core.
    pub root: String,
    /// The api_key of this core.
    pub api_key: String,
    /// The bucket id of this backend.
    pub bucket_id: String,

    pub client: HttpClient,
}

impl Debug for ChainsafeCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("bucket_id", &self.bucket_id)
            .finish_non_exhaustive()
    }
}

impl ChainsafeCore {
    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.client.send(req).await
    }
}

impl ChainsafeCore {
    pub async fn download_object(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<HttpBody>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.chainsafe.io/api/v1/bucket/{}/download",
            self.bucket_id
        );

        let req_body = &json!({
            "path": path,
        });
        let body = Buffer::from(Bytes::from(req_body.to_string()));

        let req = Request::post(url)
            .header(
                header::AUTHORIZATION,
                format_authorization_by_bearer(&self.api_key)?,
            )
            .header(header::RANGE, range.to_header())
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(new_request_build_error)?;

        self.client.fetch(req).await
    }

    pub async fn object_info(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.chainsafe.io/api/v1/bucket/{}/file",
            self.bucket_id
        );

        let req_body = &json!({
            "path": path,
        });

        let body = Buffer::from(Bytes::from(req_body.to_string()));

        let req = Request::post(url)
            .header(
                header::AUTHORIZATION,
                format_authorization_by_bearer(&self.api_key)?,
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn move_object(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let url = format!(
            "https://api.chainsafe.io/api/v1/bucket/{}/mv",
            self.bucket_id
        );

        let req_body = &json!({
            "path": from,
            "new_path": to,
        });
        let body = Buffer::from(Bytes::from(req_body.to_string()));

        let req = Request::post(url)
            .header(
                header::AUTHORIZATION,
                format_authorization_by_bearer(&self.api_key)?,
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn delete_object(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.chainsafe.io/api/v1/bucket/{}/rm",
            self.bucket_id
        );

        let req_body = &json!({
            "paths": vec![path],
        });

        let body = Buffer::from(Bytes::from(req_body.to_string()));

        let req = Request::post(url)
            .header(
                header::AUTHORIZATION,
                format_authorization_by_bearer(&self.api_key)?,
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn upload_object(&self, path: &str, bs: Buffer) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.chainsafe.io/api/v1/bucket/{}/upload",
            self.bucket_id
        );

        let file_part = FormDataPart::new("file").content(bs);

        let multipart = Multipart::new()
            .part(file_part)
            .part(FormDataPart::new("path").content(path));

        let req = Request::post(url).header(
            header::AUTHORIZATION,
            format_authorization_by_bearer(&self.api_key)?,
        );

        let req = multipart.apply(req)?;

        self.send(req).await
    }

    pub async fn list_objects(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.chainsafe.io/api/v1/bucket/{}/ls",
            self.bucket_id
        );

        let req_body = &json!({
            "path": path,
        });

        let body = Buffer::from(Bytes::from(req_body.to_string()));

        let req = Request::post(url)
            .header(
                header::AUTHORIZATION,
                format_authorization_by_bearer(&self.api_key)?,
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn create_dir(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "https://api.chainsafe.io/api/v1/bucket/{}/mkdir",
            self.bucket_id
        );

        let req_body = &json!({
            "path": path,
        });

        let body = Buffer::from(Bytes::from(req_body.to_string()));

        let req = Request::post(url)
            .header(
                header::AUTHORIZATION,
                format_authorization_by_bearer(&self.api_key)?,
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(new_request_build_error)?;

        self.send(req).await
    }
}

#[derive(Debug, Deserialize)]
pub struct Info {
    pub name: String,
    pub cid: String,
    pub content_type: String,
    pub size: u64,
    pub version: i64,
    pub created_at: i64,
}

#[derive(Deserialize)]
pub struct ObjectInfoResponse {
    pub content: Info,
}

pub(super) fn parse_info(info: Info) -> Metadata {
    let mode = if info.content_type == "application/chainsafe-files-directory" {
        EntryMode::DIR
    } else {
        EntryMode::FILE
    };

    let mut md = Metadata::new(mode);

    md.set_content_length(info.size)
        .set_content_type(&info.content_type);

    md
}
