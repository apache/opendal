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

use std::default::Default;
use std::fmt::Debug;
use std::fmt::Formatter;

use bytes::Bytes;
use http::header;
use http::request::Builder;
use http::Request;
use http::Response;
use serde::Deserialize;
use serde::Serialize;

use crate::raw::build_rooted_abs_path;
use crate::raw::new_json_serialize_error;
use crate::raw::new_request_build_error;
use crate::raw::AsyncBody;
use crate::raw::HttpClient;
use crate::raw::IncomingAsyncBody;
use crate::types::Result;

pub struct DropboxCore {
    pub token: String,
    pub client: HttpClient,
    pub root: String,
}

impl Debug for DropboxCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("DropboxCore");
        de.finish()
    }
}

impl DropboxCore {
    pub async fn dropbox_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url: String = "https://content.dropboxapi.com/2/files/download".to_string();
        let download_args = DropboxDownloadArgs {
            path: build_rooted_abs_path(&self.root, path),
        };
        let request_payload =
            serde_json::to_string(&download_args).map_err(new_json_serialize_error)?;
        let request = self
            .build_auth_header(Request::post(&url))
            .header("Dropbox-API-Arg", request_payload)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        self.client.send(request).await
    }

    pub async fn dropbox_update(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = "https://content.dropboxapi.com/2/files/upload".to_string();
        let args = DropboxUploadArgs {
            path: build_rooted_abs_path(&self.root, path),
            ..Default::default()
        };
        let mut request_builder = Request::post(&url);
        if let Some(size) = size {
            request_builder = request_builder.header(header::CONTENT_LENGTH, size);
        }
        if let Some(mime) = content_type {
            request_builder = request_builder.header(header::CONTENT_TYPE, mime);
        }
        let request = self
            .build_auth_header(request_builder)
            .header(
                "Dropbox-API-Arg",
                serde_json::to_string(&args).map_err(new_json_serialize_error)?,
            )
            .body(body)
            .map_err(new_request_build_error)?;

        self.client.send(request).await
    }

    pub async fn dropbox_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = "https://api.dropboxapi.com/2/files/delete_v2".to_string();
        let args = DropboxDeleteArgs {
            path: build_rooted_abs_path(&self.root, path),
        };

        let bs = Bytes::from(serde_json::to_string(&args).map_err(new_json_serialize_error)?);

        let request = self
            .build_auth_header(Request::post(&url))
            .header(header::CONTENT_TYPE, "application/json")
            .body(AsyncBody::Bytes(bs))
            .map_err(new_request_build_error)?;
        self.client.send(request).await
    }

    fn build_auth_header(&self, mut req: Builder) -> Builder {
        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);
        req
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxDownloadArgs {
    path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxUploadArgs {
    path: String,
    mode: String,
    mute: bool,
    autorename: bool,
    strict_conflict: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DropboxDeleteArgs {
    path: String,
}

impl Default for DropboxUploadArgs {
    fn default() -> Self {
        DropboxUploadArgs {
            mode: "overwrite".to_string(),
            path: "".to_string(),
            mute: true,
            autorename: false,
            strict_conflict: false,
        }
    }
}
