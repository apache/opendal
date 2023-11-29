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

use bytes::Bytes;
use http::Request;
use http::Response;
use http::{header, StatusCode};
use serde_json::json;

use super::backend::RepoType;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct HuggingFaceCore {
    pub repo_type: RepoType,
    pub repo_id: String,
    pub revision: String,
    pub root: String,
    pub token: Option<String>,

    pub client: HttpClient,
}

impl Debug for HuggingFaceCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HuggingFaceCore")
            .field("repo_type", &self.repo_type)
            .field("repo_id", &self.repo_id)
            .field("revision", &self.revision)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl HuggingFaceCore {
    pub async fn hf_path_info(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = match self.repo_type {
            RepoType::Model => format!(
                "https://huggingface.co/api/models/{}/paths-info/{}",
                &self.repo_id, &self.revision
            ),
            RepoType::Dataset => format!(
                "https://huggingface.co/api/datasets/{}/paths-info/{}",
                &self.repo_id, &self.revision
            ),
        };

        let mut req = Request::post(&url);

        if let Some(token) = &self.token {
            let auth_header_content = format_authorization_by_bearer(token)?;
            req = req.header(header::AUTHORIZATION, auth_header_content);
        }

        req = req.header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");

        let path = percent_encode_path(&p);
        let req_body = &json!({
            "paths": path,
            "expand": "True",
        });

        // NOTE: We need to transfer body to x-www-form-urlencoded format.
        let req_body =
            serde_urlencoded::to_string(&req_body).expect("failed to encode request body");

        let req = req
            .body(AsyncBody::Bytes(Bytes::from(req_body)))
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn hf_list(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = match self.repo_type {
            RepoType::Model => format!(
                "https://huggingface.co/api/models/{}/tree/{}/{}?expand=True",
                &self.repo_id,
                &self.revision,
                percent_encode_path(&p)
            ),
            RepoType::Dataset => format!(
                "https://huggingface.co/api/datasets/{}/tree/{}/{}?expand=True",
                &self.repo_id,
                &self.revision,
                percent_encode_path(&p)
            ),
        };

        if recursive {
            url.push_str("&recursive=True");
        }

        let mut req = Request::get(&url);

        if let Some(token) = &self.token {
            let auth_header_content = format_authorization_by_bearer(token)?;
            req = req.header(header::AUTHORIZATION, auth_header_content);
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn hf_read(&self, path: &str, arg: OpRead) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = match self.repo_type {
            RepoType::Model => format!(
                "https://huggingface.co/{}/resolve/{}/{}",
                &self.repo_id,
                &self.revision,
                percent_encode_path(&p)
            ),
            RepoType::Dataset => format!(
                "https://huggingface.co/datasets/{}/resolve/{}/{}",
                &self.repo_id,
                &self.revision,
                percent_encode_path(&p)
            ),
        };

        let mut req = Request::get(&url);

        if let Some(token) = &self.token {
            let auth_header_content = format_authorization_by_bearer(token)?;
            req = req.header(header::AUTHORIZATION, auth_header_content);
        }

        let range = arg.range();
        if !range.is_full() {
            req = req.header("Range", &range.to_header());
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        let resp = self.client.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(resp),
            _ => Err(parse_error(resp).await?),
        }
    }
}
