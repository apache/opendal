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

use crate::raw::*;
use crate::*;
use http::header;
use http::Request;
use http::Response;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub struct LakefsCore {
    pub info: Arc<AccessorInfo>,
    pub endpoint: String,
    pub repository: String,
    pub branch: String,
    pub root: String,
    pub username: String,
    pub password: String,
}

impl Debug for LakefsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakefsCore")
            .field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .field("password", &self.password)
            .field("root", &self.root)
            .field("repository", &self.repository)
            .field("branch", &self.branch)
            .finish_non_exhaustive()
    }
}

impl LakefsCore {
    pub async fn get_object_metadata(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/refs/{}/objects/stat?path={}",
            self.endpoint,
            self.repository,
            self.branch,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        // Inject operation to the request.
        let req = req.extension(Operation::Read);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn get_object_content(
        &self,
        path: &str,
        range: BytesRange,
        _args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/refs/{}/objects?path={}",
            self.endpoint,
            self.repository,
            self.branch,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }
        // Inject operation to the request.
        let req = req.extension(Operation::Read);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().fetch(req).await
    }

    pub async fn list_objects(
        &self,
        path: &str,
        delimiter: &str,
        amount: &Option<usize>,
        after: Option<String>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/api/v1/repositories/{}/refs/{}/objects/ls?",
            self.endpoint, self.repository, self.branch
        );

        if !p.is_empty() {
            url.push_str(&format!("&prefix={}", percent_encode_path(&p)));
        }

        if !delimiter.is_empty() {
            url.push_str(&format!("&delimiter={}", delimiter));
        }

        if let Some(amount) = amount {
            url.push_str(&format!("&amount={}", amount));
        }

        if let Some(after) = after {
            url.push_str(&format!("&after={}", after));
        }

        let mut req = Request::get(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        // Inject operation to the request.
        let req = req.extension(Operation::Read);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn upload_object(
        &self,
        path: &str,
        _args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/branches/{}/objects?path={}",
            self.endpoint,
            self.repository,
            self.branch,
            percent_encode_path(&p)
        );

        let mut req = Request::post(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        // Inject operation to the request.
        let req = req.extension(Operation::Write);
        let req = req.body(body).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn delete_object(&self, path: &str, _args: &OpDelete) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/branches/{}/objects?path={}",
            self.endpoint,
            self.repository,
            self.branch,
            percent_encode_path(&p)
        );

        let mut req = Request::delete(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        // Inject operation to the request.
        let req = req.extension(Operation::Delete);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn copy_object(&self, path: &str, dest: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();
        let d = build_abs_path(&self.root, dest)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/branches/{}/objects/copy?dest_path={}",
            self.endpoint,
            self.repository,
            self.branch,
            percent_encode_path(&d)
        );

        let mut req = Request::post(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        req = req.header(header::CONTENT_TYPE, "application/json");
        let mut map = HashMap::new();
        map.insert("src_path", p);

        let req = req
            // Inject operation to the request.
            .extension(Operation::Delete)
            .body(serde_json::to_vec(&map).unwrap().into())
            .map_err(new_request_build_error)?;
        self.info.http_client().send(req).await
    }
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub(super) struct LakefsStatus {
    pub path: String,
    pub path_type: String,
    pub physical_address: String,
    pub checksum: String,
    pub size_bytes: Option<u64>,
    pub mtime: i64,
    pub content_type: Option<String>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub(super) struct LakefsListResponse {
    pub pagination: Pagination,
    pub results: Vec<LakefsStatus>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub(super) struct Pagination {
    pub has_more: bool,
    pub max_per_page: u64,
    pub next_offset: String,
    pub results: u64,
}
