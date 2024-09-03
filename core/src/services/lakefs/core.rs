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

use http::header;
use http::Request;
use http::Response;
use serde::Deserialize;

use crate::raw::*;
use crate::*;

pub struct LakefsCore {
    pub endpoint: String,
    pub repository: String,
    pub branch: String,
    pub root: String,
    pub username: String,
    pub password: String,
    pub client: HttpClient,
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

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
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

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.fetch(req).await
    }

    pub async fn list_objects(
        &self,
        path: &str,
        delimiter: &str,
        amount: &Option<usize>,
        after: &Option<&str>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/api/v1/repositories/{}/refs/{}/objects/ls?",
            self.endpoint, self.repository, self.branch
        );

        if !p.is_empty() {
            write!(url, "&prefix={}", percent_encode_path(&p))
                .expect("write into string must succeed");
        }

        if !delimiter.is_empty() {
            write!(url, "&delimiter={delimiter}").expect("write into string must succeed");
        }

        if let Some(amount) = amount {
            write!(url, "&amount={amount}").expect("write into string must succeed");
        }

        if let Some(after) = after {
            write!(url, "&after={after}").expect("write into string must succeed");
        }

        let mut req = Request::get(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[allow(dead_code)]
pub(super) struct LakefsStatus {
    pub path: String,
    pub path_type: String,
    pub physical_address: String,
    pub checksum: String,
    pub size_bytes: u64,
    pub mtime: i64,
    pub content_type: String,
}
