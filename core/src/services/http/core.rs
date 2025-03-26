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
use std::sync::Arc;

use http::header;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::Request;
use http::Response;

use crate::raw::*;
use crate::*;

pub struct HttpCore {
    pub info: Arc<AccessorInfo>,

    pub endpoint: String,
    pub root: String,

    pub authorization: Option<String>,
}

impl Debug for HttpCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpCore")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish()
    }
}

impl HttpCore {
    pub fn has_authorization(&self) -> bool {
        self.authorization.is_some()
    }

    pub fn http_get_request(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Request<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    pub async fn http_get(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let req = self.http_get_request(path, range, args)?;
        self.info.http_client().fetch(req).await
    }

    pub fn http_head_request(&self, path: &str, args: &OpStat) -> Result<Request<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::head(&url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    pub async fn http_head(&self, path: &str, args: &OpStat) -> Result<Response<Buffer>> {
        let req = self.http_head_request(path, args)?;
        self.info.http_client().send(req).await
    }
}
