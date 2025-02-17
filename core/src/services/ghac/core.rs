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

use crate::raw::{
    build_abs_path, new_json_serialize_error, new_request_build_error, percent_encode_path,
    BytesContentRange, BytesRange, HttpClient,
};
use crate::Buffer;
use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE};
use http::{header, Request};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

/// The base url for cache url.
pub const CACHE_URL_BASE: &str = "_apis/artifactcache";
/// Cache API requires to provide an accept header.
pub const CACHE_HEADER_ACCEPT: &str = "application/json;api-version=6.0-preview.1";
/// The cache url env for ghac.
///
/// The url will be like `https://artifactcache.actions.githubusercontent.com/<id>/`
pub const ACTIONS_CACHE_URL: &str = "ACTIONS_CACHE_URL";
/// The runtime token env for ghac.
///
/// This token will be valid for 6h and github action will running for 6
/// hours at most. So we don't need to refetch it again.
pub const ACTIONS_RUNTIME_TOKEN: &str = "ACTIONS_RUNTIME_TOKEN";

/// Core for github action cache services.
#[derive(Clone)]
pub struct GhacCore {
    // root should end with "/"
    pub root: String,

    pub cache_url: String,
    pub catch_token: String,
    pub version: String,

    pub client: HttpClient,
}

impl Debug for GhacCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GhacCore")
            .field("root", &self.root)
            .field("cache_url", &self.cache_url)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl GhacCore {
    pub fn ghac_query(&self, path: &str) -> crate::Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}{CACHE_URL_BASE}/cache?keys={}&version={}",
            self.cache_url,
            percent_encode_path(&p),
            self.version
        );

        let mut req = Request::get(&url);
        req = req.header(AUTHORIZATION, format!("Bearer {}", self.catch_token));
        req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn ghac_get_location(
        &self,
        location: &str,
        range: BytesRange,
    ) -> crate::Result<Request<Buffer>> {
        let mut req = Request::get(location);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    pub fn ghac_reserve(&self, path: &str) -> crate::Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}{CACHE_URL_BASE}/caches", self.cache_url);

        let bs = serde_json::to_vec(&GhacReserveRequest {
            key: p,
            version: self.version.to_string(),
        })
        .map_err(new_json_serialize_error)?;

        let mut req = Request::post(&url);
        req = req.header(AUTHORIZATION, format!("Bearer {}", self.catch_token));
        req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);
        req = req.header(CONTENT_LENGTH, bs.len());
        req = req.header(CONTENT_TYPE, "application/json");

        let req = req
            .body(Buffer::from(Bytes::from(bs)))
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn ghac_upload(
        &self,
        cache_id: i64,
        offset: u64,
        size: u64,
        body: Buffer,
    ) -> crate::Result<Request<Buffer>> {
        let url = format!("{}{CACHE_URL_BASE}/caches/{cache_id}", self.cache_url);

        let mut req = Request::patch(&url);
        req = req.header(AUTHORIZATION, format!("Bearer {}", self.catch_token));
        req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);
        req = req.header(CONTENT_LENGTH, size);
        req = req.header(CONTENT_TYPE, "application/octet-stream");
        req = req.header(
            CONTENT_RANGE,
            BytesContentRange::default()
                .with_range(offset, offset + size - 1)
                .to_header(),
        );

        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn ghac_commit(&self, cache_id: i64, size: u64) -> crate::Result<Request<Buffer>> {
        let url = format!("{}{CACHE_URL_BASE}/caches/{cache_id}", self.cache_url);

        let bs =
            serde_json::to_vec(&GhacCommitRequest { size }).map_err(new_json_serialize_error)?;

        let mut req = Request::post(&url);
        req = req.header(AUTHORIZATION, format!("Bearer {}", self.catch_token));
        req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);
        req = req.header(CONTENT_TYPE, "application/json");
        req = req.header(CONTENT_LENGTH, bs.len());

        let req = req
            .body(Buffer::from(Bytes::from(bs)))
            .map_err(new_request_build_error)?;

        Ok(req)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GhacQueryResponse {
    // Not used fields.
    // cache_key: String,
    // scope: String,
    pub archive_location: String,
}

#[derive(Serialize)]
pub struct GhacReserveRequest {
    pub key: String,
    pub version: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GhacReserveResponse {
    pub cache_id: i64,
}

#[derive(Serialize)]
pub struct GhacCommitRequest {
    pub size: u64,
}
