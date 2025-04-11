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

use std::env;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use ::ghac::v1 as ghac_types;
use bytes::{Buf, Bytes};
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use http::{Request, StatusCode, Uri};
use prost::Message;
use serde::{Deserialize, Serialize};

use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// The base url for cache url.
pub const CACHE_URL_BASE: &str = "_apis/artifactcache";
/// The base url for cache service v2.
pub const CACHE_URL_BASE_V2: &str = "twirp/github.actions.results.api.v1.CacheService";
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
/// The cache service version env for ghac.
pub const ACTIONS_CACHE_SERVICE_V2: &str = "ACTIONS_CACHE_SERVICE_V2";
/// The results url env for ghac.
pub const ACTIONS_RESULTS_URL: &str = "ACTIONS_RESULTS_URL";
/// The content type for protobuf.
pub const CONTENT_TYPE_JSON: &str = "application/json";
/// The content type for protobuf.
pub const CONTENT_TYPE_PROTOBUF: &str = "application/protobuf";

/// The version of github action cache.
#[derive(Clone, Copy, Debug)]
pub enum GhacVersion {
    V1,
    V2,
}

/// Core for github action cache services.
#[derive(Clone)]
pub struct GhacCore {
    pub info: Arc<AccessorInfo>,

    // root should end with "/"
    pub root: String,

    pub cache_url: String,
    pub catch_token: String,
    pub version: String,

    pub service_version: GhacVersion,
}

impl Debug for GhacCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GhacCore")
            .field("root", &self.root)
            .field("cache_url", &self.cache_url)
            .field("version", &self.version)
            .field("service_version", &self.service_version)
            .finish_non_exhaustive()
    }
}

impl GhacCore {
    pub async fn ghac_get_download_url(&self, path: &str) -> Result<String> {
        let p = build_abs_path(&self.root, path);

        match self.service_version {
            GhacVersion::V1 => {
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
                let resp = self.info.http_client().send(req).await?;
                let location = if resp.status() == StatusCode::OK {
                    let slc = resp.into_body();
                    let query_resp: GhacQueryResponse = serde_json::from_reader(slc.reader())
                        .map_err(new_json_deserialize_error)?;
                    query_resp.archive_location
                } else {
                    return Err(parse_error(resp));
                };
                Ok(location)
            }
            GhacVersion::V2 => {
                let url = format!(
                    "{}{CACHE_URL_BASE_V2}/GetCacheEntryDownloadURL",
                    self.cache_url,
                );

                let req = ghac_types::GetCacheEntryDownloadUrlRequest {
                    key: p,
                    version: self.version.clone(),

                    metadata: None,
                    restore_keys: vec![],
                };

                let body = Buffer::from(req.encode_to_vec());

                let req = Request::post(&url)
                    .header(AUTHORIZATION, format!("Bearer {}", self.catch_token))
                    .header(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)
                    .header(CONTENT_LENGTH, body.len())
                    .body(body)
                    .map_err(new_request_build_error)?;
                let resp = self.info.http_client().send(req).await?;
                let location = if resp.status() == StatusCode::OK {
                    let slc = resp.into_body();
                    let query_resp = ghac_types::GetCacheEntryDownloadUrlResponse::decode(slc)
                        .map_err(new_prost_decode_error)?;
                    if !query_resp.ok {
                        let mut err = Error::new(
                            ErrorKind::NotFound,
                            "GetCacheEntryDownloadURL returns non-ok, the key doesn't exist",
                        );

                        // GHAC is a cache service, so it's acceptable for it to occasionally not contain
                        // data that users have just written. However, we don't want users to always have
                        // to retry reading it, nor do we want our CI to fail constantly.
                        //
                        // Here's the trick: we check if the environment variable `OPENDAL_TEST` is set to `ghac`.
                        // If it is, we mark the error as temporary to allow retries in the test CI.
                        if env::var("OPENDAL_TEST") == Ok("ghac".to_string()) {
                            err = err.set_temporary();
                        }
                        return Err(err);
                    }
                    query_resp.signed_download_url
                } else {
                    return Err(parse_error(resp));
                };

                Ok(location)
            }
        }
    }

    pub async fn ghac_get_upload_url(&self, path: &str) -> Result<String> {
        let p = build_abs_path(&self.root, path);

        match self.service_version {
            GhacVersion::V1 => {
                let url = format!("{}{CACHE_URL_BASE}/caches", self.cache_url);

                let bs = serde_json::to_vec(&GhacReserveRequest {
                    key: p,
                    version: self.version.to_string(),
                })
                .map_err(new_json_serialize_error)?;

                let mut req = Request::post(&url);
                req = req.header(AUTHORIZATION, format!("Bearer {}", self.catch_token));
                req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);
                req = req.header(CONTENT_TYPE, CONTENT_TYPE_JSON);
                req = req.header(CONTENT_LENGTH, bs.len());

                let req = req
                    .body(Buffer::from(Bytes::from(bs)))
                    .map_err(new_request_build_error)?;
                let resp = self.info.http_client().send(req).await?;
                let cache_id = if resp.status().is_success() {
                    let slc = resp.into_body();
                    let reserve_resp: GhacReserveResponse = serde_json::from_reader(slc.reader())
                        .map_err(new_json_deserialize_error)?;
                    reserve_resp.cache_id
                } else {
                    return Err(
                        parse_error(resp).map(|err| err.with_operation("Backend::ghac_reserve"))
                    );
                };

                let url = format!("{}{CACHE_URL_BASE}/caches/{cache_id}", self.cache_url);
                Ok(url)
            }
            GhacVersion::V2 => {
                let url = format!("{}{CACHE_URL_BASE_V2}/CreateCacheEntry", self.cache_url,);

                let req = ghac_types::CreateCacheEntryRequest {
                    key: p,
                    version: self.version.clone(),
                    metadata: None,
                };

                let body = Buffer::from(req.encode_to_vec());

                let req = Request::post(&url)
                    .header(AUTHORIZATION, format!("Bearer {}", self.catch_token))
                    .header(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)
                    .header(CONTENT_LENGTH, body.len())
                    .body(body)
                    .map_err(new_request_build_error)?;
                let resp = self.info.http_client().send(req).await?;
                let location = if resp.status() == StatusCode::OK {
                    let (parts, slc) = resp.into_parts();
                    let query_resp = ghac_types::CreateCacheEntryResponse::decode(slc)
                        .map_err(new_prost_decode_error)?;
                    if !query_resp.ok {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "create cache entry returns non-ok",
                        )
                        .with_context("parts", format!("{:?}", parts)));
                    }
                    query_resp.signed_upload_url
                } else {
                    return Err(parse_error(resp));
                };
                Ok(location)
            }
        }
    }

    pub async fn ghac_finalize_upload(&self, path: &str, url: &str, size: u64) -> Result<()> {
        let p = build_abs_path(&self.root, path);

        match self.service_version {
            GhacVersion::V1 => {
                let bs = serde_json::to_vec(&GhacCommitRequest { size })
                    .map_err(new_json_serialize_error)?;

                let req = Request::post(url)
                    .header(AUTHORIZATION, format!("Bearer {}", self.catch_token))
                    .header(ACCEPT, CACHE_HEADER_ACCEPT)
                    .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
                    .header(CONTENT_LENGTH, bs.len())
                    .body(Buffer::from(bs))
                    .map_err(new_request_build_error)?;
                let resp = self.info.http_client().send(req).await?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(parse_error(resp))
                }
            }
            GhacVersion::V2 => {
                let url = format!(
                    "{}{CACHE_URL_BASE_V2}/FinalizeCacheEntryUpload",
                    self.cache_url,
                );

                let req = ghac_types::FinalizeCacheEntryUploadRequest {
                    key: p,
                    version: self.version.clone(),
                    size_bytes: size as i64,

                    metadata: None,
                };
                let body = Buffer::from(req.encode_to_vec());

                let req = Request::post(&url)
                    .header(AUTHORIZATION, format!("Bearer {}", self.catch_token))
                    .header(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)
                    .header(CONTENT_LENGTH, body.len())
                    .body(body)
                    .map_err(new_request_build_error)?;
                let resp = self.info.http_client().send(req).await?;
                if resp.status() != StatusCode::OK {
                    return Err(parse_error(resp));
                };
                Ok(())
            }
        }
    }
}

/// Determines if the current environment is GitHub Enterprise Server (GHES)
///
/// We need to know this since GHES doesn't support ghac v2 yet.
pub fn is_ghes() -> bool {
    // Fetch GitHub Server URL with fallback to "https://github.com"
    let server_url =
        env::var("GITHUB_SERVER_URL").unwrap_or_else(|_| "https://github.com".to_string());

    let Ok(url) = Uri::from_str(&server_url) else {
        // We just return false if the URL is invalid
        return false;
    };

    // Check against known non-GHES host patterns
    let hostname = url.host().unwrap_or("").trim_end().to_lowercase();

    let is_github_host = hostname == "github.com";
    let is_ghe_host = hostname.ends_with(".ghe.com");
    let is_localhost = hostname.ends_with(".localhost");

    !is_github_host && !is_ghe_host && !is_localhost
}

/// Determines the cache service version based on environment
pub fn get_cache_service_version() -> GhacVersion {
    if is_ghes() {
        // GHES only supports v1 regardless of feature flags
        GhacVersion::V1
    } else {
        // Check for presence of non-empty ACTIONS_CACHE_SERVICE_V2
        let value = env::var(ACTIONS_CACHE_SERVICE_V2).unwrap_or_default();
        if value.is_empty() {
            GhacVersion::V1
        } else {
            GhacVersion::V2
        }
    }
}

/// Returns the appropriate cache service URL based on version
pub fn get_cache_service_url(version: GhacVersion) -> String {
    match version {
        GhacVersion::V1 => {
            // Priority order for v1: CACHE_URL -> RESULTS_URL
            env::var(ACTIONS_CACHE_URL)
                .or_else(|_| env::var(ACTIONS_RESULTS_URL))
                .unwrap_or_default()
        }
        GhacVersion::V2 => {
            // Only RESULTS_URL is used for v2
            env::var(ACTIONS_RESULTS_URL).unwrap_or_default()
        }
    }
}

/// Parse prost decode error into opendal::Error.
pub fn new_prost_decode_error(e: prost::DecodeError) -> Error {
    Error::new(ErrorKind::Unexpected, "deserialize protobuf").set_source(e)
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
