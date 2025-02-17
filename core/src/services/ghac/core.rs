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

use super::error::{parse_error, parse_grpc_error};
use crate::raw::{
    build_abs_path, new_json_deserialize_error, new_json_serialize_error, new_request_build_error,
    percent_encode_path, HttpClient,
};
use crate::*;
use ::ghac::v1 as ghac_grpc;
use bytes::{Buf, Bytes};
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use http::{Request, StatusCode, Uri};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use tokio::sync::OnceCell;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::IntoRequest;

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
/// The cache service version env for ghac.
pub const ACTIONS_CACHE_SERVICE_V2: &str = "ACTIONS_CACHE_SERVICE_V2";
/// The results url env for ghac.
pub const ACTIONS_RESULTS_URL: &str = "ACTIONS_RESULTS_URL";

/// The version of github action cache.
#[derive(Clone, Copy, Debug)]
pub enum GhacVersion {
    V1,
    V2,
}

/// Core for github action cache services.
#[derive(Clone)]
pub struct GhacCore {
    // root should end with "/"
    pub root: String,

    pub cache_url: String,
    pub catch_token: String,
    pub version: String,

    pub service_version: GhacVersion,
    pub http_client: HttpClient,
    pub grpc_client: OnceCell<ghac_grpc::cache_service_client::CacheServiceClient<Channel>>,
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
    pub async fn get_grpc_client(
        &self,
    ) -> Result<ghac_grpc::cache_service_client::CacheServiceClient<Channel>> {
        let uri = http::Uri::from_str(&self.cache_url).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to parse cache url")
                .with_context("cache_url", &self.cache_url)
                .set_source(err)
        })?;
        self.grpc_client
            .get_or_try_init(|| async move {
                let channel = Channel::builder(uri.clone())
                    .tls_config(ClientTlsConfig::default().with_enabled_roots())
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "Failed to setup tls config for grpc service",
                        )
                        .with_context("uri", uri.to_string())
                        .set_source(err)
                    })?
                    .connect()
                    .await
                    .map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "Failed to connect to cache service")
                            .with_context("uri", uri.to_string())
                            .set_source(err)
                    })?;
                Ok(ghac_grpc::cache_service_client::CacheServiceClient::new(
                    channel,
                ))
            })
            .await
            .cloned()
    }

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
                let resp = self.http_client.send(req).await?;
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
                let grpc_client = self.get_grpc_client().await?;

                let req = ghac_grpc::GetCacheEntryDownloadUrlRequest {
                    key: p,
                    version: self.version.clone(),

                    metadata: None,
                    restore_keys: vec![],
                };
                let mut req = req.into_request();
                req.metadata_mut().insert(
                    AUTHORIZATION.as_str(),
                    format!("Bearer {}", self.catch_token).parse().unwrap(),
                );
                let resp = grpc_client
                    .clone()
                    .get_cache_entry_download_url(req)
                    .await
                    .map_err(parse_grpc_error)?;
                Ok(resp.into_inner().signed_download_url)
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
                req = req.header(CONTENT_TYPE, "application/json");
                req = req.header(CONTENT_LENGTH, bs.len());

                let req = req
                    .body(Buffer::from(Bytes::from(bs)))
                    .map_err(new_request_build_error)?;
                let resp = self.http_client.send(req).await?;
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
                let grpc_client = self.get_grpc_client().await?;

                let req = ghac_grpc::CreateCacheEntryRequest {
                    key: p,
                    version: self.version.clone(),

                    metadata: None,
                };
                let mut req = req.into_request();
                req.metadata_mut().insert(
                    AUTHORIZATION.as_str(),
                    format!("Bearer {}", self.catch_token).parse().unwrap(),
                );
                let resp = grpc_client
                    .clone()
                    .create_cache_entry(req)
                    .await
                    .map_err(parse_grpc_error)?;
                Ok(resp.into_inner().signed_upload_url)
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
                    .header(CONTENT_TYPE, "application/json")
                    .header(CONTENT_LENGTH, bs.len())
                    .body(Buffer::from(bs))
                    .map_err(new_request_build_error)?;
                let resp = self.http_client.send(req).await?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(parse_error(resp))
                }
            }
            GhacVersion::V2 => {
                let grpc_client = self.get_grpc_client().await?;

                let req = ghac_grpc::FinalizeCacheEntryUploadRequest {
                    key: p,
                    version: self.version.clone(),
                    size_bytes: size as i64,

                    metadata: None,
                };
                let mut req = req.into_request();
                req.metadata_mut().insert(
                    AUTHORIZATION.as_str(),
                    format!("Bearer {}", self.catch_token).parse().unwrap(),
                );
                let _ = grpc_client
                    .clone()
                    .finalize_cache_entry_upload(req)
                    .await
                    .map_err(parse_grpc_error)?;
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
