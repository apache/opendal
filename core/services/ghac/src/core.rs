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
use std::fmt::Debug;
use std::str::FromStr;

use ::ghac::v1 as ghac_types;
use bytes::Buf;
use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use http::header::ACCEPT;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_RANGE;
use http::header::CONTENT_TYPE;
use http::header::{self};
use prost::Message;
use serde::Deserialize;
use serde::Serialize;

use opendal_core::raw::*;
use opendal_core::*;

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
    pub info: ServiceInfo,
    pub capability: Capability,

    // root should end with "/"
    pub root: String,

    pub cache_url: String,
    pub catch_token: String,
    pub version: String,

    pub service_version: GhacVersion,
}

impl Debug for GhacCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GhacCore")
            .field("root", &self.root)
            .field("cache_url", &self.cache_url)
            .field("version", &self.version)
            .field("service_version", &self.service_version)
            .finish_non_exhaustive()
    }
}

impl GhacCore {
    async fn ghac_get_download_url(&self, ctx: &OperationContext, path: &str) -> Result<String> {
        let p = build_absolute_path(&self.root, path);

        match self.service_version {
            GhacVersion::V1 => {
                let url = build_cache_url(
                    &self.cache_url,
                    format!(
                        "{CACHE_URL_BASE}/cache?keys={}&version={}",
                        percent_encode_path(&p),
                        self.version
                    ),
                );

                let mut req = Request::get(&url);
                req = req.header(AUTHORIZATION, format!("Bearer {}", self.catch_token));
                req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);

                let req = req
                    .extension(Operation::Read)
                    .extension(ServiceOperation("GetCacheEntry"))
                    .body(Buffer::new())
                    .map_err(new_request_build_error)?;
                let resp = ctx.http_transport().send(req).await?;
                let location = if resp.status() == StatusCode::OK {
                    let slc = resp.into_body();
                    let query_resp: GhacQueryResponse = serde_json::from_reader(slc.reader())
                        .map_err(new_json_deserialize_error)?;
                    query_resp.archive_location
                } else {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("GetCacheEntry")),
                        resp,
                    ));
                };
                Ok(location)
            }
            GhacVersion::V2 => {
                let url = build_cache_url(
                    &self.cache_url,
                    format!("{CACHE_URL_BASE_V2}/GetCacheEntryDownloadURL"),
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
                    .extension(Operation::Read)
                    .extension(ServiceOperation("GetCacheEntryDownloadURL"))
                    .body(body)
                    .map_err(new_request_build_error)?;
                let resp = ctx.http_transport().send(req).await?;
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
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("GetCacheEntryDownloadUrl")),
                        resp,
                    ));
                };

                Ok(location)
            }
        }
    }

    pub async fn ghac_stat(&self, ctx: &OperationContext, path: &str) -> Result<Response<Buffer>> {
        let location = self.ghac_get_download_url(ctx, path).await?;

        let req = Request::get(location)
            .header(header::RANGE, "bytes=0-0")
            .extension(Operation::Stat)
            .extension(ServiceOperation("DownloadCache"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn ghac_read(
        &self,
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<HttpBody>> {
        let location = self.ghac_get_download_url(ctx, path).await?;

        let mut req = Request::get(location);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }
        let req = req
            .extension(Operation::Read)
            .extension(ServiceOperation("DownloadCache"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().fetch(req).await
    }

    pub async fn ghac_get_upload_url(&self, ctx: &OperationContext, path: &str) -> Result<String> {
        let p = build_absolute_path(&self.root, path);

        match self.service_version {
            GhacVersion::V1 => {
                let url = build_cache_url(&self.cache_url, format!("{CACHE_URL_BASE}/caches"));

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
                    .extension(Operation::Write)
                    .extension(ServiceOperation("ReserveCache"))
                    .body(Buffer::from(Bytes::from(bs)))
                    .map_err(new_request_build_error)?;
                let resp = ctx.http_transport().send(req).await?;
                let cache_id = if resp.status().is_success() {
                    let slc = resp.into_body();
                    let reserve_resp: GhacReserveResponse = serde_json::from_reader(slc.reader())
                        .map_err(new_json_deserialize_error)?;
                    reserve_resp.cache_id
                } else {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("ReserveCache")),
                        resp,
                    )
                    .with_operation("Backend::ghac_reserve"));
                };

                let url = build_cache_url(
                    &self.cache_url,
                    format!("{CACHE_URL_BASE}/caches/{cache_id}"),
                );
                Ok(url)
            }
            GhacVersion::V2 => {
                let url = build_cache_url(
                    &self.cache_url,
                    format!("{CACHE_URL_BASE_V2}/CreateCacheEntry"),
                );

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
                    .extension(Operation::Write)
                    .extension(ServiceOperation("CreateCacheEntry"))
                    .body(body)
                    .map_err(new_request_build_error)?;
                let resp = ctx.http_transport().send(req).await?;
                let location = if resp.status() == StatusCode::OK {
                    let (parts, slc) = resp.into_parts();
                    let query_resp = ghac_types::CreateCacheEntryResponse::decode(slc)
                        .map_err(new_prost_decode_error)?;
                    if !query_resp.ok {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "create cache entry returns non-ok",
                        )
                        .with_context("parts", format!("{parts:?}")));
                    }
                    query_resp.signed_upload_url
                } else {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("CreateCacheEntry")),
                        resp,
                    ));
                };
                Ok(location)
            }
        }
    }

    pub async fn ghac_v1_write(
        &self,
        ctx: &OperationContext,
        upload_url: &str,
        size: u64,
        offset: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let end = size
            .checked_sub(1)
            .and_then(|last| offset.checked_add(last))
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "invalid ghac v1 upload range"))?;

        let mut req = Request::patch(upload_url);
        req = req.header(AUTHORIZATION, format!("Bearer {}", self.catch_token));
        req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);
        req = req.header(CONTENT_LENGTH, size);
        req = req.header(CONTENT_TYPE, "application/octet-stream");
        req = req.header(
            CONTENT_RANGE,
            BytesContentRange::default()
                .with_range(offset, end)
                .to_header(),
        );
        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("UploadCache"))
            .body(body)
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn ghac_finalize_upload(
        &self,
        ctx: &OperationContext,
        path: &str,
        url: &str,
        size: u64,
    ) -> Result<()> {
        let p = build_absolute_path(&self.root, path);

        match self.service_version {
            GhacVersion::V1 => {
                let bs = serde_json::to_vec(&GhacCommitRequest { size })
                    .map_err(new_json_serialize_error)?;

                let req = Request::post(url)
                    .header(AUTHORIZATION, format!("Bearer {}", self.catch_token))
                    .header(ACCEPT, CACHE_HEADER_ACCEPT)
                    .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
                    .header(CONTENT_LENGTH, bs.len())
                    .extension(Operation::Write)
                    .extension(ServiceOperation("CommitCache"))
                    .body(Buffer::from(bs))
                    .map_err(new_request_build_error)?;
                let resp = ctx.http_transport().send(req).await?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(parse_error(
                        ErrorContext::new(ServiceOperation("CommitCache")),
                        resp,
                    ))
                }
            }
            GhacVersion::V2 => {
                let url = build_cache_url(
                    &self.cache_url,
                    format!("{CACHE_URL_BASE_V2}/FinalizeCacheEntryUpload"),
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
                    .extension(Operation::Write)
                    .extension(ServiceOperation("FinalizeCacheEntryUpload"))
                    .body(body)
                    .map_err(new_request_build_error)?;
                let resp = ctx.http_transport().send(req).await?;
                if resp.status() != StatusCode::OK {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("FinalizeCacheEntryUpload")),
                        resp,
                    ));
                }
                let (parts, slc) = resp.into_parts();
                let finalize_resp = ghac_types::FinalizeCacheEntryUploadResponse::decode(slc)
                    .map_err(new_prost_decode_error)?;
                if !finalize_resp.ok {
                    return Err(with_error_response_context(
                        Error::new(
                            ErrorKind::Unexpected,
                            "finalize cache entry upload returns non-ok",
                        )
                        .with_context("service_operation", "FinalizeCacheEntryUpload"),
                        parts,
                    ));
                }
                Ok(())
            }
        }
    }
}

fn build_cache_url(base: &str, path: impl AsRef<str>) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.as_ref().trim_start_matches('/')
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_cache_url_handles_trailing_slash() {
        assert_eq!(
            build_cache_url(
                "https://artifactcache.actions.githubusercontent.com/123",
                "_apis/artifactcache/cache?keys=k&version=v"
            ),
            "https://artifactcache.actions.githubusercontent.com/123/_apis/artifactcache/cache?keys=k&version=v"
        );
        assert_eq!(
            build_cache_url(
                "https://artifactcache.actions.githubusercontent.com/123/",
                "_apis/artifactcache/cache?keys=k&version=v"
            ),
            "https://artifactcache.actions.githubusercontent.com/123/_apis/artifactcache/cache?keys=k&version=v"
        );
    }

    #[test]
    fn test_parse_error_range_not_satisfiable() {
        let response = Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .body(Buffer::new())
            .unwrap();
        let err = parse_error(ErrorContext::new(ServiceOperation("Download")), response);
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
        assert!(!err.is_temporary());
    }

    #[test]
    fn test_build_cache_url_handles_leading_slash() {
        assert_eq!(
            build_cache_url(
                "https://results.actions.githubusercontent.com",
                "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry"
            ),
            "https://results.actions.githubusercontent.com/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry"
        );
    }

    struct FixedResponseTransport {
        status: StatusCode,
        body: Bytes,
        requests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl HttpTransport for FixedResponseTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.lock().unwrap().push(req.uri().to_string());
            let buf = Buffer::from(self.body.clone());
            let len = buf.len() as u64;
            Response::builder()
                .status(self.status)
                .body(HttpBody::new(futures::stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    fn test_core(service_version: GhacVersion) -> GhacCore {
        GhacCore {
            info: ServiceInfo::new("ghac", "/", "test-version"),
            capability: Capability {
                stat: true,
                read: true,
                write: true,
                write_can_multi: true,
                shared: true,
                ..Default::default()
            },
            root: "/".to_string(),
            cache_url: "https://results.actions.githubusercontent.com".to_string(),
            catch_token: "test-token".to_string(),
            version: "test-version".to_string(),
            service_version,
        }
    }

    #[tokio::test]
    async fn test_ghac_v1_write_rejects_empty_chunk_and_range_overflow() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let transport = FixedResponseTransport {
                status: StatusCode::NO_CONTENT,
                body: Bytes::new(),
                requests: requests.clone(),
            };
            let ctx = OperationContext::from_parts(
                HttpTransporter::new(transport),
                Executor::default(),
            );
            let core = test_core(GhacVersion::V1);

            for offset in [0, 10] {
                let err = core
                    .ghac_v1_write(
                        &ctx,
                        "https://artifactcache.actions.githubusercontent.com/1/_apis/artifactcache/caches/1",
                        0,
                        offset,
                        Buffer::new(),
                    )
                    .await
                    .unwrap_err();
                assert_eq!(err.kind(), ErrorKind::Unexpected);
            }

            let err = core
                .ghac_v1_write(
                    &ctx,
                    "https://artifactcache.actions.githubusercontent.com/1/_apis/artifactcache/caches/1",
                    2,
                    u64::MAX,
                    Buffer::from("ab"),
                )
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(requests.lock().unwrap().is_empty());
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_ghac_v2_finalize_upload_checks_ok_flag() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let core = test_core(GhacVersion::V2);

            let non_ok_body = Bytes::from(
                ghac_types::FinalizeCacheEntryUploadResponse {
                    ok: false,
                    entry_id: 0,
                }
                .encode_to_vec(),
            );
            let non_ok_requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let non_ok_ctx = OperationContext::from_parts(
                HttpTransporter::new(FixedResponseTransport {
                    status: StatusCode::OK,
                    body: non_ok_body,
                    requests: non_ok_requests.clone(),
                }),
                Executor::default(),
            );
            let err = core
                .ghac_finalize_upload(&non_ok_ctx, "key", "", 16)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.message().contains("non-ok"));
            assert_eq!(non_ok_requests.lock().unwrap().len(), 1);

            let ok_body = Bytes::from(
                ghac_types::FinalizeCacheEntryUploadResponse {
                    ok: true,
                    entry_id: 42,
                }
                .encode_to_vec(),
            );
            let ok_requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let ok_ctx = OperationContext::from_parts(
                HttpTransporter::new(FixedResponseTransport {
                    status: StatusCode::OK,
                    body: ok_body,
                    requests: ok_requests.clone(),
                }),
                Executor::default(),
            );
            core.ghac_finalize_upload(&ok_ctx, "key", "", 16)
                .await
                .unwrap();
            assert_eq!(ok_requests.lock().unwrap().len(), 1);

            let invalid_requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let invalid_ctx = OperationContext::from_parts(
                HttpTransporter::new(FixedResponseTransport {
                    status: StatusCode::OK,
                    body: Bytes::from_static(&[0xff, 0xff, 0xff]),
                    requests: invalid_requests,
                }),
                Executor::default(),
            );
            let err = core
                .ghac_finalize_upload(&invalid_ctx, "key", "", 16)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.message().contains("deserialize protobuf"));
        })
        .await
        .expect("test timed out");
    }
}

/// Context needed to classify an error from this service.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ErrorContext {
    service_operation: ServiceOperation,
}

impl ErrorContext {
    pub(crate) const fn new(service_operation: ServiceOperation) -> Self {
        Self { service_operation }
    }
}

/// Parse an error response using its service request context.
pub(crate) fn parse_error(ctx: ErrorContext, resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();

    let (kind, retryable) = match parts.status {
        StatusCode::NOT_FOUND | StatusCode::NO_CONTENT => (ErrorKind::NotFound, false),
        StatusCode::CONFLICT => (ErrorKind::AlreadyExists, false),
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::RANGE_NOT_SATISFIABLE => (ErrorKind::RangeNotSatisfied, false),
        StatusCode::TOO_MANY_REQUESTS => (ErrorKind::RateLimited, true),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let bs = body.to_bytes();
    let message = String::from_utf8_lossy(&bs);

    let mut err = Error::new(kind, message);

    err = err.with_context("service_operation", ctx.service_operation.0);
    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}
