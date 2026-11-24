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

use std::collections::HashMap;
use std::fmt::Debug;

use http::Request;
use http::Response;
use http::header;
use opendal_core::raw::*;
use opendal_core::*;
use serde::Deserialize;

pub struct LakefsCore {
    pub info: ServiceInfo,
    pub capability: Capability,
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
            .field("password", &"<redacted>")
            .field("root", &self.root)
            .field("repository", &self.repository)
            .field("branch", &self.branch)
            .finish_non_exhaustive()
    }
}

/// Percent-encode a single URL path segment (encoding `/` as `%2F`).
fn percent_encode_segment(segment: &str) -> String {
    percent_encode_path(segment).replace('/', "%2F")
}

impl LakefsCore {
    pub async fn get_object_metadata(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/refs/{}/objects/stat?path={}",
            self.endpoint,
            percent_encode_segment(&self.repository),
            percent_encode_segment(&self.branch),
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        // Inject operation to the request.
        let req = req
            .extension(Operation::Stat)
            .extension(ServiceOperation("StatObject"));
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn get_object_content(
        &self,
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
        _args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let p = build_absolute_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/refs/{}/objects?path={}",
            self.endpoint,
            percent_encode_segment(&self.repository),
            percent_encode_segment(&self.branch),
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }
        // Inject operation to the request.
        let req = req
            .extension(Operation::Read)
            .extension(ServiceOperation("GetObject"));
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        ctx.http_transport().fetch(req).await
    }

    pub async fn list_objects(
        &self,
        ctx: &OperationContext,
        path: &str,
        delimiter: &str,
        amount: &Option<usize>,
        after: Option<String>,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path);

        let mut url = QueryPairsWriter::new(&format!(
            "{}/api/v1/repositories/{}/refs/{}/objects/ls",
            self.endpoint,
            percent_encode_segment(&self.repository),
            percent_encode_segment(&self.branch),
        ));

        if !p.is_empty() {
            url = url.push("prefix", &percent_encode_path(&p));
        }

        if !delimiter.is_empty() {
            url = url.push("delimiter", &percent_encode_path(delimiter));
        }

        if let Some(amount) = amount {
            url = url.push("amount", &amount.to_string());
        }

        if let Some(after) = after
            && !after.is_empty()
        {
            url = url.push("after", &percent_encode_path(&after));
        }

        let mut req = Request::get(url.finish());

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        // Inject operation to the request.
        let req = req
            .extension(Operation::List)
            .extension(ServiceOperation("ListObjects"));
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn upload_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        _args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/branches/{}/objects?path={}",
            self.endpoint,
            percent_encode_segment(&self.repository),
            percent_encode_segment(&self.branch),
            percent_encode_path(&p)
        );

        let mut req = Request::post(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        // Inject operation to the request.
        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("UploadObject"));
        let req = req.body(body).map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn delete_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        _args: &OpDelete,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/branches/{}/objects?path={}",
            self.endpoint,
            percent_encode_segment(&self.repository),
            percent_encode_segment(&self.branch),
            percent_encode_path(&p)
        );

        let mut req = Request::delete(&url);

        let auth_header_content = format_authorization_by_basic(&self.username, &self.password)?;
        req = req.header(header::AUTHORIZATION, auth_header_content);
        // Inject operation to the request.
        let req = req
            .extension(Operation::Delete)
            .extension(ServiceOperation("DeleteObject"));
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn copy_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        dest: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();
        let d = build_absolute_path(&self.root, dest)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/v1/repositories/{}/branches/{}/objects/copy?dest_path={}",
            self.endpoint,
            percent_encode_segment(&self.repository),
            percent_encode_segment(&self.branch),
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
            .extension(Operation::Copy)
            .extension(ServiceOperation("CopyObject"))
            .body(serde_json::to_vec(&map).unwrap().into())
            .map_err(new_request_build_error)?;
        ctx.http_transport().send(req).await
    }

    /// Parse LakefsStatus into Metadata
    pub fn parse_lakefs_status_into_metadata(status: &LakefsStatus) -> Result<Metadata> {
        // Determine entry mode based on path_type
        // "common_prefix" indicates a directory in list operations
        let mode = if status.path_type == "common_prefix" {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        };

        let mut meta = if mode == EntryMode::FILE {
            MetadataBuilder::file(status.size_bytes.ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "lakefs response does not contain file size",
                )
            })?)
        } else {
            MetadataBuilder::dir()
        };

        // Set checksum as etag
        if !status.checksum.is_empty() {
            meta.etag(&status.checksum);
        }

        // Set content type
        if let Some(ref content_type) = status.content_type {
            meta.content_type(content_type);
        }

        // Set last modified time
        if let Ok(timestamp) = Timestamp::from_second(status.mtime) {
            meta.last_modified(timestamp);
        }

        Ok(meta.build())
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

use http::StatusCode;

/// LakefsError is the error returned by Lakefs File System.
#[derive(Default, Deserialize)]
struct LakefsError {
    error: String,
}

impl Debug for LakefsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakefsError")
            .field("message", &self.error.replace('\n', " "))
            .finish()
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
    let bs = body.to_bytes();

    let (kind, retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::PRECONDITION_FAILED => (ErrorKind::ConditionNotMatch, false),
        StatusCode::RANGE_NOT_SATISFIABLE => (ErrorKind::RangeNotSatisfied, false),
        StatusCode::TOO_MANY_REQUESTS => (ErrorKind::RateLimited, true),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let message = match serde_json::from_slice::<LakefsError>(&bs) {
        Ok(hf_error) => format!("{:?}", hf_error.error),
        Err(_) => String::from_utf8_lossy(&bs).into_owned(),
    };

    let mut err = Error::new(kind, message);

    err = err.with_context("service_operation", ctx.service_operation.0);
    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::stream;

    use super::*;
    use crate::backend::LakefsBuilder;

    #[derive(Debug, Clone)]
    struct RecordedRequest {
        uri: String,
        operation: Option<Operation>,
        service_operation: Option<&'static str>,
    }

    #[derive(Clone, Default)]
    struct RecordingTransport {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl HttpTransport for RecordingTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.lock().unwrap().push(RecordedRequest {
                uri: req.uri().to_string(),
                operation: req.extensions().get::<Operation>().copied(),
                service_operation: req.extensions().get::<ServiceOperation>().map(|op| op.0),
            });
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter([]), Some(0)))
                .map_err(new_request_build_error)
        }
    }

    fn build_core(endpoint: &str, repository: &str, branch: &str, root: &str) -> Arc<LakefsCore> {
        let service = LakefsBuilder::default()
            .endpoint(endpoint)
            .repository(repository)
            .branch(branch)
            .root(root)
            .username("user")
            .password("pass")
            .build()
            .expect("builder must succeed");
        let any: Box<dyn std::any::Any> = Box::new(service);
        let backend = any
            .downcast_ref::<crate::backend::LakefsBackend>()
            .expect("build must return LakefsBackend");
        backend.core.clone()
    }

    #[test]
    fn test_parse_error() -> Result<()> {
        let resp = r#"
            {
                "error": "Invalid username or password."
            }
            "#;
        let decoded_response = serde_json::from_slice::<LakefsError>(resp.as_bytes())
            .map_err(new_json_deserialize_error)?;

        assert_eq!(decoded_response.error, "Invalid username or password.");

        Ok(())
    }

    #[test]
    fn test_parse_error_range_not_satisfiable_and_rate_limited() -> Result<()> {
        let range_resp = Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .body(Buffer::from(r#"{"error":"invalid range"}"#))
            .map_err(new_request_build_error)?;
        let range_err = parse_error(ErrorContext::new(ServiceOperation("GetObject")), range_resp);
        assert_eq!(range_err.kind(), ErrorKind::RangeNotSatisfied);
        assert!(!range_err.is_temporary());

        let rate_resp = Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(Buffer::from(r#"{"error":"too many requests"}"#))
            .map_err(new_request_build_error)?;
        let rate_err = parse_error(ErrorContext::new(ServiceOperation("StatObject")), rate_resp);
        assert_eq!(rate_err.kind(), ErrorKind::RateLimited);
        assert!(rate_err.is_temporary());

        Ok(())
    }

    #[tokio::test]
    async fn test_request_encoding_endpoint_trim_and_operation_extensions() -> Result<()> {
        let core = build_core(
            "http://127.0.0.1:8000///",
            "org/my repo+1",
            "feature/a+b?c#d",
            "/data",
        );
        let transport = RecordingTransport::default();
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(transport.clone()));

        tokio::time::timeout(Duration::from_secs(5), async {
            let _ = core.get_object_metadata(&ctx, "a/b.txt").await?;
            let _ = core
                .get_object_content(
                    &ctx,
                    "a/b.txt",
                    BytesRange::new(0, None),
                    &OpRead::default(),
                )
                .await?;
            let _ = core
                .list_objects(&ctx, "a/", "/", &Some(10), Some("a/1.txt".to_string()))
                .await?;
            let _ = core
                .upload_object(&ctx, "a/b.txt", &OpWrite::default(), Buffer::new())
                .await?;
            let _ = core
                .delete_object(&ctx, "a/b.txt", &OpDelete::default())
                .await?;
            let _ = core.copy_object(&ctx, "a/b.txt", "a/c.txt").await?;
            Result::<()>::Ok(())
        })
        .await
        .expect("requests must not hang")?;

        let recorded = transport.requests.lock().unwrap();
        assert_eq!(recorded.len(), 6);

        let expected = [
            (
                "http://127.0.0.1:8000/api/v1/repositories/org%2Fmy%20repo%2B1/refs/feature%2Fa%2Bb%3Fc%23d/objects/stat?path=data/a/b.txt",
                Operation::Stat,
                "StatObject",
            ),
            (
                "http://127.0.0.1:8000/api/v1/repositories/org%2Fmy%20repo%2B1/refs/feature%2Fa%2Bb%3Fc%23d/objects?path=data/a/b.txt",
                Operation::Read,
                "GetObject",
            ),
            (
                "http://127.0.0.1:8000/api/v1/repositories/org%2Fmy%20repo%2B1/refs/feature%2Fa%2Bb%3Fc%23d/objects/ls?prefix=data/a/&delimiter=/&amount=10&after=a/1.txt",
                Operation::List,
                "ListObjects",
            ),
            (
                "http://127.0.0.1:8000/api/v1/repositories/org%2Fmy%20repo%2B1/branches/feature%2Fa%2Bb%3Fc%23d/objects?path=data/a/b.txt",
                Operation::Write,
                "UploadObject",
            ),
            (
                "http://127.0.0.1:8000/api/v1/repositories/org%2Fmy%20repo%2B1/branches/feature%2Fa%2Bb%3Fc%23d/objects?path=data/a/b.txt",
                Operation::Delete,
                "DeleteObject",
            ),
            (
                "http://127.0.0.1:8000/api/v1/repositories/org%2Fmy%20repo%2B1/branches/feature%2Fa%2Bb%3Fc%23d/objects/copy?dest_path=data/a/c.txt",
                Operation::Copy,
                "CopyObject",
            ),
        ];

        for (actual, (uri, op, svc_op)) in recorded.iter().zip(expected) {
            assert_eq!(actual.uri, uri);
            assert_eq!(actual.operation, Some(op));
            assert_eq!(actual.service_operation, Some(svc_op));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_list_objects_query_string_formatting() -> Result<()> {
        let core = build_core("http://127.0.0.1:8000/", "repo", "main", "/");
        let transport = RecordingTransport::default();
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(transport.clone()));

        tokio::time::timeout(Duration::from_secs(5), async {
            let _ = core.list_objects(&ctx, "", "", &None, None).await?;
            let _ = core
                .list_objects(&ctx, "", "/", &None, Some(String::new()))
                .await?;
            let _ = core
                .list_objects(&ctx, "a b/", "/", &Some(25), Some("a b/c+d".to_string()))
                .await?;
            Result::<()>::Ok(())
        })
        .await
        .expect("requests must not hang")?;

        let recorded = transport.requests.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        assert_eq!(
            recorded[0].uri,
            "http://127.0.0.1:8000/api/v1/repositories/repo/refs/main/objects/ls"
        );
        assert_eq!(
            recorded[1].uri,
            "http://127.0.0.1:8000/api/v1/repositories/repo/refs/main/objects/ls?delimiter=/"
        );
        assert_eq!(
            recorded[2].uri,
            "http://127.0.0.1:8000/api/v1/repositories/repo/refs/main/objects/ls?prefix=a%20b/&delimiter=/&amount=25&after=a%20b/c%2Bd"
        );
        Ok(())
    }
}
