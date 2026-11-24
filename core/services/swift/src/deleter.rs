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
use std::sync::Arc;

use http::StatusCode;

use super::core::parse_error;
use super::core::*;
use opendal_core::raw::*;
use opendal_core::*;

pub struct SwiftDeleter {
    core: Arc<SwiftCore>,
    ctx: OperationContext,
}

impl SwiftDeleter {
    pub fn new(core: Arc<SwiftCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::BatchDelete for SwiftDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let resp = self.core.swift_delete(&self.ctx, &path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("DeleteObject")),
                resp,
            )),
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<oio::BatchDeleteResult> {
        let resp = self.core.swift_bulk_delete(&self.ctx, &batch).await?;

        let status = resp.status();
        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("BulkDelete")),
                resp,
            ));
        }

        let bs = resp.into_body().to_bytes();
        let result: BulkDeleteResponse =
            serde_json::from_slice(&bs).map_err(new_json_deserialize_error)?;

        if result.errors.is_empty()
            && !batch.is_empty()
            && let Some(resp_status) = parse_bulk_delete_status(&result.response_status)
            && !resp_status.is_success()
        {
            let detail = match result.response_body.as_deref() {
                Some(body) if !body.is_empty() => format!("{} ({body})", result.response_status),
                _ => result.response_status.clone(),
            };
            return Err(bulk_delete_error(&detail));
        }

        let mut errors_by_path: HashMap<String, &str> = HashMap::with_capacity(result.errors.len());
        for entry in &result.errors {
            if let Some(raw_path) = entry.first() {
                let normalized = percent_decode_path(raw_path)
                    .trim_start_matches('/')
                    .to_string();
                let status_str = entry.get(1).map(String::as_str).unwrap_or_default();
                errors_by_path.insert(normalized, status_str);
            }
        }

        let container = self.core.container.trim_start_matches('/');
        let mut batched_result = oio::BatchDeleteResult {
            // `result.errors.len()` is server-controlled and may exceed `batch.len()`; use
            // `saturating_sub` (mirroring services/tos) to avoid an unsigned underflow that
            // wraps to a huge `with_capacity` (release: abort).
            succeeded: Vec::with_capacity(batch.len().saturating_sub(result.errors.len())),
            failed: Vec::with_capacity(result.errors.len()),
        };

        for (path, op) in batch {
            // Swift bulk-delete error paths include the container prefix and may have a
            // leading slash (e.g. "/container/path/to/file"), so normalize both sides.
            let abs = build_absolute_path(&self.core.root, &path);
            let full_path = format!("{container}/{abs}");

            if let Some(&status_str) = errors_by_path.get(&full_path) {
                if parse_bulk_delete_status(status_str) == Some(StatusCode::NOT_FOUND) {
                    batched_result.succeeded.push((path, op));
                } else {
                    batched_result
                        .failed
                        .push((path, op, bulk_delete_error(status_str)));
                }
            } else {
                // Either deleted successfully or not found (both are success for us).
                batched_result.succeeded.push((path, op));
            }
        }

        Ok(batched_result)
    }
}

fn parse_bulk_delete_status(status_str: &str) -> Option<StatusCode> {
    status_str
        .split_whitespace()
        .next()
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
}

fn bulk_delete_error(status_str: &str) -> Error {
    let status = parse_bulk_delete_status(status_str);
    let (kind, retryable) = match status {
        Some(StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) => {
            (ErrorKind::PermissionDenied, false)
        }
        Some(StatusCode::TOO_MANY_REQUESTS) => (ErrorKind::RateLimited, true),
        Some(StatusCode::NOT_MODIFIED | StatusCode::PRECONDITION_FAILED) => {
            (ErrorKind::ConditionNotMatch, false)
        }
        Some(code) if code.is_server_error() => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let mut err = Error::new(kind, format!("bulk delete error: {status_str}"));
    if retryable {
        err = err.set_temporary();
    }
    err
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use http::Response;

    use super::*;
    use opendal_core::raw::oio::BatchDelete;

    struct StaticTransport {
        status: StatusCode,
        body: String,
    }

    impl HttpTransport for StaticTransport {
        async fn fetch(&self, _req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Buffer::from(self.body.clone());
            let size = body.len() as u64;
            Response::builder()
                .status(self.status)
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(size)))
                .map_err(new_request_build_error)
        }
    }

    fn deleter_with_body(root: &str, body: &str) -> SwiftDeleter {
        let core = Arc::new(SwiftCore {
            info: ServiceInfo::new(crate::SWIFT_SCHEME, root, "container"),
            capability: Capability {
                delete: true,
                delete_max_size: Some(10000),
                ..Default::default()
            },
            root: root.to_string(),
            endpoint: "http://127.0.0.1:8080/v1/AUTH_test".to_string(),
            container: "container".to_string(),
            token: "test-token".to_string(),
            temp_url_key: String::new(),
            temp_url_hash_algorithm: TempUrlHashAlgorithm::Sha256,
        });
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
            StaticTransport {
                status: StatusCode::OK,
                body: body.to_string(),
            },
        ));
        SwiftDeleter::new(core, ctx)
    }

    #[tokio::test]
    async fn delete_batch_matches_leading_slash_and_encoded_error_paths() {
        let body = r#"{
            "Number Deleted": 1,
            "Number Not Found": 0,
            "Response Status": "400 Bad Request",
            "Response Body": "",
            "Errors": [
                ["/container/prefix/path/to/forbidden.txt", "403 Forbidden"],
                ["container/prefix/unauth.txt", "401 Unauthorized"],
                ["/container/prefix/dir/file%20with%20space%2520%2Bplus.txt", "503 Service Unavailable"],
                ["/container/prefix/rate-limited.txt", "429 Too Many Requests"],
                ["/container/prefix/missing.txt", "404 Not Found"],
                ["/container/prefix/conflict.txt", "409 Conflict"]
            ]
        }"#;
        let deleter = deleter_with_body("/prefix/", body);

        let result = deleter
            .delete_batch(vec![
                ("ok.txt".to_string(), OpDelete::default()),
                ("path/to/forbidden.txt".to_string(), OpDelete::default()),
                ("unauth.txt".to_string(), OpDelete::default()),
                (
                    "dir/file with space%20+plus.txt".to_string(),
                    OpDelete::default(),
                ),
                ("rate-limited.txt".to_string(), OpDelete::default()),
                ("missing.txt".to_string(), OpDelete::default()),
                ("conflict.txt".to_string(), OpDelete::default()),
            ])
            .await
            .unwrap();

        let succeeded: Vec<&str> = result
            .succeeded
            .iter()
            .map(|(path, _)| path.as_str())
            .collect();
        assert_eq!(succeeded, vec!["ok.txt", "missing.txt"]);

        assert_eq!(result.failed.len(), 5);

        assert_eq!(result.failed[0].0, "path/to/forbidden.txt");
        assert_eq!(result.failed[0].2.kind(), ErrorKind::PermissionDenied);
        assert!(!result.failed[0].2.is_temporary());

        assert_eq!(result.failed[1].0, "unauth.txt");
        assert_eq!(result.failed[1].2.kind(), ErrorKind::PermissionDenied);
        assert!(!result.failed[1].2.is_temporary());

        assert_eq!(result.failed[2].0, "dir/file with space%20+plus.txt");
        assert_eq!(result.failed[2].2.kind(), ErrorKind::Unexpected);
        assert!(result.failed[2].2.is_temporary());

        assert_eq!(result.failed[3].0, "rate-limited.txt");
        assert_eq!(result.failed[3].2.kind(), ErrorKind::RateLimited);
        assert!(result.failed[3].2.is_temporary());

        assert_eq!(result.failed[4].0, "conflict.txt");
        assert_eq!(result.failed[4].2.kind(), ErrorKind::Unexpected);
        assert!(!result.failed[4].2.is_temporary());
    }

    #[tokio::test]
    async fn delete_batch_errors_on_non_success_response_status_with_empty_errors() {
        let body = r#"{
            "Number Deleted": 0,
            "Number Not Found": 0,
            "Response Status": "502 Bad Gateway",
            "Response Body": "Max delete failures exceeded",
            "Errors": []
        }"#;
        let deleter = deleter_with_body("/", body);

        let err = match deleter
            .delete_batch(vec![("file.txt".to_string(), OpDelete::default())])
            .await
        {
            Ok(_) => panic!("expected bulk delete error for non-success Response Status"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());
    }
}
