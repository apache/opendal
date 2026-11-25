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

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use serde::Deserialize;
use serde_json::json;

use opendal_core::raw::*;
use opendal_core::*;

pub struct DbfsCore {
    pub root: String,
    pub endpoint: String,
    pub token: String,
}

impl Debug for DbfsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbfsCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl DbfsCore {
    fn dbfs_path(&self, path: &str) -> String {
        let p = build_rooted_absolute_path(&self.root, path);
        let trimmed = p.trim_end_matches('/');
        if trimmed.is_empty() {
            "/".to_string()
        } else {
            trimmed.to_string()
        }
    }

    pub async fn dbfs_create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let url = format!("{}/api/2.0/dbfs/mkdirs", self.endpoint);
        let mut req = Request::post(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let p = self.dbfs_path(path);

        let req_body = &json!({
            "path": p,
        });
        let body = Buffer::from(Bytes::from(req_body.to_string()));

        let req = req
            .extension(Operation::CreateDir)
            .extension(ServiceOperation("Mkdirs"))
            .body(body)
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn dbfs_delete(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let url = format!("{}/api/2.0/dbfs/delete", self.endpoint);
        let mut req = Request::post(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let p = self.dbfs_path(path);

        let request_body = &json!({
            "path": p,
            // TODO: support recursive toggle, should we add a new field in OpDelete?
            "recursive": true,
        });

        let body = Buffer::from(Bytes::from(request_body.to_string()));

        let req = req
            .extension(Operation::Delete)
            .extension(ServiceOperation("Delete"))
            .body(body)
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn dbfs_rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
    ) -> Result<Response<Buffer>> {
        let source = build_rooted_absolute_path(&self.root, from);
        let target = build_rooted_absolute_path(&self.root, to);

        let url = format!("{}/api/2.0/dbfs/move", self.endpoint);
        let mut req = Request::post(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req_body = &json!({
            "source_path": source,
            "destination_path": target,
        });

        let body = Buffer::from(Bytes::from(req_body.to_string()));

        let req = req
            .extension(Operation::Rename)
            .extension(ServiceOperation("Move"))
            .body(body)
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn dbfs_list(&self, ctx: &OperationContext, path: &str) -> Result<Response<Buffer>> {
        let p = self.dbfs_path(path);

        let url = format!(
            "{}/api/2.0/dbfs/list?path={}",
            self.endpoint,
            percent_encode_path(&p)
        );
        let mut req = Request::get(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .extension(Operation::List)
            .extension(ServiceOperation("List"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub fn dbfs_create_file_request(&self, path: &str, body: Bytes) -> Result<Request<Buffer>> {
        let url = format!("{}/api/2.0/dbfs/put", self.endpoint);

        let contents = BASE64_STANDARD.encode(body);
        let mut req = Request::post(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req_body = &json!({
            "path": build_rooted_absolute_path(&self.root, path),
            "contents": contents,
            "overwrite": true,
        });

        let body = Buffer::from(Bytes::from(req_body.to_string()));

        req.extension(Operation::Write)
            .extension(ServiceOperation("Put"))
            .body(body)
            .map_err(new_request_build_error)
    }

    pub async fn dbfs_get_status(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = self.dbfs_path(path);

        let url = format!(
            "{}/api/2.0/dbfs/get-status?path={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .extension(Operation::Stat)
            .extension(ServiceOperation("GetStatus"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn dbfs_ensure_parent_path(&self, ctx: &OperationContext, path: &str) -> Result<()> {
        let parent = get_parent(path);
        let resp = self.dbfs_get_status(ctx, parent).await?;

        match resp.status() {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => {
                let resp = self.dbfs_create_dir(ctx, parent).await?;
                match resp.status() {
                    StatusCode::CREATED | StatusCode::OK => Ok(()),
                    _ => Err(parse_error(
                        ErrorContext::new(ServiceOperation("Mkdirs")),
                        resp,
                    )),
                }
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("GetStatus")),
                resp,
            )),
        }
    }
}

/// DbfsError is the error returned by DBFS service.
#[derive(Default, Deserialize)]
struct DbfsError {
    error_code: String,
    message: String,
}

impl Debug for DbfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbfsError")
            .field("error_code", &self.error_code)
            // replace `\n` to ` ` for better reading.
            .field("message", &self.message.replace('\n', " "))
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
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let message = match serde_json::from_slice::<DbfsError>(&bs) {
        Ok(dbfs_error) => format!("{:?}", dbfs_error.message),
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
pub(crate) mod test_support {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;

    use bytes::Bytes;
    use futures::stream;
    use http::Method;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::Buffer;
    use opendal_core::Error;
    use opendal_core::ErrorKind;
    use opendal_core::HttpBody;
    use opendal_core::HttpTransport;
    use opendal_core::HttpTransporter;
    use opendal_core::OperationContext;
    use opendal_core::Result;
    use opendal_core::raw::new_request_build_error;

    #[derive(Debug, Clone)]
    pub(crate) struct RecordedRequest {
        pub(crate) method: Method,
        pub(crate) uri: String,
        pub(crate) body: Bytes,
    }

    #[derive(Clone)]
    pub(crate) struct RecordingTransport {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        responses: Arc<Mutex<VecDeque<(StatusCode, String)>>>,
    }

    impl RecordingTransport {
        pub(crate) fn new(
            responses: impl IntoIterator<Item = (StatusCode, impl Into<String>)>,
        ) -> Self {
            let queue = responses
                .into_iter()
                .map(|(status, body)| (status, body.into()))
                .collect();
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(queue)),
            }
        }

        pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().unwrap().clone()
        }

        pub(crate) fn context(&self) -> OperationContext {
            OperationContext::default().with_http_transport(HttpTransporter::new(self.clone()))
        }
    }

    impl HttpTransport for RecordingTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let (parts, body) = req.into_parts();
            self.requests.lock().unwrap().push(RecordedRequest {
                method: parts.method,
                uri: parts.uri.to_string(),
                body: body.to_bytes(),
            });

            let (status, resp_body) =
                self.responses.lock().unwrap().pop_front().ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "unexpected extra HTTP request in mock transport",
                    )
                })?;

            let payload = Buffer::from(resp_body);
            let len = payload.len() as u64;
            Response::builder()
                .status(status)
                .body(HttpBody::new(stream::iter([Ok(payload)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use http::Method;
    use serde_json::Value;

    use super::test_support::RecordingTransport;
    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn root_path_operations_preserve_leading_slash() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let transport = RecordingTransport::new([
                (StatusCode::OK, "{}"),
                (StatusCode::OK, "{}"),
                (StatusCode::OK, "{}"),
                (StatusCode::OK, "{}"),
            ]);
            let ctx = transport.context();
            let core = DbfsCore {
                root: "/".to_string(),
                endpoint: "https://example.databricks.com".to_string(),
                token: "test-token".to_string(),
            };

            core.dbfs_create_dir(&ctx, "/").await.unwrap();
            core.dbfs_delete(&ctx, "/").await.unwrap();
            core.dbfs_list(&ctx, "/").await.unwrap();
            core.dbfs_get_status(&ctx, "/").await.unwrap();

            let reqs = transport.requests();
            assert_eq!(reqs.len(), 4);

            assert_eq!(reqs[0].method, Method::POST);
            assert_eq!(
                reqs[0].uri,
                "https://example.databricks.com/api/2.0/dbfs/mkdirs"
            );
            let mkdir_body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
            assert_eq!(mkdir_body["path"], "/");

            assert_eq!(reqs[1].method, Method::POST);
            assert_eq!(
                reqs[1].uri,
                "https://example.databricks.com/api/2.0/dbfs/delete"
            );
            let delete_body: Value = serde_json::from_slice(&reqs[1].body).unwrap();
            assert_eq!(delete_body["path"], "/");

            assert_eq!(reqs[2].method, Method::GET);
            assert_eq!(
                reqs[2].uri,
                "https://example.databricks.com/api/2.0/dbfs/list?path=/"
            );

            assert_eq!(reqs[3].method, Method::GET);
            assert_eq!(
                reqs[3].uri,
                "https://example.databricks.com/api/2.0/dbfs/get-status?path=/"
            );
        })
        .await
        .expect("test timed out");
    }

    #[test]
    fn dbfs_path_trims_non_root_trailing_slashes() {
        let root_core = DbfsCore {
            root: "/".to_string(),
            endpoint: "https://example.databricks.com".to_string(),
            token: "test-token".to_string(),
        };
        assert_eq!(root_core.dbfs_path("/"), "/");
        assert_eq!(root_core.dbfs_path("dir/"), "/dir");
        assert_eq!(root_core.dbfs_path("dir/file.txt"), "/dir/file.txt");

        let sub_core = DbfsCore {
            root: "/workspace/".to_string(),
            endpoint: "https://example.databricks.com".to_string(),
            token: "test-token".to_string(),
        };
        assert_eq!(sub_core.dbfs_path("/"), "/workspace");
        assert_eq!(sub_core.dbfs_path("dir/"), "/workspace/dir");
        assert_eq!(sub_core.dbfs_path("file.txt"), "/workspace/file.txt");
    }
}
