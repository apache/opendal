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

use asyncband::once::OnceCell;
use bytes::Buf;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use serde::Deserialize;

use opendal_core::raw::*;
use opendal_core::*;

pub struct WebhdfsCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub root: String,
    pub endpoint: String,
    pub user_name: Option<String>,
    pub delegation: Option<String>,
    pub root_checker: OnceCell<()>,

    pub atomic_write_dir: Option<String>,
    pub disable_list_batch: bool,
}

impl Debug for WebhdfsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhdfsCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl WebhdfsCore {
    fn push_auth_query(&self, mut query: QueryPairsWriter) -> QueryPairsWriter {
        if let Some(user) = &self.user_name {
            query = query.push("user.name", &percent_encode_query_value(user));
        }
        if let Some(delegation) = &self.delegation {
            query = query.push("delegation", &percent_encode_query_value(delegation));
        }
        query
    }

    pub async fn webhdfs_create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path);
        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let url = self
            .push_auth_query(
                QueryPairsWriter::new(&base)
                    .push("op", "MKDIRS")
                    .push("overwrite", "true")
                    .push("noredirect", "true"),
            )
            .finish();

        let req = Request::put(&url);

        let req = req
            .extension(Operation::CreateDir)
            .extension(ServiceOperation("Mkdirs"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    /// create object
    pub async fn webhdfs_create_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path);
        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let url = self
            .push_auth_query(
                QueryPairsWriter::new(&base)
                    .push("op", "CREATE")
                    .push("overwrite", "true")
                    .push("noredirect", "true"),
            )
            .finish();

        let req = Request::put(&url);

        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("Create"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = ctx.http_transport().send(req).await?;

        let status = resp.status();

        if status != StatusCode::CREATED && status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("Create")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let resp: LocationResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let mut req = Request::put(&resp.location);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size);
        };

        if let Some(content_type) = args.content_type() {
            req = req.header(CONTENT_TYPE, content_type);
        };
        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("Create"))
            .body(body)
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn webhdfs_rename_object(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
    ) -> Result<Response<Buffer>> {
        let from = build_absolute_path(&self.root, from);
        let to = build_rooted_absolute_path(&self.root, to);

        let base = format!(
            "{}/webhdfs/v1/{}",
            self.endpoint,
            percent_encode_path(&from)
        );
        let url = self
            .push_auth_query(
                QueryPairsWriter::new(&base)
                    .push("op", "RENAME")
                    .push("destination", &percent_encode_path(&to)),
            )
            .finish();

        let req = Request::put(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    async fn webhdfs_init_append(&self, ctx: &OperationContext, path: &str) -> Result<String> {
        let p = build_absolute_path(&self.root, path);
        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let url = self
            .push_auth_query(
                QueryPairsWriter::new(&base)
                    .push("op", "APPEND")
                    .push("noredirect", "true"),
            )
            .finish();

        let req = Request::post(url)
            .extension(Operation::Write)
            .extension(ServiceOperation("Append"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = ctx.http_transport().send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: LocationResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                Ok(resp.location)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Append")),
                resp,
            )),
        }
    }

    pub async fn webhdfs_append(
        &self,
        ctx: &OperationContext,
        path: &str,
        size: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let url = self.webhdfs_init_append(ctx, path).await?;

        let mut req = Request::post(&url);

        req = req.header(CONTENT_LENGTH, size.to_string());

        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("Append"))
            .body(body)
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    /// CONCAT will concat sources to the path
    pub async fn webhdfs_concat(
        &self,
        ctx: &OperationContext,
        path: &str,
        sources: Vec<String>,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path);

        let sources = sources
            .iter()
            .map(|p| build_rooted_absolute_path(&self.root, p))
            .collect::<Vec<String>>()
            .join(",");

        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let url = self
            .push_auth_query(
                QueryPairsWriter::new(&base)
                    .push("op", "CONCAT")
                    .push("sources", &percent_encode_path(&sources)),
            )
            .finish();

        let req = Request::post(url);

        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("Concat"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn webhdfs_list_status(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path);
        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let url = self
            .push_auth_query(QueryPairsWriter::new(&base).push("op", "LISTSTATUS"))
            .finish();

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        ctx.http_transport().send(req).await
    }

    pub async fn webhdfs_list_status_batch(
        &self,
        ctx: &OperationContext,
        path: &str,
        start_after: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path);
        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let mut query = QueryPairsWriter::new(&base).push("op", "LISTSTATUS_BATCH");
        if !start_after.is_empty() {
            query = query.push("startAfter", &percent_encode_query_value(start_after));
        }
        let url = self.push_auth_query(query).finish();

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        ctx.http_transport().send(req).await
    }

    fn webhdfs_open_request(&self, path: &str, range: &BytesRange) -> Result<Request<Buffer>> {
        if range.is_suffix() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "webhdfs does not support suffix range read",
            ));
        }

        let p = build_absolute_path(&self.root, path);
        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let mut query = self.push_auth_query(QueryPairsWriter::new(&base).push("op", "OPEN"));

        if !range.is_full() {
            query = query.push("offset", &range.offset().to_string());
            if let Some(size) = range.size() {
                query = query.push("length", &size.to_string());
            }
        }
        let url = query.finish();

        let req = Request::get(&url)
            .extension(Operation::Read)
            .extension(ServiceOperation("Open"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn webhdfs_read_file(
        &self,
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<HttpBody>> {
        let req = self.webhdfs_open_request(path, &range)?;
        ctx.http_transport().fetch(req).await
    }

    pub(super) async fn webhdfs_get_file_status(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path);
        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let url = self
            .push_auth_query(QueryPairsWriter::new(&base).push("op", "GETFILESTATUS"))
            .finish();

        let req = Request::get(&url)
            .extension(Operation::Stat)
            .extension(ServiceOperation("GetFileStatus"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }

    pub async fn webhdfs_delete(
        &self,
        ctx: &OperationContext,
        path: &str,
        recursive: bool,
    ) -> Result<Response<Buffer>> {
        let p = build_absolute_path(&self.root, path);
        let base = format!("{}/webhdfs/v1/{}", self.endpoint, percent_encode_path(&p));
        let url = self
            .push_auth_query(
                QueryPairsWriter::new(&base)
                    .push("op", "DELETE")
                    .push("recursive", if recursive { "true" } else { "false" }),
            )
            .finish();

        let req = Request::delete(&url)
            .extension(Operation::Delete)
            .extension(ServiceOperation("Delete"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        ctx.http_transport().send(req).await
    }
}

fn percent_encode_query_value(value: &str) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            let _ = write!(&mut encoded, "%{byte:02X}");
        }
    }
    encoded
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct LocationResponse {
    pub location: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct WebHdfsErrorWrapper {
    pub remote_exception: WebHdfsError,
}

/// WebHdfsError is the error message returned by WebHdfs service
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct WebHdfsError {
    exception: String,
    message: String,
    java_class_name: String,
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

fn classify_forbidden_error(
    ctx: ErrorContext,
    remote_error: Option<&WebHdfsError>,
) -> (ErrorKind, bool) {
    let Some(error) = remote_error else {
        return (ErrorKind::PermissionDenied, false);
    };

    // Hadoop reports an unsatisfiable OPEN offset as a 403 IOException.
    if ctx.service_operation == ServiceOperation("Open")
        && error.java_class_name == "java.io.IOException"
        && error.message.starts_with("Offset=")
        && error.message.contains(" out of the range [0, ")
    {
        return (ErrorKind::RangeNotSatisfied, false);
    }

    // WebHDFS maps all java.io.IOException subclasses (except FileNotFoundException)
    // to HTTP 403 Forbidden, so inspect the Java exception class before assuming permission error.
    let exception_name = error
        .java_class_name
        .rsplit(['.', '$'])
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or(error.exception.as_str());

    match exception_name {
        "AccessControlException"
        | "SecurityException"
        | "AuthorizationException"
        | "InvalidToken" => (ErrorKind::PermissionDenied, false),
        "FileAlreadyExistsException" => (ErrorKind::AlreadyExists, false),
        "ParentNotDirectoryException" | "PathIsNotDirectoryException" => {
            (ErrorKind::NotADirectory, false)
        }
        "PathIsDirectoryException" => (ErrorKind::IsADirectory, false),
        "PathIsNotEmptyDirectoryException" | "AlreadyBeingCreatedException" => {
            (ErrorKind::Conflict, false)
        }
        "SafeModeException" | "StandbyException" | "RetriableException" => {
            (ErrorKind::Unexpected, true)
        }
        _ => (ErrorKind::Unexpected, false),
    }
}

/// Parse an error response using its service request context.
pub(crate) fn parse_error(ctx: ErrorContext, resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();
    let remote_error = serde_json::from_slice::<WebHdfsErrorWrapper>(&bs)
        .ok()
        .map(|error| error.remote_exception);
    let (kind, retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::UNAUTHORIZED => (ErrorKind::PermissionDenied, false),
        StatusCode::FORBIDDEN => classify_forbidden_error(ctx, remote_error.as_ref()),
        StatusCode::TOO_MANY_REQUESTS => (ErrorKind::RateLimited, true),
        // passing invalid arguments will return BAD_REQUEST
        // should be un-retryable
        StatusCode::BAD_REQUEST => (ErrorKind::Unexpected, false),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let message = match remote_error {
        Some(error) => format!("{error:?}"),
        None => String::from_utf8_lossy(&bs).into_owned(),
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
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use bytes::Buf;
    use serde_json::from_reader;

    use super::*;

    struct MockTransport {
        requests: Arc<Mutex<Vec<String>>>,
        responses: Mutex<VecDeque<Response<Buffer>>>,
    }

    impl MockTransport {
        fn new(responses: Vec<Response<Buffer>>) -> (Self, Arc<Mutex<Vec<String>>>) {
            let requests = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    requests: requests.clone(),
                    responses: Mutex::new(VecDeque::from(responses)),
                },
                requests,
            )
        }
    }

    impl HttpTransport for MockTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.lock().unwrap().push(req.uri().to_string());
            let resp = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request in MockTransport");
            let (parts, body) = resp.into_parts();
            let size = body.len() as u64;
            Ok(Response::from_parts(
                parts,
                HttpBody::new(futures::stream::iter([Ok(body)]), Some(size)),
            ))
        }
    }

    fn test_core(user_name: Option<&str>, delegation: Option<&str>) -> WebhdfsCore {
        WebhdfsCore {
            info: ServiceInfo::new("webhdfs", "/", ""),
            capability: Capability::default(),
            root: "/".to_string(),
            endpoint: "http://127.0.0.1:9870".to_string(),
            user_name: user_name.map(str::to_string),
            delegation: delegation.map(str::to_string),
            root_checker: OnceCell::new(),
            atomic_write_dir: None,
            disable_list_batch: false,
        }
    }

    #[test]
    fn test_parse_open_offset_out_of_range() {
        for (operation, class, message, expected) in [
            (
                "Open",
                "java.io.IOException",
                "Offset=6 out of the range [0, 6); OPEN, path=/file",
                ErrorKind::RangeNotSatisfied,
            ),
            (
                "Open",
                "java.io.IOException",
                "Permission denied",
                ErrorKind::Unexpected,
            ),
            (
                "Open",
                "org.apache.hadoop.security.AccessControlException",
                "Offset=6 out of the range [0, 6); OPEN, path=/file",
                ErrorKind::PermissionDenied,
            ),
            (
                "GetFileStatus",
                "java.io.IOException",
                "Offset=6 out of the range [0, 6); OPEN, path=/file",
                ErrorKind::Unexpected,
            ),
        ] {
            let body = serde_json::json!({
                "RemoteException": { "exception": class.rsplit('.').next().unwrap(), "javaClassName": class, "message": message }
            });
            let response = Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Buffer::from(body.to_string()))
                .unwrap();
            let err = parse_error(ErrorContext::new(ServiceOperation(operation)), response);
            assert_eq!(err.kind(), expected, "{operation}: {message}");
            assert!(!err.is_temporary());
        }
    }

    #[test]
    fn test_parse_forbidden_exceptions_and_rate_limit() {
        for (class, exception, expected_kind, expected_temporary) in [
            (
                "org.apache.hadoop.security.AccessControlException",
                "AccessControlException",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "java.lang.SecurityException",
                "SecurityException",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "org.apache.hadoop.security.authorize.AuthorizationException",
                "AuthorizationException",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "org.apache.hadoop.security.token.SecretManager$InvalidToken",
                "InvalidToken",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "org.apache.hadoop.fs.FileAlreadyExistsException",
                "FileAlreadyExistsException",
                ErrorKind::AlreadyExists,
                false,
            ),
            (
                "org.apache.hadoop.fs.ParentNotDirectoryException",
                "ParentNotDirectoryException",
                ErrorKind::NotADirectory,
                false,
            ),
            (
                "org.apache.hadoop.fs.PathIsNotDirectoryException",
                "PathIsNotDirectoryException",
                ErrorKind::NotADirectory,
                false,
            ),
            (
                "org.apache.hadoop.fs.PathIsDirectoryException",
                "PathIsDirectoryException",
                ErrorKind::IsADirectory,
                false,
            ),
            (
                "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException",
                "PathIsNotEmptyDirectoryException",
                ErrorKind::Conflict,
                false,
            ),
            (
                "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException",
                "AlreadyBeingCreatedException",
                ErrorKind::Conflict,
                false,
            ),
            (
                "org.apache.hadoop.hdfs.server.namenode.SafeModeException",
                "SafeModeException",
                ErrorKind::Unexpected,
                true,
            ),
            (
                "org.apache.hadoop.ipc.StandbyException",
                "StandbyException",
                ErrorKind::Unexpected,
                true,
            ),
            (
                "org.apache.hadoop.ipc.RetriableException",
                "RetriableException",
                ErrorKind::Unexpected,
                true,
            ),
        ] {
            let body = serde_json::json!({
                "RemoteException": {
                    "exception": exception,
                    "javaClassName": class,
                    "message": "test error"
                }
            });
            let resp = Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Buffer::from(body.to_string()))
                .unwrap();
            let err = parse_error(ErrorContext::new(ServiceOperation("Test")), resp);
            assert_eq!(err.kind(), expected_kind, "{class}");
            assert_eq!(err.is_temporary(), expected_temporary, "{class}");
        }

        // Plain 403 without RemoteException JSON still maps to PermissionDenied.
        let plain_403 = Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Buffer::from("Forbidden"))
            .unwrap();
        let err = parse_error(ErrorContext::new(ServiceOperation("Test")), plain_403);
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(!err.is_temporary());

        // HTTP 429 maps to temporary RateLimited.
        let rate_limited = Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(Buffer::from("Too Many Requests"))
            .unwrap();
        let err = parse_error(ErrorContext::new(ServiceOperation("Test")), rate_limited);
        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert!(err.is_temporary());
    }

    #[tokio::test]
    async fn test_auth_query_encoding_and_append_redirect() -> Result<()> {
        let core = test_core(Some("alice+1&admin=true"), Some("tok+en/1==&x=1"));
        let expected_auth =
            "user.name=alice%2B1%26admin%3Dtrue&delegation=tok%2Ben%2F1%3D%3D%26x%3D1";

        let datanode_create_url =
            "http://datanode:9864/webhdfs/v1/file.txt?op=CREATE&namenoderpcaddress=namenode:8020";
        let datanode_append_url = "http://datanode:9864/webhdfs/v1/file.txt?op=APPEND&namenoderpcaddress=namenode:8020&user.name=alice%2B1%26admin%3Dtrue";

        let (transport, requests) = MockTransport::new(vec![
            Response::builder()
                .status(StatusCode::OK)
                .body(Buffer::from("{}"))
                .unwrap(),
            Response::builder()
                .status(StatusCode::OK)
                .body(Buffer::from(
                    serde_json::json!({ "Location": datanode_create_url }).to_string(),
                ))
                .unwrap(),
            Response::builder()
                .status(StatusCode::CREATED)
                .body(Buffer::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::OK)
                .body(Buffer::from(
                    serde_json::json!({ "Location": datanode_append_url }).to_string(),
                ))
                .unwrap(),
            Response::builder()
                .status(StatusCode::OK)
                .body(Buffer::new())
                .unwrap(),
        ]);
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));

        core.webhdfs_get_file_status(&ctx, "file.txt").await?;
        core.webhdfs_create_object(
            &ctx,
            "file.txt",
            Some(4),
            &OpWrite::default(),
            Buffer::from("data"),
        )
        .await?;
        core.webhdfs_append(&ctx, "file.txt", 4, Buffer::from("more"))
            .await?;

        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 5);
        assert_eq!(
            captured[0],
            format!("http://127.0.0.1:9870/webhdfs/v1/file.txt?op=GETFILESTATUS&{expected_auth}")
        );
        assert_eq!(
            captured[1],
            format!(
                "http://127.0.0.1:9870/webhdfs/v1/file.txt?op=CREATE&overwrite=true&noredirect=true&{expected_auth}"
            )
        );
        assert_eq!(captured[2], datanode_create_url);
        assert_eq!(
            captured[3],
            format!(
                "http://127.0.0.1:9870/webhdfs/v1/file.txt?op=APPEND&noredirect=true&{expected_auth}"
            )
        );
        // DataNode APPEND URL must be used verbatim without duplicating user.name/delegation.
        assert_eq!(captured[4], datanode_append_url);
        assert_eq!(captured[4].matches("user.name=").count(), 1);
        assert!(!captured[4].contains("delegation="));

        Ok(())
    }

    #[test]
    fn test_webhdfs_open_request_rejects_suffix_range() {
        let core = test_core(None, None);

        let err = core
            .webhdfs_open_request("a.txt", &BytesRange::suffix(5))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let req = core
            .webhdfs_open_request("a.txt", &BytesRange::new(2, Some(5)))
            .unwrap();
        assert_eq!(
            req.uri().to_string(),
            "http://127.0.0.1:9870/webhdfs/v1/a.txt?op=OPEN&offset=2&length=5"
        );

        let full_req = core
            .webhdfs_open_request("a.txt", &BytesRange::new(0, None))
            .unwrap();
        assert_eq!(
            full_req.uri().to_string(),
            "http://127.0.0.1:9870/webhdfs/v1/a.txt?op=OPEN"
        );
    }

    /// Error response example from https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Error%20Responses
    #[tokio::test]
    async fn test_parse_error() -> Result<()> {
        let ill_args = bytes::Bytes::from(
            r#"
{
  "RemoteException":
  {
    "exception"    : "IllegalArgumentException",
    "javaClassName": "java.lang.IllegalArgumentException",
    "message"      : "Invalid value for webhdfs parameter \"permission\": ..."
  }
}
    "#,
        );
        let body = Buffer::from(ill_args.clone());
        let resp = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(body)
            .unwrap();

        let err = parse_error(ErrorContext::new(ServiceOperation("Test")), resp);
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err_msg: WebHdfsError = from_reader::<_, WebHdfsErrorWrapper>(ill_args.reader())
            .expect("must success")
            .remote_exception;
        assert_eq!(err_msg.exception, "IllegalArgumentException");
        assert_eq!(
            err_msg.java_class_name,
            "java.lang.IllegalArgumentException"
        );
        assert_eq!(
            err_msg.message,
            "Invalid value for webhdfs parameter \"permission\": ..."
        );

        Ok(())
    }
}
