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

use std::sync::Arc;

use asyncband::once::OnceCell;
use http::StatusCode;

use super::core::FILE;
use super::core::X_MS_VERSION_ID;
use super::core::parse_error;
use super::core::{AzdlsCore, ErrorContext};
use opendal_core::raw::*;
use opendal_core::*;

/// Writer type for azdls: non-append uses PositionWriter, append uses AppendWriter.
pub type AzdlsWriters =
    TwoWays<oio::PositionWriter<AzdlsLazyPositionWriter>, oio::AppendWriter<AzdlsWriter>>;

pub struct AzdlsLazyPositionWriter {
    inner: AzdlsWriter,
    created: OnceCell<()>,
}

impl AzdlsLazyPositionWriter {
    pub fn new(inner: AzdlsWriter) -> Self {
        Self {
            inner,
            created: OnceCell::new(),
        }
    }

    async fn ensure_created(&self) -> Result<()> {
        self.created
            .get_or_try_init(async || self.inner.create_if_needed().await)
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AzdlsWriter {
    core: Arc<AzdlsCore>,
    ctx: OperationContext,
    op: OpWrite,
    path: String,
}

impl AzdlsWriter {
    pub fn new(core: Arc<AzdlsCore>, ctx: OperationContext, op: OpWrite, path: String) -> Self {
        Self {
            core,
            ctx,
            op,
            path,
        }
    }

    async fn create_if_needed(&self) -> Result<()> {
        let resp = self
            .core
            .azdls_create(&self.ctx, &self.path, FILE, &self.op)
            .await?;
        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            _ => {
                // Both `if_not_exists` and `if_none_match(<etag>)` send `If-None-Match`
                // on CreateFile; ADLS can reject either failed precondition with
                // `409 PathAlreadyExists`, which must classify as `ConditionNotMatch`.
                let if_none_match = self.op.if_not_exists() || self.op.if_none_match().is_some();
                let err = parse_error(
                    ErrorContext::new(ServiceOperation("CreateFile"))
                        .with_caller_condition(self.op.is_conditional())
                        .with_if_not_exists(if_none_match),
                    resp,
                )
                .with_operation("Backend::azdls_create_request");
                if !self.op.is_conditional() && err.kind() == ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    fn parse_metadata(headers: &http::HeaderMap) -> Result<Metadata> {
        let mut metadata = MetadataBuilder::unknown();

        if let Some(last_modified) = parse_last_modified(headers)? {
            metadata.last_modified(last_modified);
        }
        let etag = parse_etag(headers)?;
        if let Some(etag) = etag {
            metadata.etag(etag);
        }
        let version_id = parse_header_to_str(headers, X_MS_VERSION_ID)?;
        if let Some(version_id) = version_id {
            metadata.version(version_id);
        }

        Ok(metadata.build())
    }
}

impl oio::PositionWrite for AzdlsWriter {
    async fn write_all_at(&self, offset: u64, buf: Buffer) -> Result<()> {
        let size = buf.len() as u64;
        let resp = self
            .core
            .azdls_append(&self.ctx, &self.path, Some(size), offset, false, false, buf)
            .await?;

        match resp.status() {
            StatusCode::OK | StatusCode::ACCEPTED => Ok(()),
            _ => Err(
                parse_error(ErrorContext::new(ServiceOperation("AppendData")), resp)
                    .with_operation("Backend::azdls_append_request"),
            ),
        }
    }

    async fn close(&self, size: u64) -> Result<Metadata> {
        // Flush accumulated appends once.
        let resp = self
            .core
            .azdls_flush(&self.ctx, &self.path, size, true)
            .await?;

        let mut meta = AzdlsWriter::parse_metadata(resp.headers())?.into_builder();
        meta.set_file(size);

        match resp.status() {
            StatusCode::OK | StatusCode::ACCEPTED => Ok(meta.build()),
            _ => Err(
                parse_error(ErrorContext::new(ServiceOperation("FlushData")), resp)
                    .with_operation("Backend::azdls_flush_request"),
            ),
        }
    }

    async fn abort(&self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "Abort is not supported for azdls writer",
        ))
    }
}

impl oio::PositionWrite for AzdlsLazyPositionWriter {
    async fn write_all_at(&self, offset: u64, buf: Buffer) -> Result<()> {
        self.ensure_created().await?;
        self.inner.write_all_at(offset, buf).await
    }

    async fn close(&self, size: u64) -> Result<Metadata> {
        self.ensure_created().await?;
        self.inner.close(size).await
    }

    async fn abort(&self) -> Result<()> {
        self.inner.abort().await
    }
}

impl oio::AppendWrite for AzdlsWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .azdls_get_properties(&self.ctx, &self.path, &OpStat::default())
            .await?;

        let status = resp.status();
        let headers = resp.headers();

        match status {
            StatusCode::OK => Ok(parse_content_length(headers)?.unwrap_or_default()),
            StatusCode::NOT_FOUND => Ok(0),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("GetPathProperties")),
                resp,
            )),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        if offset == 0 {
            // Only create when starting a new file; avoid 404 when appending to a non-existent path.
            self.create_if_needed().await?;
        }

        // append + flush in a single request to minimize roundtrips for append mode.
        let resp = self
            .core
            .azdls_append(&self.ctx, &self.path, Some(size), offset, true, false, body)
            .await?;

        let mut meta = AzdlsWriter::parse_metadata(resp.headers())?.into_builder();
        let md5 = parse_content_md5(resp.headers())?;
        if let Some(md5) = md5 {
            meta.content_md5(md5);
        }
        meta.set_file(offset + size);

        match resp.status() {
            StatusCode::OK | StatusCode::ACCEPTED => Ok(meta.build()),
            _ => Err(
                parse_error(ErrorContext::new(ServiceOperation("AppendData")), resp)
                    .with_operation("Backend::azdls_append_request"),
            ),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use bytes::Bytes;
    use http::{Method, Response, StatusCode, header::IF_NONE_MATCH};
    use opendal_core::ErrorKind;

    use crate::backend::test_util::{ScriptedTransport, test_operator};

    #[tokio::test]
    async fn write_if_none_match_wildcard_fails_on_path_already_exists_without_overwriting() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::CONFLICT)
                .header("x-ms-error-code", "PathAlreadyExists")
                .body(Bytes::from_static(
                    br#"{"error":{"code":"PathAlreadyExists","message":"The specified path already exists."}}"#,
                ))
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        let err = op
            .write_with("exists.txt", "new")
            .if_none_match("*")
            .await
            .expect_err("write with if_none_match('*') must fail on 409 PathAlreadyExists");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(
            err.to_string()
                .contains("The specified path already exists.")
        );

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, Method::PUT);
        assert!(requests[0].uri.contains("resource=file"));
        assert_eq!(requests[0].headers[IF_NONE_MATCH], "*");
    }

    #[tokio::test]
    async fn write_if_not_exists_fails_on_path_already_exists_without_overwriting() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::CONFLICT)
                .header("x-ms-error-code", "PathAlreadyExists")
                .body(Bytes::from_static(
                    br#"{"error":{"code":"PathAlreadyExists","message":"The specified path already exists."}}"#,
                ))
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        let err = op
            .write_with("exists.txt", "new")
            .if_not_exists(true)
            .await
            .expect_err("write with if_not_exists must fail on 409 PathAlreadyExists");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(transport.requests().len(), 1);
    }

    #[tokio::test]
    async fn write_if_none_match_etag_returns_condition_not_match() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::CONFLICT)
                .header("x-ms-error-code", "PathAlreadyExists")
                .body(Bytes::from_static(
                    br#"{"error":{"code":"PathAlreadyExists","message":"exists"}}"#,
                ))
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        let err = op
            .write_with("exists.txt", "new")
            .if_none_match("\"etag-1\"")
            .await
            .expect_err("write with if_none_match('<etag>') must fail on 409 PathAlreadyExists");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].headers[IF_NONE_MATCH], "\"etag-1\"");

        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .header("x-ms-error-code", "ConditionNotMet")
                .body(Bytes::from_static(
                    br#"{"error":{"code":"ConditionNotMet","message":"The condition specified using HTTP conditional header(s) is not met."}}"#,
                ))
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        let err = op
            .write_with("exists.txt", "new")
            .if_none_match("\"etag-1\"")
            .await
            .expect_err("write with if_none_match('<etag>') must fail on 412 ConditionNotMet");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].headers[IF_NONE_MATCH], "\"etag-1\"");
    }

    #[tokio::test]
    async fn write_if_match_returns_unsupported_without_sending_request() {
        let transport = ScriptedTransport::new(vec![]);
        let op = test_operator(transport.clone());

        let err = op
            .write_with("exists.txt", "new")
            .if_match("\"etag-1\"")
            .await
            .expect_err("write with if_match must be rejected when capability is false");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn write_unconditional_swallows_already_exists_and_overwrites() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::CONFLICT)
                .header("x-ms-error-code", "PathAlreadyExists")
                .body(Bytes::from_static(
                    br#"{"error":{"code":"PathAlreadyExists","message":"exists"}}"#,
                ))
                .unwrap(),
            Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::OK)
                .body(Bytes::new())
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        op.write("exists.txt", "hello")
            .await
            .expect("unconditional write must proceed after 409 PathAlreadyExists");

        let requests = transport.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method, Method::PUT);
        assert!(!requests[0].headers.contains_key(IF_NONE_MATCH));
        assert_eq!(requests[1].method, Method::PATCH);
        assert!(requests[1].uri.contains("action=append"));
        assert_eq!(requests[2].method, Method::PATCH);
        assert!(requests[2].uri.contains("action=flush"));
    }

    #[tokio::test]
    async fn append_preserves_unconditional_create_and_rejects_conditional_already_exists() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::CONFLICT)
                .header("x-ms-error-code", "PathAlreadyExists")
                .body(Bytes::from_static(
                    br#"{"error":{"code":"PathAlreadyExists","message":"exists"}}"#,
                ))
                .unwrap(),
            Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Bytes::new())
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        op.write_with("log.txt", "hello")
            .append(true)
            .await
            .expect("unconditional append must proceed after 409 PathAlreadyExists");
        assert_eq!(transport.requests().len(), 3);

        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::CONFLICT)
                .header("x-ms-error-code", "PathAlreadyExists")
                .body(Bytes::from_static(
                    br#"{"error":{"code":"PathAlreadyExists","message":"exists"}}"#,
                ))
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        let err = op
            .write_with("log.txt", "hello")
            .append(true)
            .if_none_match("*")
            .await
            .expect_err("conditional append must not swallow 409 PathAlreadyExists");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(transport.requests().len(), 2);

        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::CONFLICT)
                .header("x-ms-error-code", "PathAlreadyExists")
                .body(Bytes::from_static(
                    br#"{"error":{"code":"PathAlreadyExists","message":"exists"}}"#,
                ))
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        let err = op
            .write_with("log.txt", "hello")
            .append(true)
            .if_none_match("\"etag-1\"")
            .await
            .expect_err("append with if_none_match('<etag>') must fail on 409 PathAlreadyExists");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(transport.requests().len(), 2);
    }

    #[tokio::test]
    async fn write_with_if_not_exists_and_if_none_match_sends_single_if_none_match_header() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::CREATED)
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::OK)
                .body(Bytes::new())
                .unwrap(),
        ]);
        let op = test_operator(transport.clone());

        op.write_with("new.txt", "hello")
            .if_not_exists(true)
            .if_none_match("\"etag-1\"")
            .await
            .expect("write must succeed");

        let requests = transport.requests();
        let headers: Vec<_> = requests[0]
            .headers
            .get_all(IF_NONE_MATCH)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(headers, vec!["*"]);
    }
}
