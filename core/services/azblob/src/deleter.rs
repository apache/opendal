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

use http::StatusCode;

use super::core::parse_error;
use super::core::parse_error_with_code;
use super::core::*;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

fn is_missing_blob_code(code: Option<&str>) -> bool {
    matches!(code, Some("BlobNotFound" | "ParentNotFound"))
}

fn parse_batch_content_id(part: &MixedPart, batch_len: usize) -> Result<usize> {
    let mut values = part.part_headers().get_all("content-id").iter();
    let Some(value) = values.next() else {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "invalid batch response: part is missing Content-ID header",
        ));
    };
    if values.next().is_some() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "invalid batch response: part contains duplicate Content-ID header",
        ));
    }

    let raw = value.to_str().map_err(|err| {
        Error::new(
            ErrorKind::Unexpected,
            "invalid batch response: Content-ID header is not valid UTF-8",
        )
        .set_source(err)
    })?;
    let idx = raw.trim().parse::<usize>().map_err(|err| {
        Error::new(
            ErrorKind::Unexpected,
            "invalid batch response: Content-ID header is not a valid index",
        )
        .set_source(err)
    })?;
    if idx >= batch_len {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "invalid batch response: Content-ID index out of bounds",
        ));
    }
    Ok(idx)
}

pub struct AzblobDeleter {
    core: Arc<AzblobCore>,
    ctx: OperationContext,
}

impl AzblobDeleter {
    pub fn new(core: Arc<AzblobCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::BatchDelete for AzblobDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let error_ctx = ErrorContext::new(ServiceOperation("DeleteBlob"))
            .with_caller_condition(args.is_conditional());
        let resp = self
            .core
            .azblob_delete_blob(&self.ctx, &path, &args)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::ACCEPTED => Ok(()),
            // Parse 404 responses before treating them as idempotent success so
            // container-level errors such as ContainerNotFound still surface.
            StatusCode::NOT_FOUND => {
                let (err, code) = parse_error_with_code(error_ctx, resp);
                if !is_missing_blob_code(code.as_deref()) {
                    Err(err)
                } else if args.if_match().is_some() {
                    Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "delete precondition requires a live target",
                    )
                    .set_source(err))
                } else {
                    Ok(())
                }
            }
            _ => Err(parse_error(error_ctx, resp)),
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        // TODO: Add remove version support.
        // construct and complete batch request
        let resp = self.core.azblob_batch_delete(&self.ctx, &batch).await?;

        // check response status
        if resp.status() != StatusCode::ACCEPTED {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("BatchDeleteBlobs")),
                resp,
            ));
        }

        // get boundary from response header
        let boundary = parse_multipart_boundary(resp.headers())?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "invalid response: no boundary provided in header",
                )
            })?
            .to_string();

        let bs = resp.into_body().to_bytes();
        let multipart: Multipart<MixedPart> =
            Multipart::new().with_boundary(&boundary).parse(bs)?;
        let parts = multipart.into_parts();

        if batch.len() != parts.len() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "invalid batch response, requests and response parts don't match",
            ));
        }

        let has_content_id = parts
            .iter()
            .any(|part| part.part_headers().contains_key("content-id"));
        let batch_len = batch.len();
        let mut pending: Vec<Option<(String, OpDelete)>> = batch.into_iter().map(Some).collect();

        let mut batched_result = BatchDeleteResult::default();

        for (pos, part) in parts.into_iter().enumerate() {
            let idx = if has_content_id {
                parse_batch_content_id(&part, batch_len)?
            } else {
                pos
            };
            let Some((path, args)) = pending[idx].take() else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "invalid batch response: duplicate Content-ID",
                ));
            };
            let resp = part.into_response();
            let error_ctx = ErrorContext::new(ServiceOperation("BatchDeleteBlobs"))
                .with_caller_condition(args.is_conditional());

            match resp.status() {
                StatusCode::ACCEPTED => {
                    batched_result.succeeded.push((path, args));
                }
                StatusCode::NOT_FOUND => {
                    let (err, code) = parse_error_with_code(error_ctx, resp);
                    if !is_missing_blob_code(code.as_deref()) {
                        batched_result.failed.push((path, args, err));
                    } else if args.if_match().is_some() {
                        batched_result.failed.push((
                            path,
                            args,
                            Error::new(
                                ErrorKind::ConditionNotMatch,
                                "delete precondition requires a live target",
                            )
                            .set_source(err),
                        ));
                    } else {
                        batched_result.succeeded.push((path, args));
                    }
                }
                _ => {
                    let err = parse_error(error_ctx, resp);
                    batched_result.failed.push((path, args, err));
                }
            }
        }

        // If no object is deleted, return directly.
        if batched_result.succeeded.is_empty() && !batched_result.failed.is_empty() {
            let err = batched_result.failed.remove(0).2;
            return Err(err);
        }

        Ok(batched_result)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use http::header::CONTENT_TYPE;
    use opendal_core::raw::oio::BatchDelete;

    use super::*;
    use crate::backend::AzblobBuilder;

    const CONTAINER_NOT_FOUND_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>ContainerNotFound</Code>
  <Message>The specified container does not exist.</Message>
</Error>"#;

    const BLOB_NOT_FOUND_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>BlobNotFound</Code>
  <Message>The specified blob does not exist.</Message>
</Error>"#;

    const PARENT_NOT_FOUND_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>ParentNotFound</Code>
  <Message>The specified parent path does not exist.</Message>
</Error>"#;

    const RESOURCE_NOT_FOUND_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>ResourceNotFound</Code>
  <Message>The specified resource does not exist.</Message>
</Error>"#;

    #[derive(Clone)]
    struct StaticTransport {
        status: StatusCode,
        headers: Vec<(&'static str, &'static str)>,
        body: String,
        requests: Arc<AtomicUsize>,
    }

    impl StaticTransport {
        fn new(
            status: StatusCode,
            headers: Vec<(&'static str, &'static str)>,
            body: impl Into<String>,
        ) -> Self {
            Self {
                status,
                headers,
                body: body.into(),
                requests: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl HttpTransport for StaticTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            let mut builder = Response::builder().status(self.status);
            for (k, v) in &self.headers {
                builder = builder.header(*k, *v);
            }
            let bytes = Bytes::from(self.body.clone());
            let size = bytes.len() as u64;
            let body = if bytes.is_empty() {
                HttpBody::new(futures::stream::iter(std::iter::empty()), Some(0))
            } else {
                HttpBody::new(
                    futures::stream::iter(vec![Ok(Buffer::from(bytes))]),
                    Some(size),
                )
            };
            Ok(builder.body(body).expect("mock response must build"))
        }
    }

    fn deleter_with_transport(transport: StaticTransport) -> AzblobDeleter {
        let backend = AzblobBuilder::default()
            .container("test")
            .endpoint("https://myaccount.blob.core.windows.net")
            .skip_signature()
            .build_backend()
            .expect("azblob backend must build");
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        AzblobDeleter::new(backend.core, ctx)
    }

    fn op_delete_if_match(etag: &str) -> OpDelete {
        OpDelete::from_options(
            &Capability {
                delete_with_if_match: true,
                ..Default::default()
            },
            options::DeleteOptions {
                if_match: Some(etag.to_string()),
                ..Default::default()
            },
        )
        .expect("delete options must lower")
    }

    #[tokio::test]
    async fn delete_once_surfaces_container_not_found_and_non_blob_404() {
        let deleter = deleter_with_transport(StaticTransport::new(
            StatusCode::NOT_FOUND,
            vec![("x-ms-error-code", "ContainerNotFound")],
            CONTAINER_NOT_FOUND_XML,
        ));

        let err = deleter
            .delete_once("a".to_string(), OpDelete::default())
            .await
            .expect_err("ContainerNotFound must not be masked as Ok");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(!err.is_temporary());

        let err = deleter
            .delete_once("a".to_string(), op_delete_if_match("\"etag\""))
            .await
            .expect_err("ContainerNotFound must not be masked as ConditionNotMatch");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let resource_not_found_deleter = deleter_with_transport(StaticTransport::new(
            StatusCode::NOT_FOUND,
            vec![("x-ms-error-code", "ResourceNotFound")],
            RESOURCE_NOT_FOUND_XML,
        ));
        let err = resource_not_found_deleter
            .delete_once("a".to_string(), OpDelete::default())
            .await
            .expect_err("ResourceNotFound must not be treated as missing blob");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn delete_once_accepts_blob_not_found_and_parent_not_found() {
        for (code, body) in [
            ("BlobNotFound", BLOB_NOT_FOUND_XML),
            ("ParentNotFound", PARENT_NOT_FOUND_XML),
        ] {
            let deleter = deleter_with_transport(StaticTransport::new(
                StatusCode::NOT_FOUND,
                vec![("x-ms-error-code", code)],
                body,
            ));

            deleter
                .delete_once("missing".to_string(), OpDelete::default())
                .await
                .expect("unconditional missing blob delete should succeed");

            let err = deleter
                .delete_once("missing".to_string(), op_delete_if_match("\"etag\""))
                .await
                .expect_err("conditional missing blob delete must fail");
            assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
            assert!(StdError::source(&err).is_some());
        }
    }

    #[tokio::test]
    async fn delete_batch_correlates_out_of_order_content_id_responses() {
        let boundary = "batchresponse_test";
        let body = format!(
            "--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: 1\r\n\
\r\n\
HTTP/1.1 412 The condition specified using HTTP conditional header(s) is not met.\r\n\
x-ms-error-code: ConditionNotMet\r\n\
content-type: application/xml\r\n\
\r\n\
<Error><Code>ConditionNotMet</Code><Message>Precondition failed</Message></Error>\r\n\
--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: 0\r\n\
\r\n\
HTTP/1.1 202 Accepted\r\n\
x-ms-request-id: req-0\r\n\
\r\n\
--{boundary}--\r\n"
        );
        let deleter = deleter_with_transport(StaticTransport::new(
            StatusCode::ACCEPTED,
            vec![(
                CONTENT_TYPE.as_str(),
                "multipart/mixed; boundary=batchresponse_test",
            )],
            body,
        ));

        let result = deleter
            .delete_batch(vec![
                ("first".to_string(), OpDelete::default()),
                ("second".to_string(), op_delete_if_match("\"stale\"")),
            ])
            .await
            .expect("batch with partial success should return BatchDeleteResult");

        assert_eq!(result.succeeded.len(), 1);
        assert_eq!(result.succeeded[0].0, "first");
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].0, "second");
        assert_eq!(result.failed[0].2.kind(), ErrorKind::ConditionNotMatch);
    }

    #[tokio::test]
    async fn delete_batch_rejects_duplicate_missing_or_out_of_bounds_content_id() {
        let boundary = "batchresponse_invalid";
        let cases = [
            // Duplicate Content-ID: 0
            format!(
                "--{boundary}\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\nHTTP/1.1 202 Accepted\r\n\r\n\
--{boundary}\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\nHTTP/1.1 202 Accepted\r\n\r\n--{boundary}--\r\n"
            ),
            // Out of bounds Content-ID: 2
            format!(
                "--{boundary}\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\nHTTP/1.1 202 Accepted\r\n\r\n\
--{boundary}\r\nContent-Type: application/http\r\nContent-ID: 2\r\n\r\nHTTP/1.1 202 Accepted\r\n\r\n--{boundary}--\r\n"
            ),
            // Missing Content-ID on second part while first has it
            format!(
                "--{boundary}\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\nHTTP/1.1 202 Accepted\r\n\r\n\
--{boundary}\r\nContent-Type: application/http\r\n\r\nHTTP/1.1 202 Accepted\r\n\r\n--{boundary}--\r\n"
            ),
        ];

        for body in cases {
            let deleter = deleter_with_transport(StaticTransport::new(
                StatusCode::ACCEPTED,
                vec![(
                    CONTENT_TYPE.as_str(),
                    "multipart/mixed; boundary=batchresponse_invalid",
                )],
                body,
            ));

            let err = deleter
                .delete_batch(vec![
                    ("a".to_string(), OpDelete::default()),
                    ("b".to_string(), OpDelete::default()),
                ])
                .await
                .err()
                .expect("malformed Content-ID batch must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
        }
    }

    #[tokio::test]
    async fn delete_batch_surfaces_container_not_found_and_handles_missing_blobs() {
        let boundary = "batchresponse_404";
        let all_missing_container = format!(
            "--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: 0\r\n\
\r\n\
HTTP/1.1 404 Not Found\r\n\
x-ms-error-code: ContainerNotFound\r\n\
content-type: application/xml\r\n\
\r\n\
{CONTAINER_NOT_FOUND_XML}\r\n\
--{boundary}--\r\n"
        );
        let deleter = deleter_with_transport(StaticTransport::new(
            StatusCode::ACCEPTED,
            vec![(
                CONTENT_TYPE.as_str(),
                "multipart/mixed; boundary=batchresponse_404",
            )],
            all_missing_container,
        ));

        let err = deleter
            .delete_batch(vec![("a".to_string(), OpDelete::default())])
            .await
            .err()
            .expect("ContainerNotFound in batch delete must not succeed");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let mixed = format!(
            "--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: 0\r\n\
\r\n\
HTTP/1.1 404 Not Found\r\n\
x-ms-error-code: BlobNotFound\r\n\
content-type: application/xml\r\n\
\r\n\
{BLOB_NOT_FOUND_XML}\r\n\
--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: 1\r\n\
\r\n\
HTTP/1.1 404 Not Found\r\n\
x-ms-error-code: ContainerNotFound\r\n\
content-type: application/xml\r\n\
\r\n\
{CONTAINER_NOT_FOUND_XML}\r\n\
--{boundary}--\r\n"
        );
        let deleter = deleter_with_transport(StaticTransport::new(
            StatusCode::ACCEPTED,
            vec![(
                CONTENT_TYPE.as_str(),
                "multipart/mixed; boundary=batchresponse_404",
            )],
            mixed,
        ));

        let result = deleter
            .delete_batch(vec![
                ("missing-blob".to_string(), OpDelete::default()),
                ("missing-container".to_string(), OpDelete::default()),
            ])
            .await
            .expect("mixed batch should report partial result");
        assert_eq!(result.succeeded.len(), 1);
        assert_eq!(result.succeeded[0].0, "missing-blob");
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].0, "missing-container");
        assert_eq!(result.failed[0].2.kind(), ErrorKind::ConfigInvalid);

        // Positional fallback when all parts omit Content-ID.
        let no_content_id = format!(
            "--{boundary}\r\n\
Content-Type: application/http\r\n\
\r\n\
HTTP/1.1 202 Accepted\r\n\
\r\n\
--{boundary}\r\n\
Content-Type: application/http\r\n\
\r\n\
HTTP/1.1 404 Not Found\r\n\
x-ms-error-code: ParentNotFound\r\n\
\r\n\
{PARENT_NOT_FOUND_XML}\r\n\
--{boundary}--\r\n"
        );
        let deleter = deleter_with_transport(StaticTransport::new(
            StatusCode::ACCEPTED,
            vec![(
                CONTENT_TYPE.as_str(),
                "multipart/mixed; boundary=batchresponse_404",
            )],
            no_content_id,
        ));
        let result = deleter
            .delete_batch(vec![
                ("a".to_string(), OpDelete::default()),
                ("b".to_string(), OpDelete::default()),
            ])
            .await
            .expect("positional fallback without Content-ID should succeed");
        assert_eq!(result.succeeded.len(), 2);
        assert!(result.failed.is_empty());
    }
}
