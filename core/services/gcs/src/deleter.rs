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
use super::core::*;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

pub struct GcsDeleter {
    core: Arc<GcsCore>,
    ctx: OperationContext,
}

impl GcsDeleter {
    pub fn new(core: Arc<GcsCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::BatchDelete for GcsDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let resp = self.core.gcs_delete_object(&self.ctx, &path, &args).await?;
        let error_ctx = ErrorContext::new(ServiceOperation("DeleteObject"))
            .with_caller_condition(args.is_conditional())
            .with_delete_match_condition(
                args.if_match().is_some() || args.if_version_match().is_some(),
            );

        if resp.status().is_success()
            || (resp.status() == StatusCode::NOT_FOUND
                && args.if_match().is_none()
                && args.if_version_match().is_none())
        {
            Ok(())
        } else {
            Err(parse_error(error_ctx, resp))
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let resp = self.core.gcs_delete_objects(&self.ctx, &batch).await?;

        let status = resp.status();

        // If the overall request isn't formatted correctly and Cloud Storage is unable to parse it into sub-requests, you receive a 400 error.
        // Otherwise, Cloud Storage returns a 200 status code, even if some or all of the sub-requests fail.
        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("BatchDeleteObjects")),
                resp,
            ));
        }

        let boundary = parse_multipart_boundary(resp.headers())?.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "gcs batch delete response content type is empty",
            )
        })?;
        let multipart: Multipart<MixedPart> = Multipart::new()
            .with_boundary(boundary)
            .parse(resp.into_body().to_bytes())?;
        let parts = multipart.into_parts();

        if batch.len() != parts.len() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "invalid batch response: request and response part counts do not match",
            ));
        }

        let mut batched_result = BatchDeleteResult::default();

        // Cloud Storage returns the parts in request order.
        for (part, (path, op)) in parts.into_iter().zip(batch) {
            let resp = part.into_response();
            let error_ctx = ErrorContext::new(ServiceOperation("BatchDeleteObjects"))
                .with_caller_condition(op.is_conditional())
                .with_delete_match_condition(
                    op.if_match().is_some() || op.if_version_match().is_some(),
                );

            if resp.status().is_success()
                || (resp.status() == StatusCode::NOT_FOUND
                    && op.if_match().is_none()
                    && op.if_version_match().is_none())
            {
                batched_result.succeeded.push((path, op));
            } else {
                batched_result
                    .failed
                    .push((path, op, parse_error(error_ctx, resp)));
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
    use futures::stream;
    use http::Response;
    use http::header::CONTENT_TYPE;

    use reqsign_core::ProvideCredentialChain;
    use reqsign_google::RequestSigner;
    use reqsign_google::TokenCredentialProvider;

    use super::*;
    use opendal_core::raw::oio::BatchDelete;

    struct StaticTransport {
        status: StatusCode,
        content_type: &'static str,
        body: String,
    }

    impl HttpTransport for StaticTransport {
        async fn fetch(&self, _: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Buffer::from(self.body.clone());
            let size = body.len() as u64;
            Response::builder()
                .status(self.status)
                .header(CONTENT_TYPE, self.content_type)
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(size)))
                .map_err(new_request_build_error)
        }
    }

    fn deleter_with_body(boundary: &'static str, body: String) -> GcsDeleter {
        let core = Arc::new(GcsCore {
            info: ServiceInfo::new("gcs", "/", "test-bucket"),
            capability: Capability {
                delete: true,
                delete_max_size: Some(100),
                delete_with_if_version_match: true,
                ..Default::default()
            },
            endpoint: "https://storage.googleapis.com".to_string(),
            bucket: "test-bucket".to_string(),
            root: "/".to_string(),
            sign_ctx: opendal_core::raw::reqsign_context(),
            credential_provider: Arc::new(
                ProvideCredentialChain::new().push(TokenCredentialProvider::new("test-token")),
            ),
            request_signer: Arc::new(RequestSigner::new("storage")),
            signers: HttpTransportCache::new(),
            predefined_acl: None,
            default_storage_class: None,
            skip_signature: true,
        });
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
            StaticTransport {
                status: StatusCode::OK,
                content_type: boundary,
                body,
            },
        ));
        GcsDeleter::new(core, ctx)
    }

    #[tokio::test]
    async fn delete_batch_correlates_responses_in_request_order() {
        let boundary = "batch_positional";
        let body = format!(
            "--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: <response-0>\r\n\
\r\n\
HTTP/1.1 204 No Content\r\n\
\r\n\
--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: <response-1>\r\n\
\r\n\
HTTP/1.1 403 Forbidden\r\n\
Content-Type: application/json\r\n\
\r\n\
{{\"error\":{{\"code\":403,\"message\":\"forbidden\"}}}}\r\n\
--{boundary}--\r\n"
        );
        let deleter = deleter_with_body("multipart/mixed; boundary=batch_positional", body);

        let result = deleter
            .delete_batch(vec![
                ("ok.txt".to_string(), OpDelete::default()),
                ("denied.txt".to_string(), OpDelete::default()),
            ])
            .await
            .expect("ordered subresponses should be classified");

        assert_eq!(result.succeeded.len(), 1);
        assert_eq!(result.succeeded[0].0, "ok.txt");
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].0, "denied.txt");
        assert_eq!(result.failed[0].2.kind(), ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn delete_batch_rejects_part_count_mismatch_without_panicking() {
        let boundary = "batch_mismatch";
        let more_parts = format!(
            "--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: response-0\r\n\
\r\n\
HTTP/1.1 204 No Content\r\n\
\r\n\
--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: response-1\r\n\
\r\n\
HTTP/1.1 204 No Content\r\n\
\r\n\
--{boundary}--\r\n"
        );
        let deleter = deleter_with_body("multipart/mixed; boundary=batch_mismatch", more_parts);
        let err = deleter
            .delete_batch(vec![("only.txt".to_string(), OpDelete::default())])
            .await
            .err()
            .expect("more response parts than requests must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let fewer_parts = format!(
            "--{boundary}\r\n\
Content-Type: application/http\r\n\
Content-ID: response-0\r\n\
\r\n\
HTTP/1.1 204 No Content\r\n\
\r\n\
--{boundary}--\r\n"
        );
        let deleter = deleter_with_body("multipart/mixed; boundary=batch_mismatch", fewer_parts);
        let err = deleter
            .delete_batch(vec![
                ("a.txt".to_string(), OpDelete::default()),
                ("b.txt".to_string(), OpDelete::default()),
            ])
            .await
            .err()
            .expect("fewer response parts than requests must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }
}
