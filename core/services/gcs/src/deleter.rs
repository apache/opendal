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
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let resp = self.core.gcs_delete_object(&self.ctx, &path).await?;

        // deleting not existing objects is ok
        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(parse_error(resp))
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let paths: Vec<String> = batch.into_iter().map(|(p, _)| p).collect();
        let resp = self
            .core
            .gcs_delete_objects(&self.ctx, paths.clone())
            .await?;

        let status = resp.status();

        // If the overall request isn't formatted correctly and Cloud Storage is unable to parse it into sub-requests, you receive a 400 error.
        // Otherwise, Cloud Storage returns a 200 status code, even if some or all of the sub-requests fail.
        if status != StatusCode::OK {
            return Err(parse_error(resp));
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

        if paths.len() != parts.len() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "invalid batch response, paths and response parts don't match",
            ));
        }

        let mut batched_result = BatchDeleteResult::default();

        for (i, part) in parts.into_iter().enumerate() {
            let resp = part.into_response();
            // TODO: maybe we can take it directly?
            let path = paths[i].clone();

            // deleting not existing objects is ok
            if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
                batched_result.succeeded.push((path, OpDelete::default()));
            } else {
                batched_result
                    .failed
                    .push((path, OpDelete::default(), parse_error(resp)));
            }
        }

        // If no object is deleted, return directly.
        if batched_result.succeeded.is_empty() {
            let err = batched_result.failed.remove(0).2;
            return Err(err);
        }

        Ok(batched_result)
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use bytes::Bytes;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::Operator;

    use super::*;
    use crate::backend::GcsBuilder;

    /// Answers the batch endpoint with a body the caller controls, so the number of
    /// sub-responses can be made to disagree with the number of requested paths.
    #[derive(Clone)]
    struct BatchMockTransport {
        body: &'static str,
        calls: Arc<Mutex<usize>>,
    }

    impl BatchMockTransport {
        fn new(body: &'static str) -> Self {
            Self {
                body,
                calls: Arc::new(Mutex::new(0)),
            }
        }
    }

    impl HttpTransport for BatchMockTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            *self.calls.lock().expect("lock poisoned") += 1;
            let body = Buffer::from(Bytes::from_static(self.body.as_bytes()));
            let size = body.len() as u64;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "multipart/mixed; boundary=batch_x")
                .body(HttpBody::new(
                    futures::stream::iter(vec![Ok(body)]),
                    Some(size),
                ))
                .expect("mock response must build"))
        }
    }

    fn operator(body: &'static str) -> Operator {
        Operator::new(
            GcsBuilder::default()
                .bucket("example")
                .token("token".to_string())
                .disable_config_load()
                .disable_vm_metadata(),
        )
        .expect("operator must build")
        .with_context(
            OperationContext::new()
                .with_http_transport(HttpTransporter::new(BatchMockTransport::new(body))),
        )
    }

    /// A batch response carrying no sub-responses used to reach
    /// `batched_result.failed.remove(0)` with both vectors empty.
    #[tokio::test]
    async fn delete_batch_errors_when_the_response_has_no_parts() {
        let op = operator("--batch_x--\r\n");

        let err = op
            .delete_iter(vec!["a".to_string(), "b".to_string()])
            .await
            .expect_err("must report an error rather than panic");

        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    /// And one carrying more sub-responses than paths used to index `paths` out of
    /// bounds.
    #[tokio::test]
    async fn delete_batch_errors_when_the_response_has_extra_parts() {
        let op = operator(concat!(
            "--batch_x\r\n",
            "Content-Type: application/http\r\n\r\n",
            "HTTP/1.1 204 No Content\r\n\r\n",
            "--batch_x\r\n",
            "Content-Type: application/http\r\n\r\n",
            "HTTP/1.1 204 No Content\r\n\r\n",
            "--batch_x\r\n",
            "Content-Type: application/http\r\n\r\n",
            "HTTP/1.1 204 No Content\r\n\r\n",
            "--batch_x--\r\n"
        ));

        let err = op
            .delete_iter(vec!["a".to_string(), "b".to_string()])
            .await
            .expect_err("must report an error rather than panic");

        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    /// Control: a response with one part per path still succeeds.
    #[tokio::test]
    async fn delete_batch_succeeds_when_the_counts_agree() {
        let op = operator(concat!(
            "--batch_x\r\n",
            "Content-Type: application/http\r\n\r\n",
            "HTTP/1.1 204 No Content\r\n\r\n",
            "--batch_x\r\n",
            "Content-Type: application/http\r\n\r\n",
            "HTTP/1.1 204 No Content\r\n\r\n",
            "--batch_x--\r\n"
        ));

        op.delete_iter(vec!["a".to_string(), "b".to_string()])
            .await
            .expect("must succeed");
    }
}
