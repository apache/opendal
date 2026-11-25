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

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Buf;
use http::StatusCode;

use crate::core::parse_error;
use crate::core::parse_s3_error_code;
use crate::core::*;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

pub struct S3Deleter {
    core: Arc<S3Core>,
    ctx: OperationContext,
}

impl S3Deleter {
    pub fn new(core: Arc<S3Core>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::BatchDelete for S3Deleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        // This would delete the bucket, do not perform
        if self.core.root == "/" && path == "/" {
            return Ok(());
        }

        let error_ctx = ErrorContext::new(ServiceOperation("DeleteObject"))
            .with_caller_condition(args.is_conditional());
        let resp = self.core.s3_delete_object(&self.ctx, &path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(()),
            // Allow 404 when deleting a non-existing object.
            // This is not a standard behavior, only some s3 alike service like GCS XML API do this.
            // ref: <https://cloud.google.com/storage/docs/xml-api/delete-object>
            // Parse the response first so non-object 404 errors such as NoSuchBucket still surface.
            StatusCode::NOT_FOUND => {
                let err = parse_error(error_ctx, resp);
                if err.kind() != ErrorKind::NotFound {
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

    async fn delete_batch(&self, mut batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        // DeleteObjects errors identify only the key and version. Keep one entry per
        // identity in the native batch and send the remaining entries individually.
        let collisions = {
            let mut seen = HashSet::with_capacity(batch.len());
            batch
                .iter()
                .enumerate()
                .filter_map(|(index, (path, op))| {
                    (!seen.insert((path.as_str(), op.version()))).then_some(index)
                })
                .collect::<Vec<_>>()
        };
        let mut individual = Vec::with_capacity(collisions.len());
        for index in collisions.into_iter().rev() {
            individual.push(batch.swap_remove(index));
        }

        let mut result = if batch.len() == 1 {
            individual.append(&mut batch);
            BatchDeleteResult::default()
        } else {
            self.delete_distinct_batch(batch).await?
        };

        for (path, op) in individual {
            // Retain the native batch's progress even if this request fails.
            match self.delete_once(path.clone(), op.clone()).await {
                Ok(()) => result.succeeded.push((path, op)),
                Err(err) => result.failed.push((path, op, err)),
            }
        }
        Ok(result)
    }
}

impl S3Deleter {
    async fn delete_distinct_batch(
        &self,
        batch: Vec<(String, OpDelete)>,
    ) -> Result<BatchDeleteResult> {
        let resp = self.core.s3_delete_objects(&self.ctx, &batch).await?;

        let status = resp.status();
        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("DeleteObjects")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let result: DeleteObjectsResult =
            quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        let mut errors = result.error;
        let mut batched_result = BatchDeleteResult {
            // `errors.len()` is server-controlled and may exceed `batch.len()`; use
            // `saturating_sub` (mirroring services/tos) to avoid an unsigned underflow
            // that wraps to a huge `with_capacity` (release: abort).
            succeeded: Vec::with_capacity(batch.len().saturating_sub(errors.len())),
            failed: Vec::with_capacity(errors.len()),
        };
        for (path, op) in batch {
            let abs_path = build_absolute_path(&self.core.root, &path);
            // Assume errors are rare, so lookup and erase is acceptable.
            if let Some(idx) = errors
                .iter()
                .position(|e| e.key == abs_path && e.version_id.as_deref() == op.version())
            {
                let error = errors.swap_remove(idx);
                let conditional = op.if_match().is_some();
                batched_result.failed.push((
                    path,
                    op,
                    parse_delete_objects_result_error(error, conditional),
                ));
            } else {
                batched_result.succeeded.push((path, op));
            }
        }

        Ok(batched_result)
    }
}

fn parse_delete_objects_result_error(err: DeleteObjectsResultError, conditional: bool) -> Error {
    let error_ctx =
        ErrorContext::new(ServiceOperation("DeleteObjects")).with_caller_condition(conditional);
    let (kind, retryable) =
        parse_s3_error_code(error_ctx, err.code.as_str()).unwrap_or((ErrorKind::Unexpected, false));
    let kind = if conditional && kind == ErrorKind::NotFound {
        ErrorKind::ConditionNotMatch
    } else {
        kind
    };
    let mut err: Error = Error::new(kind, format!("{err:?}"));
    if retryable {
        err = err.set_temporary();
    }
    err
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use http::Method;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::*;

    use crate::backend::S3Builder;

    const NO_SUCH_BUCKET_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
  <BucketName>missing-bucket</BucketName>
</Error>"#;

    const NO_SUCH_KEY_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Key>missing-key</Key>
</Error>"#;

    #[derive(Clone)]
    struct StaticDeleteTransport {
        status: StatusCode,
        body: &'static str,
        requests: Arc<AtomicUsize>,
    }

    impl StaticDeleteTransport {
        fn new(status: StatusCode, body: &'static str) -> Self {
            Self {
                status,
                body,
                requests: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl HttpTransport for StaticDeleteTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            assert_eq!(req.method(), Method::DELETE);
            self.requests.fetch_add(1, Ordering::Relaxed);

            let body = Buffer::from(Bytes::from_static(self.body.as_bytes()));
            let size = body.len() as u64;
            Ok(Response::builder()
                .status(self.status)
                .body(HttpBody::new(
                    futures::stream::iter(vec![Ok(body)]),
                    Some(size),
                ))
                .expect("mock response must build"))
        }
    }

    fn operator_with_response(
        status: StatusCode,
        body: &'static str,
    ) -> (Operator, Arc<AtomicUsize>) {
        let transport = StaticDeleteTransport::new(status, body);
        let requests = transport.requests.clone();
        let op = Operator::new(
            S3Builder::default()
                .bucket("missing-bucket")
                .region("us-east-1")
                .skip_signature()
                .disable_config_load()
                .disable_ec2_metadata(),
        )
        .expect("S3 operator must build")
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)));
        (op, requests)
    }

    #[tokio::test]
    async fn delete_once_surfaces_no_such_bucket_as_config_invalid() {
        let (op, requests) = operator_with_response(StatusCode::NOT_FOUND, NO_SUCH_BUCKET_XML);

        let err = op
            .delete("object")
            .await
            .expect_err("NoSuchBucket must not be masked as Ok");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(!err.is_temporary());

        let err = op
            .delete_with("object")
            .if_match("\"etag\"")
            .await
            .expect_err("NoSuchBucket must not be masked as ConditionNotMatch");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(!err.is_temporary());

        assert_eq!(requests.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn delete_once_handles_not_found_object_bodies() {
        for body in [NO_SUCH_KEY_XML, "", "<html>404 Not Found</html>"] {
            let (op, requests) = operator_with_response(StatusCode::NOT_FOUND, body);

            op.delete("missing")
                .await
                .expect("unconditional 404 for missing object should succeed");

            let err = op
                .delete_with("missing")
                .if_match("\"etag\"")
                .await
                .expect_err("conditional 404 for missing object must fail");
            assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
            assert!(!err.is_temporary());
            assert!(err.source().is_some());

            assert_eq!(requests.load(Ordering::Relaxed), 2);
        }
    }

    #[tokio::test]
    async fn delete_once_succeeds_on_no_content() {
        let (op, requests) = operator_with_response(StatusCode::NO_CONTENT, "");

        op.delete("object")
            .await
            .expect("204 delete should succeed");
        op.delete_with("object")
            .if_match("\"etag\"")
            .await
            .expect("conditional 204 delete should succeed");

        assert_eq!(requests.load(Ordering::Relaxed), 2);
    }
}
