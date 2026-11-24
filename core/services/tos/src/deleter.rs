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

use bytes::Buf;
use http::StatusCode;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

use crate::core::parse_tos_error_code;
use crate::core::*;

pub struct TosDeleter {
    core: Arc<TosCore>,
    ctx: OperationContext,
}

impl TosDeleter {
    pub fn new(core: Arc<TosCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::BatchDelete for TosDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        if self.core.root == "/" && path == "/" {
            return Ok(());
        }

        let resp = self.core.tos_delete_object(&self.ctx, &path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => {
                let err = crate::core::parse_error(
                    crate::core::ErrorContext::new(ServiceOperation("DeleteObject")),
                    resp,
                );
                if err.kind() == ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(err)
                }
            }
            _ => Err(crate::core::parse_error(
                crate::core::ErrorContext::new(ServiceOperation("DeleteObject")),
                resp,
            )),
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let resp = self.core.tos_delete_objects(&self.ctx, &batch).await?;

        let status = resp.status();
        if status != StatusCode::OK {
            return Err(crate::core::parse_error(
                crate::core::ErrorContext::new(ServiceOperation("DeleteMultipleObjects")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let result: DeleteObjectsResult =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let mut deleted = result.deleted;
        let mut errors = result.errors;
        let mut batched_result = BatchDeleteResult {
            succeeded: Vec::with_capacity(batch.len().saturating_sub(errors.len())),
            failed: Vec::with_capacity(errors.len()),
        };
        for (path, op) in batch {
            let abs_path = build_absolute_path(&self.core.root, &path);
            let op_version = op.version().filter(|v| !v.is_empty());
            if let Some(idx) = errors.iter().position(|e| {
                e.key == abs_path && e.version_id.as_deref().filter(|v| !v.is_empty()) == op_version
            }) {
                let error = errors.swap_remove(idx);
                batched_result
                    .failed
                    .push((path, op, parse_delete_objects_result_error(error)));
            } else if let Some(idx) = deleted.iter().position(|e| {
                e.key == abs_path
                    && (op_version.is_none()
                        || e.version_id.as_deref().filter(|v| !v.is_empty()) == op_version)
            }) {
                let deleted = deleted.swap_remove(idx);
                let op = if op_version.is_some() {
                    if let Some(version) = deleted.version_id.as_deref().filter(|v| !v.is_empty()) {
                        op.into_version(version)
                    } else {
                        op
                    }
                } else {
                    op
                };
                batched_result.succeeded.push((path, op));
            } else {
                batched_result.succeeded.push((path, op));
            }
        }

        Ok(batched_result)
    }
}

fn parse_delete_objects_result_error(err: DeleteObjectsResultError) -> Error {
    let (kind, retryable) =
        parse_tos_error_code(err.code.as_str()).unwrap_or((ErrorKind::Unexpected, false));
    let mut err = Error::new(kind, format!("{err:?}"));
    if retryable {
        err = err.set_temporary();
    }
    err
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response};
    use opendal_core::raw::oio::BatchDelete;

    use super::*;

    struct StaticTransport {
        status: StatusCode,
        body: String,
    }

    impl HttpTransport for StaticTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let len = self.body.len() as u64;
            Response::builder()
                .status(self.status)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(self.body.clone()))]),
                    Some(len),
                ))
                .map_err(new_request_build_error)
        }
    }

    fn build_core() -> Arc<TosCore> {
        let backend = crate::Tos::default()
            .bucket("test-bucket")
            .skip_signature()
            .build()
            .expect("build must succeed");
        let backend = (&backend as &dyn std::any::Any)
            .downcast_ref::<crate::backend::TosBackend>()
            .expect("build must return TosBackend");
        backend.core.clone()
    }

    #[tokio::test]
    async fn test_delete_batch_matches_empty_version_id_error_and_classifies_access_denied() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let body = r#"{
                "Deleted": [
                    {"Key": "ok.txt", "VersionId": ""}
                ],
                "Error": [
                    {"Key": "denied.txt", "VersionId": "", "Code": "AccessDenied", "Message": "Access Denied"},
                    {"Key": "throttled.txt", "VersionId": "", "Code": "SlowDown", "Message": "Please reduce your request rate"}
                ]
            }"#;
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(
                StaticTransport {
                    status: StatusCode::OK,
                    body: body.to_string(),
                },
            ));
            let deleter = TosDeleter::new(build_core(), ctx);

            let res = deleter
                .delete_batch(vec![
                    ("ok.txt".to_string(), OpDelete::default()),
                    ("denied.txt".to_string(), OpDelete::default()),
                    ("throttled.txt".to_string(), OpDelete::default()),
                ])
                .await
                .unwrap();

            assert_eq!(res.succeeded.len(), 1);
            assert_eq!(res.succeeded[0].0, "ok.txt");

            assert_eq!(res.failed.len(), 2);
            assert_eq!(res.failed[0].0, "denied.txt");
            assert_eq!(res.failed[0].2.kind(), ErrorKind::PermissionDenied);
            assert!(!res.failed[0].2.is_temporary());

            assert_eq!(res.failed[1].0, "throttled.txt");
            assert_eq!(res.failed[1].2.kind(), ErrorKind::RateLimited);
            assert!(res.failed[1].2.is_temporary());
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_delete_batch_distinguishes_versions() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let body = r#"{
                "Deleted": [
                    {"Key": "a.txt", "VersionId": "v2"}
                ],
                "Error": [
                    {"Key": "a.txt", "VersionId": "v1", "Code": "AccessDenied", "Message": "Access Denied"}
                ]
            }"#;
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(
                StaticTransport {
                    status: StatusCode::OK,
                    body: body.to_string(),
                },
            ));
            let deleter = TosDeleter::new(build_core(), ctx);

            let res = deleter
                .delete_batch(vec![
                    ("a.txt".to_string(), OpDelete::new().into_version("v1")),
                    ("a.txt".to_string(), OpDelete::new().into_version("v2")),
                ])
                .await
                .unwrap();

            assert_eq!(res.succeeded.len(), 1);
            assert_eq!(res.succeeded[0].0, "a.txt");
            assert_eq!(res.succeeded[0].1.version(), Some("v2"));

            assert_eq!(res.failed.len(), 1);
            assert_eq!(res.failed[0].0, "a.txt");
            assert_eq!(res.failed[0].1.version(), Some("v1"));
            assert_eq!(res.failed[0].2.kind(), ErrorKind::PermissionDenied);
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_delete_once_404_surfaces_no_such_bucket_and_ignores_not_found() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = build_core();

            let json_no_such_bucket =
                r#"{"Code":"NoSuchBucket","Message":"The specified bucket does not exist"}"#;
            let xml_no_such_bucket = r#"<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>"#;

            for body in [json_no_such_bucket, xml_no_such_bucket] {
                let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(
                    StaticTransport {
                        status: StatusCode::NOT_FOUND,
                        body: body.to_string(),
                    },
                ));
                let deleter = TosDeleter::new(core.clone(), ctx);
                let err = deleter
                    .delete_once("a.txt".to_string(), OpDelete::default())
                    .await
                    .expect_err("404 NoSuchBucket must not be swallowed");
                assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
            }

            let json_no_such_key =
                r#"{"Code":"NoSuchKey","Message":"The specified key does not exist"}"#;
            for body in [json_no_such_key, ""] {
                let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(
                    StaticTransport {
                        status: StatusCode::NOT_FOUND,
                        body: body.to_string(),
                    },
                ));
                let deleter = TosDeleter::new(core.clone(), ctx);
                deleter
                    .delete_once("a.txt".to_string(), OpDelete::default())
                    .await
                    .expect("404 NoSuchKey or empty 404 must succeed idempotently");
            }
        })
        .await
        .expect("test timed out");
    }
}
