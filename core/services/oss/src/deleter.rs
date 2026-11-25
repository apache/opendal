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

use super::core::parse_error;
use super::core::*;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

pub struct OssDeleter {
    core: Arc<OssCore>,
    ctx: OperationContext,
}

impl OssDeleter {
    pub fn new(core: Arc<OssCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::BatchDelete for OssDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let error_ctx = ErrorContext::new(ServiceOperation("DeleteObject"));
        let resp = self.core.oss_delete_object(&self.ctx, &path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(()),
            // Parse 404 responses first so non-object 404 errors such as NoSuchBucket still surface.
            StatusCode::NOT_FOUND => {
                let err = parse_error(error_ctx, resp);
                if err.kind() == ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(err)
                }
            }
            _ => Err(parse_error(error_ctx, resp)),
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        // Sadly, OSS will not return failed keys, so we will build
        // a set to calculate the failed keys.
        let mut keys: HashSet<(String, OpDelete)> = batch
            .iter()
            .map(|path| (path.0.to_owned(), path.1.clone()))
            .collect();

        let resp = self.core.oss_delete_objects(&self.ctx, batch).await?;

        let status = resp.status();

        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("DeleteMultipleObjects")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let result: DeleteObjectsResult =
            quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        if result.deleted.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "oss delete this key failed for reason we don't know",
            ));
        }

        let mut batched_result = BatchDeleteResult {
            succeeded: Vec::with_capacity(result.deleted.len().min(keys.len())),
            // `result.deleted.len()` is server-controlled and may exceed `keys.len()`; use
            // `saturating_sub` (mirroring services/tos) to avoid an unsigned underflow that
            // wraps to a huge `with_capacity` (release: abort).
            failed: Vec::with_capacity(keys.len().saturating_sub(result.deleted.len())),
        };

        for i in result.deleted {
            let rel = if i.key.starts_with('/') {
                i.key.strip_prefix(&self.core.root)
            } else {
                i.key.strip_prefix(&self.core.root[1..])
            };
            let Some(rel) = rel else {
                continue;
            };
            let path = if rel.is_empty() {
                "/".to_string()
            } else {
                rel.to_string()
            };
            let op = match i.version_id {
                Some(version) => OpDelete::new().into_version(&version),
                None => OpDelete::new(),
            };
            let object = (path, op);
            if keys.remove(&object) {
                batched_result.succeeded.push(object);
            }
        }
        // TODO: we should handle those errors with code.
        for (path, op) in keys {
            batched_result.failed.push((
                path,
                op,
                Error::new(
                    ErrorKind::Unexpected,
                    "oss delete this key failed for reason we don't know",
                ),
            ));
        }

        Ok(batched_result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;

    use bytes::Bytes;
    use futures::stream;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::*;

    use crate::backend::OssBuilder;

    #[derive(Clone)]
    struct ScriptedDeleteTransport {
        responses: Arc<Mutex<VecDeque<(StatusCode, &'static str)>>>,
        bodies: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedDeleteTransport {
        fn new(responses: Vec<(StatusCode, &'static str)>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into())),
                bodies: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpTransport for ScriptedDeleteTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let req_body = String::from_utf8_lossy(&req.into_body().to_bytes()).into_owned();
            self.bodies.lock().unwrap().push(req_body);

            let (status, body) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra delete request");
            let buf = Buffer::from(Bytes::from_static(body.as_bytes()));
            let size = buf.len() as u64;
            Ok(Response::builder()
                .status(status)
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(size)))
                .expect("mock response must build"))
        }
    }

    fn operator_with_responses(
        root: &str,
        responses: Vec<(StatusCode, &'static str)>,
    ) -> (Operator, Arc<Mutex<Vec<String>>>) {
        let transport = ScriptedDeleteTransport::new(responses);
        let bodies = transport.bodies.clone();
        let op = Operator::new(
            OssBuilder::default()
                .bucket("example")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .root(root)
                .skip_signature()
                .disable_config_load(),
        )
        .expect("OSS operator must build")
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)));
        (op, bodies)
    }

    #[tokio::test]
    async fn delete_once_surfaces_no_such_bucket_and_ignores_missing_object() {
        let no_such_bucket = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist.</Message>
</Error>"#;
        let no_such_key = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
</Error>"#;

        let (op, _) = operator_with_responses(
            "/",
            vec![
                (StatusCode::NOT_FOUND, no_such_bucket),
                (StatusCode::NOT_FOUND, no_such_key),
                (StatusCode::NOT_FOUND, ""),
                (StatusCode::NO_CONTENT, ""),
            ],
        );

        let err = op
            .delete("a.txt")
            .await
            .expect_err("404 NoSuchBucket must not be swallowed");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(!err.is_temporary());

        op.delete("a.txt")
            .await
            .expect("404 NoSuchKey should succeed");
        op.delete("a.txt").await.expect("empty 404 should succeed");
        op.delete("a.txt")
            .await
            .expect("204 No Content should succeed");
    }

    #[tokio::test]
    async fn delete_batch_handles_root_under_non_root_prefix_and_unmatched_entries() {
        let response = r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Deleted><Key>prefix/</Key></Deleted>
  <Deleted><Key>prefix/a.txt</Key></Deleted>
  <Deleted><Key>other/unrelated.txt</Key></Deleted>
</DeleteResult>"#;

        let (op, bodies) = operator_with_responses("/prefix/", vec![(StatusCode::OK, response)]);

        let mut deleter = op.deleter().await.unwrap();
        deleter.delete("/").await.unwrap();
        deleter.delete("a.txt").await.unwrap();
        deleter
            .close()
            .await
            .expect("batch delete with root under non-root prefix must succeed");

        let recorded = bodies.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert!(recorded[0].contains("<Key>prefix/</Key>"));
        assert!(recorded[0].contains("<Key>prefix/a.txt</Key>"));
    }
}
