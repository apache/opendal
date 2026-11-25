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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use http::StatusCode;
use uuid::Uuid;

use super::core::AzblobCore;
use super::core::ErrorContext;
use super::core::constants::X_MS_VERSION_ID;
use super::core::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

const X_MS_BLOB_TYPE: &str = "x-ms-blob-type";

pub type AzblobWriters = TwoWays<oio::BlockWriter<AzblobWriter>, oio::AppendWriter<AzblobWriter>>;

pub struct AzblobWriter {
    core: Arc<AzblobCore>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
    conditions_checked: AtomicBool,
}

impl AzblobWriter {
    pub fn new(core: Arc<AzblobCore>, ctx: OperationContext, op: OpWrite, path: String) -> Self {
        AzblobWriter {
            core,
            ctx,
            op,
            path,
            conditions_checked: AtomicBool::new(false),
        }
    }

    // skip extracting `content-md5` here, as it pertains to the content of the request rather than
    // the content of the block itself for the `append` and `complete put block list` operations.
    pub(crate) fn parse_metadata(headers: &http::HeaderMap) -> Result<Metadata> {
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

    fn error_context(&self, service_operation: ServiceOperation) -> ErrorContext {
        ErrorContext::new(service_operation)
            .with_caller_condition(self.op.is_conditional())
            .with_if_not_exists(self.op.if_not_exists())
    }
}

impl oio::AppendWrite for AzblobWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .azblob_get_blob_properties(&self.ctx, &self.path, &OpStat::default())
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                if self.op.if_not_exists() {
                    return Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "the blob already exists",
                    ));
                }
                let headers = resp.headers();
                let blob_type = headers.get(X_MS_BLOB_TYPE).and_then(|v| v.to_str().ok());
                if blob_type != Some("AppendBlob") {
                    return Err(Error::new(
                        ErrorKind::Conflict,
                        "the blob is not an appendable blob.",
                    ));
                }

                Ok(parse_content_length(headers)?.unwrap_or_default())
            }
            StatusCode::NOT_FOUND => {
                let err = parse_error(
                    ErrorContext::new(ServiceOperation("GetBlobProperties")),
                    resp,
                );
                if err.kind() != ErrorKind::NotFound {
                    return Err(err);
                }
                if self.op.if_match().is_some() {
                    return Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "write precondition requires a live target",
                    )
                    .set_source(err));
                }

                let resp = self
                    .core
                    .azblob_init_appendable_blob(&self.ctx, &self.path, &self.op)
                    .await?;

                let status = resp.status();
                match status {
                    StatusCode::CREATED => {
                        self.conditions_checked.store(true, Ordering::Relaxed);
                    }
                    _ => {
                        return Err(parse_error(
                            self.error_context(ServiceOperation("PutBlob"))
                                .with_append_blob_initialization(true),
                            resp,
                        ));
                    }
                }
                Ok(0)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("GetBlobProperties")),
                resp,
            )),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        let include_conditions = !self.conditions_checked.load(Ordering::Relaxed);

        // Azure rejects `Append Block` with an empty body, and an empty append
        // changes nothing, so read the blob properties instead of appending.
        if size == 0 {
            let stat_args = if include_conditions {
                OpStat::from(options::StatOptions {
                    if_match: self.op.if_match().map(str::to_owned),
                    if_none_match: self.op.if_none_match().map(str::to_owned),
                    ..Default::default()
                })
            } else {
                OpStat::default()
            };
            let error_ctx = if include_conditions {
                self.error_context(ServiceOperation("GetBlobProperties"))
            } else {
                ErrorContext::new(ServiceOperation("GetBlobProperties"))
            };

            let resp = self
                .core
                .azblob_get_blob_properties(&self.ctx, &self.path, &stat_args)
                .await?;

            let meta = AzblobWriter::parse_metadata(resp.headers())?;
            let status = resp.status();
            return match status {
                StatusCode::OK => {
                    self.conditions_checked.store(true, Ordering::Relaxed);
                    Ok(meta)
                }
                _ => {
                    let err = parse_error(error_ctx, resp);
                    if include_conditions
                        && self.op.if_match().is_some()
                        && err.kind() == ErrorKind::NotFound
                    {
                        Err(Error::new(
                            ErrorKind::ConditionNotMatch,
                            "write precondition requires a live target",
                        )
                        .set_source(err))
                    } else {
                        Err(err)
                    }
                }
            };
        }

        let resp = self
            .core
            .azblob_append_blob(
                &self.ctx,
                &self.path,
                offset,
                size,
                &self.op,
                include_conditions,
                body,
            )
            .await?;

        let meta = AzblobWriter::parse_metadata(resp.headers())?;
        let status = resp.status();
        match status {
            StatusCode::CREATED => {
                self.conditions_checked.store(true, Ordering::Relaxed);
                Ok(meta)
            }
            _ => {
                let error_ctx = if include_conditions {
                    self.error_context(ServiceOperation("AppendBlock"))
                } else {
                    ErrorContext::new(ServiceOperation("AppendBlock"))
                };
                let err = parse_error(error_ctx, resp);
                if include_conditions
                    && self.op.if_match().is_some()
                    && err.kind() == ErrorKind::NotFound
                {
                    Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "write precondition requires a live target",
                    )
                    .set_source(err))
                } else {
                    Err(err)
                }
            }
        }
    }
}

impl oio::BlockWrite for AzblobWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .azblob_put_blob(&self.ctx, &self.path, Some(size), &self.op, body)
            .await?;

        let status = resp.status();

        let mut meta = AzblobWriter::parse_metadata(resp.headers())?.into_builder();
        let md5 = parse_content_md5(resp.headers())?;
        if let Some(md5) = md5 {
            meta.content_md5(md5);
        }
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(meta.build()),
            _ => Err(parse_error(
                self.error_context(ServiceOperation("PutBlob")),
                resp,
            )),
        }
    }

    async fn write_block(&self, block_id: Uuid, size: u64, body: Buffer) -> Result<()> {
        let resp = self
            .core
            .azblob_put_block(&self.ctx, &self.path, block_id, Some(size), &self.op, body)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("PutBlock")),
                resp,
            )),
        }
    }

    async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<Metadata> {
        let resp = self
            .core
            .azblob_complete_put_block_list(&self.ctx, &self.path, block_ids, &self.op)
            .await?;

        let meta = AzblobWriter::parse_metadata(resp.headers())?;
        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(meta),
            _ => Err(parse_error(
                self.error_context(ServiceOperation("PutBlockList")),
                resp,
            )),
        }
    }

    async fn abort_block(&self, _block_ids: Vec<Uuid>) -> Result<()> {
        // refer to https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list?tabs=microsoft-entra-id
        // Any uncommitted blocks are garbage collected if there are no successful calls to Put Block or Put Block List on the blob within a week.
        // If Put Blob is called on the blob, any uncommitted blocks are garbage collected.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;

    use bytes::Bytes;
    use http::HeaderMap;
    use http::Method;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use http::header::IF_MATCH;
    use http::header::IF_NONE_MATCH;
    use opendal_core::*;

    use crate::backend::AzblobBuilder;

    #[derive(Clone)]
    struct RecordedRequest {
        method: Method,
        uri: String,
        headers: HeaderMap,
    }

    #[derive(Clone)]
    struct ScriptedTransport {
        responses: Arc<Mutex<VecDeque<Response<Bytes>>>>,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl ScriptedTransport {
        fn new(responses: Vec<Response<Bytes>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(VecDeque::from(responses))),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpTransport for ScriptedTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.lock().unwrap().push(RecordedRequest {
                method: req.method().clone(),
                uri: req.uri().to_string(),
                headers: req.headers().clone(),
            });

            let resp = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected HTTP request");
            let (parts, body) = resp.into_parts();
            let size = body.len() as u64;
            let stream_body = if body.is_empty() {
                HttpBody::new(futures::stream::iter(std::iter::empty()), Some(0))
            } else {
                HttpBody::new(
                    futures::stream::iter(vec![Ok(Buffer::from(body))]),
                    Some(size),
                )
            };
            Ok(Response::from_parts(parts, stream_body))
        }
    }

    fn test_operator(transport: ScriptedTransport) -> Operator {
        Operator::new(
            AzblobBuilder::default()
                .container("test")
                .endpoint("https://myaccount.blob.core.windows.net")
                .skip_signature(),
        )
        .expect("azblob operator must build")
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)))
    }

    #[tokio::test]
    async fn append_with_if_match_rejects_missing_blob_without_initializing() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("x-ms-error-code", "BlobNotFound")
                .body(Bytes::new())
                .unwrap(),
        ]);
        let requests = transport.requests.clone();
        let op = test_operator(transport);

        let err = op
            .write_with("missing.log", "hello")
            .append(true)
            .if_match("\"etag-1\"")
            .await
            .expect_err("append with if_match on missing blob must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].method, Method::HEAD);
    }

    #[tokio::test]
    async fn append_sends_conditions_only_on_first_chunk_and_checks_empty_append() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::OK)
                .header("x-ms-blob-type", "AppendBlob")
                .header("content-length", "3")
                .header("etag", "\"etag-1\"")
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::CREATED)
                .header("etag", "\"etag-2\"")
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::CREATED)
                .header("etag", "\"etag-3\"")
                .body(Bytes::new())
                .unwrap(),
        ]);
        let requests = transport.requests.clone();
        let op = test_operator(transport);

        let mut writer = op
            .writer_with("existing.log")
            .append(true)
            .if_match("\"etag-1\"")
            .await
            .unwrap();
        writer.write("hello").await.unwrap();
        writer.write("world").await.unwrap();
        writer.close().await.unwrap();

        {
            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 3);
            assert_eq!(recorded[0].method, Method::HEAD);
            assert_eq!(recorded[1].method, Method::PUT);
            assert!(recorded[1].uri.contains("comp=appendblock"));
            assert_eq!(
                recorded[1].headers.get(IF_MATCH).map(|v| v.as_bytes()),
                Some(b"\"etag-1\"".as_slice())
            );
            assert_eq!(recorded[2].method, Method::PUT);
            assert!(recorded[2].uri.contains("comp=appendblock"));
            assert!(recorded[2].headers.get(IF_MATCH).is_none());
        }

        // Empty append against an existing blob must still evaluate if_match.
        let empty_transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::OK)
                .header("x-ms-blob-type", "AppendBlob")
                .header("content-length", "3")
                .header("etag", "\"etag-1\"")
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .header("x-ms-error-code", "ConditionNotMet")
                .body(Bytes::new())
                .unwrap(),
        ]);
        let empty_requests = empty_transport.requests.clone();
        let op = test_operator(empty_transport);

        let err = op
            .write_with("existing.log", "")
            .append(true)
            .if_match("\"wrong-etag\"")
            .await
            .expect_err("empty append with wrong if_match must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let recorded = empty_requests.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert_eq!(
            recorded[1].headers.get(IF_MATCH).map(|v| v.as_bytes()),
            Some(b"\"wrong-etag\"".as_slice())
        );
    }

    #[tokio::test]
    async fn append_init_carries_user_metadata_and_does_not_repeat_if_not_exists_on_append_block() {
        let transport = ScriptedTransport::new(vec![
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("x-ms-error-code", "BlobNotFound")
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::CREATED)
                .header("etag", "\"etag-0\"")
                .body(Bytes::new())
                .unwrap(),
            Response::builder()
                .status(StatusCode::CREATED)
                .header("etag", "\"etag-1\"")
                .body(Bytes::new())
                .unwrap(),
        ]);
        let requests = transport.requests.clone();
        let op = test_operator(transport);

        op.write_with("new.log", "hello")
            .append(true)
            .if_not_exists(true)
            .user_metadata([("env".to_string(), "prod".to_string())])
            .await
            .expect("conditional append creation must succeed");

        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        assert_eq!(recorded[0].method, Method::HEAD);
        assert_eq!(recorded[1].method, Method::PUT);
        assert_eq!(
            recorded[1].headers.get(IF_NONE_MATCH).map(|v| v.as_bytes()),
            Some(b"*".as_slice())
        );
        assert_eq!(
            recorded[1]
                .headers
                .get("x-ms-meta-env")
                .map(|v| v.as_bytes()),
            Some(b"prod".as_slice())
        );
        assert_eq!(recorded[2].method, Method::PUT);
        assert!(recorded[2].uri.contains("comp=appendblock"));
        assert!(recorded[2].headers.get(IF_NONE_MATCH).is_none());
    }
}
