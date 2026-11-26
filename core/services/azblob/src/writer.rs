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
                let err = parse_error(
                    error_ctx.with_request_crc64(self.op.checksum().is_some()),
                    resp,
                );
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
                self.error_context(ServiceOperation("PutBlob"))
                    .with_request_crc64(self.op.checksum().is_some()),
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
                ErrorContext::new(ServiceOperation("PutBlock"))
                    .with_request_crc64(self.op.checksum().is_some()),
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
    use std::time::Duration;

    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use bytes::Bytes;
    use http::HeaderMap;
    use http::Method;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use http::header::IF_MATCH;
    use http::header::IF_NONE_MATCH;
    use opendal_core::raw::percent_decode_path;
    use opendal_core::*;
    use opendal_executor_tokio::TokioExecutor;
    use opendal_layer_retry::RetryLayer;
    use reqsign_azure_storage::Credential;
    use reqsign_core::Context;
    use reqsign_core::ProvideCredential;
    use reqsign_core::ProvideCredentialChain;

    use crate::backend::AzblobBuilder;
    use crate::core::constants::X_MS_BLOB_CONDITION_APPENDPOS;
    use crate::core::constants::X_MS_BLOB_TYPE;
    use crate::core::constants::X_MS_CONTENT_CRC64;
    use crate::core::constants::X_MS_VERSION_ID;

    #[derive(Clone)]
    struct RecordedRequest {
        method: Method,
        uri: String,
        headers: HeaderMap,
        body: Bytes,
        response_status: StatusCode,
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
            let resp = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected HTTP request");
            self.requests.lock().unwrap().push(RecordedRequest {
                method: req.method().clone(),
                uri: req.uri().to_string(),
                headers: req.headers().clone(),
                body: req.body().to_bytes(),
                response_status: resp.status(),
            });

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
        .with_context(
            OperationContext::new()
                .with_http_transport(HttpTransporter::new(transport))
                .with_executor(Executor::with(TokioExecutor::default())),
        )
    }

    fn retry_once() -> RetryLayer {
        RetryLayer::new()
            .with_min_delay(Duration::from_millis(1))
            .with_max_times(1)
    }

    fn crc64_mismatch() -> Response<Bytes> {
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Bytes::from_static(
                b"<Error><Code>Crc64Mismatch</Code></Error>",
            ))
            .unwrap()
    }

    fn created() -> http::response::Builder {
        Response::builder().status(StatusCode::CREATED)
    }

    #[derive(Debug)]
    struct UnexpectedCredentialRequest;

    impl ProvideCredential for UnexpectedCredentialRequest {
        type Credential = Credential;

        async fn provide_credential(
            &self,
            _: &Context,
        ) -> reqsign_core::Result<Option<Credential>> {
            panic!("rejected checksum intent must not attempt signing");
        }
    }

    #[tokio::test]
    async fn checksum_intent_rejection_precedes_signing_and_transport() {
        let transport = ScriptedTransport::new(vec![]);
        let op = Operator::new(
            AzblobBuilder::default()
                .container("test")
                .endpoint("https://account1.blob.core.windows.net")
                .account_key(&BASE64_STANDARD.encode([42; 64]))
                .credential_provider_chain(
                    ProvideCredentialChain::new().push(UnexpectedCredentialRequest),
                ),
        )
        .unwrap()
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)));
        assert!(op.info().capability().write_with_checksum);
        assert!(op.info().capability().presign_write);

        let algorithm = WriteChecksum::Algo(ChecksumAlgo::Crc64Nvme);
        let expected = WriteChecksum::Expected(Checksum::new(ChecksumAlgo::Crc64Nvme, [0; 8]));
        for checksum in [algorithm.clone(), expected.clone()] {
            let result = op.writer_with("file").checksum(checksum).await;
            assert_eq!(result.err().unwrap().kind(), ErrorKind::Unsupported);
        }
        for checksum in [WriteChecksum::Any, WriteChecksum::Fast, algorithm, expected] {
            let err = op
                .presign_write_options(
                    "file",
                    Duration::from_secs(60),
                    options::WriteOptions {
                        checksum: Some(checksum),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unsupported);
        }
    }

    #[tokio::test]
    async fn put_blob_checksums_cover_segmented_and_empty_payloads_only_on_request() {
        let vector_crc = BASE64_STANDARD.encode(0xae8b_1486_0a79_9888_u64.to_le_bytes());
        let transport = ScriptedTransport::new(vec![
            created()
                .header(X_MS_CONTENT_CRC64, &vector_crc)
                .header("content-md5", "provider-md5")
                .body(Bytes::new())
                .unwrap(),
            crc64_mismatch(),
            created()
                .header(X_MS_CONTENT_CRC64, &vector_crc)
                .header("etag", "\"new\"")
                .header(X_MS_VERSION_ID, "version")
                .body(Bytes::new())
                .unwrap(),
            created().body(Bytes::new()).unwrap(),
        ]);
        let requests = transport.requests.clone();
        let op = test_operator(transport).layer(retry_once());

        let meta = op.write("file", "123456789").await.unwrap();
        assert_eq!(meta.content_md5(), Some("provider-md5"));
        assert!(meta.checksum().is_none());

        let body = Buffer::from(vec![
            Bytes::from_static(b"1234"),
            Bytes::from_static(b"56789"),
        ]);
        let meta = op
            .write_with("file", body)
            .checksum(WriteChecksum::Any)
            .if_match("\"old\"")
            .await
            .unwrap();
        assert_eq!(meta.etag(), Some("\"new\""));
        assert_eq!(meta.version(), Some("version"));
        assert!(meta.checksum().is_none());

        let meta = op
            .write_with("empty", Buffer::new())
            .checksum(WriteChecksum::Fast)
            .await
            .unwrap();
        assert!(meta.checksum().is_none());

        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 4);
        assert!(!recorded[0].headers.contains_key(X_MS_CONTENT_CRC64));
        for request in &recorded[1..3] {
            assert_eq!(request.headers[X_MS_CONTENT_CRC64], vector_crc);
            assert_eq!(request.headers[IF_MATCH], "\"old\"");
            assert_eq!(&request.body[..], b"123456789");
        }
        assert_eq!(recorded[3].headers[X_MS_CONTENT_CRC64], "AAAAAAAAAAA=");
        assert!(recorded[3].body.is_empty());
    }

    #[tokio::test]
    async fn staged_checksum_retry_keeps_block_id_and_excludes_commit_xml() {
        let vector_crc = BASE64_STANDARD.encode(0xae8b_1486_0a79_9888_u64.to_le_bytes());
        for commit in [
            created()
                .header(X_MS_CONTENT_CRC64, "AAAAAAAAAAA=")
                .body(Bytes::new())
                .unwrap(),
            crc64_mismatch(),
        ] {
            let commit_status = commit.status();
            let transport = ScriptedTransport::new(vec![
                crc64_mismatch(),
                created().body(Bytes::new()).unwrap(),
                created().body(Bytes::new()).unwrap(),
                commit,
            ]);
            let requests = transport.requests.clone();
            let op = test_operator(transport).layer(retry_once());
            let mut writer = op
                .writer_with("blocks")
                .concurrent(2)
                .checksum(WriteChecksum::Fast)
                .if_not_exists(true)
                .await
                .unwrap();
            writer.write("123456789").await.unwrap();
            writer.write("a").await.unwrap();
            let result = writer.close().await;
            if commit_status == StatusCode::CREATED {
                assert!(result.unwrap().checksum().is_none());
            } else {
                let err = result.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert!(!err.is_temporary());
                writer.abort().await.unwrap();
            }

            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 4);
            let blocks = &recorded[..3];
            let failed = blocks
                .iter()
                .find(|req| req.response_status == StatusCode::BAD_REQUEST)
                .unwrap();
            let matches: Vec<_> = blocks.iter().filter(|req| req.uri == failed.uri).collect();
            assert_eq!(matches.len(), 2);
            assert_eq!(matches[0].body, matches[1].body);
            let block_ids: Vec<_> = [b"123456789".as_slice(), b"a".as_slice()]
                .into_iter()
                .zip([vector_crc.as_str(), "PPzLtEWEL4w="])
                .map(|(data, crc)| {
                    let mut requests = blocks.iter().filter(|req| req.body == data);
                    let request = requests.next().unwrap();
                    for req in std::iter::once(request).chain(requests) {
                        assert_eq!(req.uri, request.uri);
                        assert_eq!(req.headers[X_MS_CONTENT_CRC64], crc);
                    }
                    percent_decode_path(request.uri.split_once("?comp=block&blockid=").unwrap().1)
                })
                .collect();
            assert_ne!(block_ids[0], block_ids[1]);
            let commit = &recorded[3];
            assert!(commit.uri.ends_with("?comp=blocklist"));
            assert!(!commit.headers.contains_key(X_MS_CONTENT_CRC64));
            assert_eq!(commit.headers[IF_NONE_MATCH], "*");
            let xml = std::str::from_utf8(&commit.body).unwrap();
            assert!(xml.find(&block_ids[0]).unwrap() < xml.find(&block_ids[1]).unwrap());
        }
    }

    #[tokio::test]
    async fn append_checksum_retry_keeps_committed_prefix_and_offset() {
        let vector_crc = BASE64_STANDARD.encode(0xae8b_1486_0a79_9888_u64.to_le_bytes());
        for (outcome, expected_error) in [
            (created().body(Bytes::new()).unwrap(), None),
            (crc64_mismatch(), Some(ErrorKind::ChecksumMismatch)),
            (
                Response::builder()
                    .status(StatusCode::PRECONDITION_FAILED)
                    .header("x-ms-error-code", "AppendPositionConditionNotMet")
                    .body(Bytes::new())
                    .unwrap(),
                Some(ErrorKind::Conflict),
            ),
        ] {
            let transport = ScriptedTransport::new(vec![
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("x-ms-error-code", "BlobNotFound")
                    .body(Bytes::new())
                    .unwrap(),
                created().body(Bytes::new()).unwrap(),
                created().body(Bytes::new()).unwrap(),
                crc64_mismatch(),
                outcome,
            ]);
            let requests = transport.requests.clone();
            let op = test_operator(transport).layer(retry_once());
            let mut writer = op
                .writer_with("append")
                .append(true)
                .if_not_exists(true)
                .checksum(WriteChecksum::Any)
                .await
                .unwrap();
            writer.write("a").await.unwrap();
            let result = writer.write("123456789").await;
            if let Some(kind) = expected_error {
                let err = result.unwrap_err();
                assert_eq!(err.kind(), kind);
                assert!(err.is_persistent());
                writer.abort().await.unwrap();
            } else {
                result.unwrap();
                let meta = writer.close().await.unwrap();
                assert_eq!(meta.content_length(), 10);
                assert!(meta.checksum().is_none());
            }

            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 5);
            assert_eq!(recorded[0].method, Method::HEAD);
            assert_eq!(recorded[1].headers[X_MS_BLOB_TYPE], "AppendBlob");
            assert_eq!(recorded[1].headers[IF_NONE_MATCH], "*");
            for request in &recorded[..2] {
                assert!(!request.headers.contains_key(X_MS_CONTENT_CRC64));
            }
            assert_eq!(recorded[2].headers[X_MS_BLOB_CONDITION_APPENDPOS], "0");
            assert_eq!(&recorded[2].body[..], b"a");
            for request in &recorded[3..] {
                assert!(request.uri.ends_with("?comp=appendblock"));
                assert_eq!(request.headers[X_MS_BLOB_CONDITION_APPENDPOS], "1");
                assert_eq!(request.headers[X_MS_CONTENT_CRC64], vector_crc);
                assert_eq!(&request.body[..], b"123456789");
            }
        }
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
            .checksum(WriteChecksum::Fast)
            .if_match("\"wrong-etag\"")
            .await
            .expect_err("empty append with wrong if_match must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let recorded = empty_requests.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        for request in recorded.iter() {
            assert_eq!(request.method, Method::HEAD);
            assert!(!request.headers.contains_key(X_MS_CONTENT_CRC64));
        }
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
