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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use http::{Request, Response, StatusCode};
use opendal_core::options::WriteOptions;
use opendal_core::{
    Buffer, Checksum, ChecksumAlgo, ErrorKind, HttpBody, HttpTransport, HttpTransporter,
    OperationContext, Operator, Result, WriteChecksum, Writer,
};
use opendal_layer_retry::RetryLayer;
use opendal_service_b2::B2;
use serde_json::{Value, json};

const ABC_SHA1: &str = "a9993e364706816aba3e25717850c26c9cd0d89d";
const ABC_DIGEST: [u8; 20] =
    *b"\xa9\x99\x3e\x36\x47\x06\x81\x6a\xba\x3e\x25\x71\x78\x50\xc2\x6c\x9c\xd0\xd8\x9d";
const PART_SIZE: usize = 5 * 1024 * 1024;
const PART_SHA1: &str = "61b8d6600ac94d912874f569a9341120f680c9f8";
const TAIL_SHA1: &str = "fbf5f2a2875b3bb65b8e3b23e6cc01d58ca30447";
const SHA1_HEADER: &str = "x-bz-content-sha1";
const UPLOAD_MISMATCH: &str = "Checksum did not match data received";

#[derive(Debug)]
enum UploadRequest {
    File(&'static str),
    Part(u8, &'static str),
    Finish,
    Cancel,
}

#[derive(Clone)]
struct UploadScript(Arc<Mutex<VecDeque<(UploadRequest, StatusCode, Value)>>>);

impl HttpTransport for UploadScript {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        let (status, body) = match req.uri().path() {
            "/b2api/v4/b2_authorize_account" => (
                StatusCode::OK,
                json!({"authorizationToken": "token", "apiInfo": {"storageApi": {
                    "apiUrl": "https://api.example", "downloadUrl": "https://download.example"
                }}}),
            ),
            "/b2api/v2/b2_get_upload_url" => (
                StatusCode::OK,
                json!({"uploadUrl": "https://upload.example/file", "authorizationToken": "token"}),
            ),
            "/b2api/v2/b2_start_large_file" => (StatusCode::OK, json!({"fileId": "large-file"})),
            "/b2api/v2/b2_get_upload_part_url" => {
                assert_eq!(req.uri().query(), Some("fileId=large-file"));
                (
                    StatusCode::OK,
                    json!({"uploadUrl": "https://upload.example/part", "authorizationToken": "token"}),
                )
            }
            path => {
                let (expected, status, body) = self
                    .0
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| panic!("unexpected request to {path}"));
                match expected {
                    UploadRequest::File(digest) => {
                        assert_eq!(path, "/file");
                        assert_eq!(req.headers()[SHA1_HEADER], digest);
                        assert_eq!(req.body().to_bytes().as_ref(), b"abc");
                    }
                    UploadRequest::Part(number, digest) => {
                        assert_eq!(path, "/part");
                        assert_eq!(req.headers()["x-bz-part-number"], number.to_string());
                        assert_eq!(req.headers()[SHA1_HEADER], digest);
                        let bytes = req.body().to_bytes();
                        if number == 1 {
                            assert_eq!(bytes.len(), PART_SIZE);
                            assert!(bytes.iter().all(|&byte| byte == b'a'));
                        } else {
                            assert_eq!(bytes.as_ref(), b"tail");
                        }
                    }
                    UploadRequest::Finish => {
                        assert_eq!(path, "/b2api/v2/b2_finish_large_file");
                        let actual: Value = serde_json::from_slice(&req.body().to_bytes()).unwrap();
                        assert_eq!(
                            actual,
                            json!({"fileId": "large-file", "partSha1Array": [PART_SHA1, TAIL_SHA1]})
                        );
                    }
                    UploadRequest::Cancel => {
                        assert_eq!(path, "/b2api/v2/b2_cancel_large_file");
                        let actual: Value = serde_json::from_slice(&req.body().to_bytes()).unwrap();
                        assert_eq!(actual, json!({"fileId": "large-file"}));
                    }
                }
                (status, body)
            }
        };
        let buffer = Buffer::from(body.to_string());
        let len = buffer.len() as u64;
        Ok(Response::builder()
            .status(status)
            .body(HttpBody::new(stream::iter([Ok(buffer)]), Some(len)))
            .unwrap())
    }
}

fn operator(transport: impl HttpTransport) -> Result<Operator> {
    let op = Operator::new(
        B2::default()
            .bucket("bucket")
            .bucket_id("bucket-id")
            .application_key_id("test-key")
            .application_key("test-secret"),
    )?;
    Ok(op
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport))))
}

fn scripted_operator(
    steps: impl IntoIterator<Item = (UploadRequest, StatusCode, Value)>,
) -> Result<(Operator, UploadScript)> {
    let script = UploadScript(Arc::new(Mutex::new(steps.into_iter().collect())));
    let op = operator(script.clone())?.layer(RetryLayer::new().with_min_delay(Duration::ZERO));
    Ok((op, script))
}

fn bad_request(message: &str) -> Value {
    json!({"status": 400, "code": "bad_request", "message": message})
}

async fn large_file_writer(op: &Operator, checked: bool) -> Result<Writer> {
    let mut writer = op.writer_with("file").chunk(PART_SIZE);
    if checked {
        writer = writer.checksum(WriteChecksum::Any);
    }
    let mut writer = writer.await?;
    writer.write(vec![b'a'; PART_SIZE]).await?;
    writer.write("tail").await?;
    Ok(writer)
}

#[tokio::test]
async fn unsupported_checksum_intents_fail_before_io() -> Result<()> {
    struct NoHttp;
    impl HttpTransport for NoHttp {
        async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
            panic!("unsupported checksum intent reached HTTP transport");
        }
    }

    let op = operator(NoHttp)?;
    assert!(op.info().capability().write_with_checksum);
    let expire = Duration::from_secs(60);
    for intent in [
        WriteChecksum::Any,
        WriteChecksum::Fast,
        WriteChecksum::Algo(ChecksumAlgo::Sha1),
        WriteChecksum::Expected(Checksum::new(ChecksumAlgo::Sha1, ABC_DIGEST)),
    ] {
        if intent != WriteChecksum::Any {
            let err = op.writer_with("file").checksum(intent.clone()).await.err();
            assert_eq!(err.map(|err| err.kind()), Some(ErrorKind::Unsupported));
            assert_eq!(
                op.write_with("file", "abc")
                    .checksum(intent.clone())
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::Unsupported,
            );
        }
        let opts = WriteOptions {
            checksum: Some(intent),
            ..Default::default()
        };
        let err = op
            .presign_write_options("file", expire, opts)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_upload_replays_only_rejected_body_and_returns_sha1() -> Result<()> {
    let (op, script) = scripted_operator([
        (
            UploadRequest::File(ABC_SHA1),
            StatusCode::BAD_REQUEST,
            bad_request(UPLOAD_MISMATCH),
        ),
        (
            UploadRequest::File(ABC_SHA1),
            StatusCode::OK,
            json!({"contentLength": 3, "contentSha1": ABC_SHA1}),
        ),
    ])?;
    let body = vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")];
    let meta = op
        .write_with("file", body)
        .checksum(WriteChecksum::Any)
        .await?;
    assert_eq!(
        meta.checksum(),
        Some(Checksum::new(ChecksumAlgo::Sha1, ABC_DIGEST))
    );
    assert!(script.0.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn ordinary_success_with_unconfirmed_sha1_cannot_publish_twice() -> Result<()> {
    for receipt in [
        json!({"contentLength": 3}),
        json!({"contentLength": 3, "contentSha1": TAIL_SHA1}),
        json!({"contentLength": 3, "contentSha1": format!("unverified:{ABC_SHA1}")}),
    ] {
        let (op, script) =
            scripted_operator([(UploadRequest::File(ABC_SHA1), StatusCode::OK, receipt)])?;
        let mut w = op.writer_with("file").checksum(WriteChecksum::Any).await?;
        w.write("abc").await?;
        let err = w.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.message().contains("may already be visible"));
        assert!(!err.is_temporary());
        assert_eq!(w.close().await.unwrap_err().kind(), ErrorKind::Unexpected);
        assert!(script.0.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_success_with_an_incomplete_receipt_is_not_retried() -> Result<()> {
    struct TruncatedReceipt(UploadScript);

    impl HttpTransport for TruncatedReceipt {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let upload = req.uri().path() == "/file";
            let mut response = self.0.fetch(req).await?;
            if upload {
                *response.body_mut() = HttpBody::new(stream::empty(), Some(1));
            }
            Ok(response)
        }
    }

    let script = UploadScript(Arc::new(Mutex::new(VecDeque::from([(
        UploadRequest::File(ABC_SHA1),
        StatusCode::OK,
        json!({"contentLength": 3, "contentSha1": ABC_SHA1}),
    )]))));
    let op = operator(TruncatedReceipt(script.clone()))?
        .layer(RetryLayer::new().with_min_delay(Duration::ZERO));
    let mut writer = op.writer_with("file").checksum(WriteChecksum::Any).await?;
    writer.write("abc").await?;
    let error = writer.close().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(!error.is_temporary());
    assert_eq!(
        writer.close().await.unwrap_err().kind(),
        ErrorKind::Unexpected
    );
    assert!(script.0.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn multipart_replays_unverified_parts_and_finishes_with_ordered_sha1s() -> Result<()> {
    let (op, script) = scripted_operator([
        (
            UploadRequest::Part(1, PART_SHA1),
            StatusCode::BAD_REQUEST,
            bad_request(UPLOAD_MISMATCH),
        ),
        (
            UploadRequest::Part(1, PART_SHA1),
            StatusCode::OK,
            json!({"contentSha1": PART_SHA1}),
        ),
        (
            UploadRequest::Part(2, TAIL_SHA1),
            StatusCode::OK,
            json!({"contentSha1": PART_SHA1}),
        ),
        (
            UploadRequest::Part(2, TAIL_SHA1),
            StatusCode::OK,
            json!({"contentSha1": TAIL_SHA1}),
        ),
        (
            UploadRequest::Finish,
            StatusCode::OK,
            json!({"contentLength": PART_SIZE + 4, "contentSha1": "none"}),
        ),
    ])?;
    let meta = large_file_writer(&op, true).await?.close().await?;
    assert_eq!(meta.content_length(), (PART_SIZE + 4) as u64);
    assert_eq!(meta.checksum(), None);
    assert!(script.0.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn finish_sha1_list_rejection_is_permanent_and_can_be_aborted() -> Result<()> {
    let (op, script) = scripted_operator([
        (
            UploadRequest::Part(1, PART_SHA1),
            StatusCode::OK,
            json!({"contentSha1": PART_SHA1}),
        ),
        (
            UploadRequest::Part(2, TAIL_SHA1),
            StatusCode::OK,
            json!({"contentSha1": TAIL_SHA1}),
        ),
        (
            UploadRequest::Finish,
            StatusCode::BAD_REQUEST,
            bad_request(&format!(
                "Part 1: expected sha1 {PART_SHA1} but found {TAIL_SHA1}"
            )),
        ),
        (UploadRequest::Cancel, StatusCode::OK, json!({})),
    ])?;
    let mut w = large_file_writer(&op, true).await?;
    let err = w.close().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ChecksumMismatch);
    assert!(!err.is_temporary());
    w.abort().await?;
    assert!(script.0.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn no_intent_keeps_unchecked_uploads_and_presign() -> Result<()> {
    let no_verify = "do_not_verify";
    let (op, script) = scripted_operator([
        (
            UploadRequest::File(no_verify),
            StatusCode::OK,
            json!({"contentLength": 3, "contentSha1": format!("unverified:{ABC_SHA1}")}),
        ),
        (
            UploadRequest::Part(1, no_verify),
            StatusCode::OK,
            json!({"contentSha1": format!("unverified:{PART_SHA1}")}),
        ),
        (
            UploadRequest::Part(2, no_verify),
            StatusCode::OK,
            json!({"contentSha1": format!("unverified:{TAIL_SHA1}")}),
        ),
        (
            UploadRequest::Finish,
            StatusCode::OK,
            json!({"contentLength": PART_SIZE + 4, "contentSha1": "none"}),
        ),
    ])?;
    assert_eq!(op.write("file", "abc").await?.checksum(), None);
    assert_eq!(
        large_file_writer(&op, false)
            .await?
            .close()
            .await?
            .checksum(),
        None
    );
    let signed = op.presign_write("file", Duration::from_secs(60)).await?;
    assert_eq!(signed.header()[SHA1_HEADER], no_verify);
    assert!(script.0.lock().unwrap().is_empty());
    Ok(())
}
