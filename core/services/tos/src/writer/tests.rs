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
use std::sync::Mutex;
use std::time::Duration;

use futures::stream;
use http::HeaderValue;
use http::header::ETAG;
use oio::Write;
use opendal_core::options::WriteOptions;

use super::*;
use crate::Tos;

const VECTOR_CRC: &str = "11051210869376104954";
const FIRST_PART_CRC: &str = "14865968487149912872";
const LAST_PART_CRC: &str = "7598038797645275211";
const COMPLETE: &str = "CompleteMultipartUpload";
const ABORT: &str = "AbortMultipartUpload";
const COMPLETE_BODY: &str = r#"{"ETag":"\"complete\"","VersionId":"version"}"#;
const MISSING_UPLOAD: &str = r#"{"Code":"NoSuchUpload","Message":"the upload no longer exists"}"#;

type Reply = (&'static str, Response<Result<Buffer>>);

#[derive(Clone, Default)]
struct ScriptedTransport(Arc<Mutex<VecDeque<Reply>>>);

impl ScriptedTransport {
    fn new(replies: impl IntoIterator<Item = Reply>) -> Self {
        Self(Arc::new(Mutex::new(replies.into_iter().collect())))
    }

    fn remaining(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

impl HttpTransport for ScriptedTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        let actual = req.extensions().get::<ServiceOperation>().unwrap().0;
        let (expected, reply) = self.0.lock().unwrap().pop_front().unwrap_or_else(|| {
            panic!("unexpected {actual} request");
        });
        assert_eq!(actual, expected);
        Ok(reply.map(|body| HttpBody::new(stream::iter([body]), None)))
    }
}

fn reply(status: StatusCode, crc: Option<&str>, body: &'static str) -> Response<Result<Buffer>> {
    let mut response = Response::builder().status(status);
    if let Some(crc) = crc {
        response = response.header(X_TOS_HASH_CRC64ECMA, crc);
    }
    response.body(Ok(Buffer::from(body))).unwrap()
}

fn staged_parts(checksum: bool) -> [Reply; 3] {
    let mut first = reply(StatusCode::OK, checksum.then_some(FIRST_PART_CRC), "");
    first
        .headers_mut()
        .insert(ETAG, HeaderValue::from_static("first"));
    let mut last = reply(StatusCode::OK, checksum.then_some(LAST_PART_CRC), "");
    last.headers_mut()
        .insert(ETAG, HeaderValue::from_static("last"));
    [
        (
            "CreateMultipartUpload",
            reply(StatusCode::OK, None, r#"{"UploadId":"upload"}"#),
        ),
        ("UploadPart", first),
        ("UploadPart", last),
    ]
}

fn builder() -> Tos {
    Tos::default()
        .bucket("test")
        .access_key_id("test")
        .secret_access_key("test")
        .disable_config_load()
}

fn context(transport: &ScriptedTransport) -> OperationContext {
    OperationContext::new().with_http_transport(HttpTransporter::new(transport.clone()))
}

fn writer(transport: &ScriptedTransport, checksum: Option<WriteChecksum>) -> impl Write {
    let service = builder().build().unwrap();
    let (args, _) = OpWrite::from_options(
        &service.capability(),
        WriteOptions {
            checksum,
            ..Default::default()
        },
    )
    .unwrap();
    service
        .write(&context(transport), "test.bin", args)
        .unwrap()
}

async fn write_parts(writer: &mut impl Write) {
    writer.write(Buffer::from("1234")).await.unwrap();
    writer.write(Buffer::from("56789")).await.unwrap();
}

fn assert_error<T: std::fmt::Debug>(result: Result<T>, kind: ErrorKind) -> Error {
    let error = result.unwrap_err();
    assert_eq!(error.kind(), kind, "{error}");
    assert!(error.is_permanent(), "{error}");
    error
}

#[tokio::test]
async fn completion_reports_json_rejection_and_can_be_retried() {
    let mut rejection = reply(
        StatusCode::OK,
        None,
        r#"{"Code":"DuplicateObject","Message":"the target already exists"}"#,
    );
    rejection
        .headers_mut()
        .insert(ETAG, HeaderValue::from_bytes(b"\xff").unwrap());
    let transport = ScriptedTransport::new(staged_parts(true).into_iter().chain([
        (COMPLETE, rejection),
        (
            COMPLETE,
            reply(StatusCode::OK, Some(VECTOR_CRC), COMPLETE_BODY),
        ),
    ]));
    let mut writer = writer(&transport, Some(WriteChecksum::Fast));
    write_parts(&mut writer).await;

    let error = assert_error(writer.close().await, ErrorKind::ConditionNotMatch);
    assert!(error.message().contains("the target already exists"));
    assert!(error.message().contains("DuplicateObject"));
    assert_eq!(transport.remaining(), 1);

    let meta = writer.close().await.unwrap();
    assert_eq!(meta.etag(), Some("complete"));
    assert_eq!(meta.version(), Some("version"));
    assert_eq!(
        meta.checksum(),
        Some(Checksum::new(
            ChecksumAlgo::Crc64Ecma,
            0x995d_c9bb_df19_39fa_u64.to_be_bytes(),
        ))
    );
    assert_eq!(transport.remaining(), 0);
    assert_error(writer.close().await, ErrorKind::Unexpected);
}

#[tokio::test]
async fn successful_put_status_with_a_lost_body_cannot_replay() {
    let response = reply(StatusCode::OK, Some(VECTOR_CRC), "")
        .map(|_| Err(Error::new(ErrorKind::Unexpected, "response body truncated").set_temporary()));
    let transport = ScriptedTransport::new([("PutObject", response)]);
    let mut writer = writer(&transport, Some(WriteChecksum::Any));
    writer.write(Buffer::from("123456789")).await.unwrap();

    assert_error(writer.close().await, ErrorKind::Unexpected);
    assert_eq!(transport.remaining(), 0);
    assert_error(writer.close().await, ErrorKind::Unexpected);
}

#[tokio::test]
async fn ambiguous_completion_cannot_replay_and_abort_accepts_consumed_upload() {
    let lost_body = reply(StatusCode::OK, Some(VECTOR_CRC), "")
        .map(|_| Err(Error::new(ErrorKind::Unexpected, "response body truncated").set_temporary()));
    for completion in [
        lost_body,
        reply(StatusCode::OK, Some(VECTOR_CRC), "{"),
        reply(StatusCode::OK, Some(VECTOR_CRC), "{}"),
        reply(StatusCode::OK, Some("0"), COMPLETE_BODY),
        reply(StatusCode::OK, None, COMPLETE_BODY),
    ] {
        let transport = ScriptedTransport::new(staged_parts(true).into_iter().chain([
            (COMPLETE, completion),
            (ABORT, reply(StatusCode::NOT_FOUND, None, MISSING_UPLOAD)),
        ]));
        let mut writer = writer(&transport, Some(WriteChecksum::Fast));
        write_parts(&mut writer).await;

        assert_error(writer.close().await, ErrorKind::Unexpected);
        assert_eq!(transport.remaining(), 1);
        assert_error(writer.close().await, ErrorKind::Unexpected);
        assert_eq!(transport.remaining(), 1);
        writer.abort().await.unwrap();
        assert_eq!(transport.remaining(), 0);
    }
}

#[tokio::test]
async fn unconfirmed_part_prevents_completion() {
    for (crc, kind) in [
        (None, ErrorKind::Unexpected),
        (Some("0"), ErrorKind::ChecksumMismatch),
    ] {
        let [initiate, first, (operation, mut last)] = staged_parts(true);
        last.headers_mut().remove(X_TOS_HASH_CRC64ECMA);
        if let Some(crc) = crc {
            last.headers_mut()
                .insert(X_TOS_HASH_CRC64ECMA, HeaderValue::from_static(crc));
        }
        let transport = ScriptedTransport::new([
            initiate,
            first,
            (operation, last),
            (ABORT, reply(StatusCode::NO_CONTENT, None, "")),
        ]);
        let mut writer = writer(&transport, Some(WriteChecksum::Fast));
        write_parts(&mut writer).await;

        assert_error(writer.close().await, kind);
        assert_eq!(transport.remaining(), 1);
        writer.abort().await.unwrap();
        assert_eq!(transport.remaining(), 0);
    }
}

#[tokio::test]
async fn abort_preserves_other_errors_after_ambiguous_completion() {
    let transport = ScriptedTransport::new(staged_parts(true).into_iter().chain([
        (COMPLETE, reply(StatusCode::OK, None, COMPLETE_BODY)),
        (
            ABORT,
            reply(StatusCode::FORBIDDEN, None, r#"{"Code":"AccessDenied"}"#),
        ),
        (
            ABORT,
            reply(StatusCode::NOT_FOUND, None, r#"{"Code":"NoSuchBucket"}"#),
        ),
        (ABORT, reply(StatusCode::NOT_FOUND, None, "")),
        (ABORT, reply(StatusCode::NO_CONTENT, None, "")),
    ]));
    let mut writer = writer(&transport, Some(WriteChecksum::Fast));
    write_parts(&mut writer).await;

    assert_error(writer.close().await, ErrorKind::Unexpected);
    assert_error(writer.abort().await, ErrorKind::PermissionDenied);
    assert_error(writer.abort().await, ErrorKind::ConfigInvalid);
    assert_error(writer.abort().await, ErrorKind::NotFound);
    writer.abort().await.unwrap();
    assert_eq!(transport.remaining(), 0);
}

#[tokio::test]
async fn abort_without_observed_completion_reports_missing_upload() {
    let [initiate, first, _] = staged_parts(true);
    let transport = ScriptedTransport::new([
        initiate,
        first,
        (ABORT, reply(StatusCode::NOT_FOUND, None, MISSING_UPLOAD)),
    ]);
    let mut writer = writer(&transport, Some(WriteChecksum::Fast));
    write_parts(&mut writer).await;

    assert_error(writer.abort().await, ErrorKind::NotFound);
    assert_eq!(transport.remaining(), 0);
}

#[tokio::test]
async fn plain_multipart_write_accepts_success_without_a_checksum() {
    let transport = ScriptedTransport::new(staged_parts(false).into_iter().chain([
        (
            COMPLETE,
            reply(
                StatusCode::OK,
                None,
                r#"{"Code":"DuplicateObject","Message":"the target already exists"}"#,
            ),
        ),
        (COMPLETE, reply(StatusCode::OK, None, COMPLETE_BODY)),
    ]));
    let mut writer = writer(&transport, None);
    write_parts(&mut writer).await;

    let error = assert_error(writer.close().await, ErrorKind::Unexpected);
    assert_eq!(error.message(), "the target already exists");
    let meta = writer.close().await.unwrap();
    assert_eq!(meta.etag(), Some("complete"));
    assert_eq!(meta.version(), Some("version"));
    assert!(meta.checksum().is_none());
    assert_eq!(transport.remaining(), 0);
}

#[tokio::test]
async fn unsupported_checksum_options_fail_before_io() {
    let transport = ScriptedTransport::default();
    let op = Operator::new(builder())
        .unwrap()
        .with_context(context(&transport));
    assert!(op.info().capability().write_with_checksum);
    for checksum in [
        WriteChecksum::Algo(ChecksumAlgo::Crc64Ecma),
        WriteChecksum::Expected(Checksum::new(ChecksumAlgo::Crc64Ecma, [0; 8])),
    ] {
        assert_error(
            op.writer_with("test.bin").checksum(checksum).await,
            ErrorKind::Unsupported,
        );
    }
    for checksum in [WriteChecksum::Any, WriteChecksum::Fast] {
        assert_error(
            op.writer_with("test.bin")
                .append(true)
                .checksum(checksum.clone())
                .await,
            ErrorKind::Unsupported,
        );
        let options = WriteOptions {
            checksum: Some(checksum),
            ..Default::default()
        };
        assert_error(
            op.presign_write_options("test.bin", Duration::from_secs(60), options)
                .await,
            ErrorKind::Unsupported,
        );
    }

    let unsigned = Operator::new(builder().skip_signature())
        .unwrap()
        .with_context(context(&transport));
    assert!(!unsigned.info().capability().write_with_checksum);
    assert_error(
        unsigned
            .writer_with("test.bin")
            .checksum(WriteChecksum::Any)
            .await,
        ErrorKind::Unsupported,
    );
}

#[test]
fn receipt_requires_one_unsigned_decimal_u64() {
    let mut headers = HeaderMap::new();
    assert_eq!(parse_crc64_receipt(&headers), None);
    for (value, expected) in [
        ("0", Some(0)),
        (VECTOR_CRC, Some(0x995d_c9bb_df19_39fa)),
        ("18446744073709551615", Some(u64::MAX)),
        ("", None),
        ("+0", None),
        ("-1", None),
        (" 0", None),
        ("0, 0", None),
        ("18446744073709551616", None),
    ] {
        headers.insert(X_TOS_HASH_CRC64ECMA, HeaderValue::from_static(value));
        assert_eq!(parse_crc64_receipt(&headers), expected, "{value:?}");
    }
    headers.insert(
        X_TOS_HASH_CRC64ECMA,
        HeaderValue::from_bytes(b"\xff").unwrap(),
    );
    assert_eq!(parse_crc64_receipt(&headers), None);
    headers.insert(X_TOS_HASH_CRC64ECMA, HeaderValue::from_static("0"));
    headers.append(X_TOS_HASH_CRC64ECMA, HeaderValue::from_static("0"));
    assert_eq!(parse_crc64_receipt(&headers), None);
}
