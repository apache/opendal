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

use std::cell::RefCell;
use std::sync::Mutex;
use std::sync::Once;

use futures::executor::block_on;
use futures::stream;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;

use super::*;

const SIGNED_URL: &str = "https://example.com/object?signature=query-secret";
const AUTHORIZATION: &str = "Bearer auth-secret";
const COOKIE: &str = "session=cookie-secret";

thread_local! {
    static RECORDS: RefCell<Vec<(Level, String)>> = const { RefCell::new(Vec::new()) };
}

struct RecordingLogger;

impl log::Log for RecordingLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target() == "opendal::services::http"
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            RECORDS.with_borrow_mut(|records| {
                records.push((record.level(), record.args().to_string()));
            });
        }
    }

    fn flush(&self) {}
}

struct FixedTransport(Mutex<Option<Result<Response<HttpBody>>>>);

impl HttpTransport for FixedTransport {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        assert_eq!(request.method(), http::Method::PUT);
        assert_eq!(request.uri(), SIGNED_URL);
        assert_eq!(request.headers()[header::AUTHORIZATION], AUTHORIZATION);
        assert_eq!(request.headers()[header::COOKIE], COOKIE);
        assert_eq!(request.body().to_vec(), b"request payload");
        self.0.lock().unwrap().take().unwrap()
    }
}

fn logging_operator(result: Result<Response<HttpBody>>) -> Operator {
    static INIT_LOGGER: Once = Once::new();
    INIT_LOGGER.call_once(|| {
        log::set_logger(&RecordingLogger).unwrap();
        log::set_max_level(log::LevelFilter::Debug);
    });
    let transport = HttpTransporter::new(FixedTransport(Mutex::new(Some(result))));
    Operator::new(services::Memory::default())
        .unwrap()
        .layer(LoggingLayer::default())
        .with_context(OperationContext::default().with_http_transport(transport))
}

fn signed_request() -> Request<Buffer> {
    Request::put(SIGNED_URL)
        .header(header::AUTHORIZATION, AUTHORIZATION)
        .header(header::COOKIE, COOKIE)
        .body(Buffer::from("request payload"))
        .unwrap()
}

fn take_http_records() -> Vec<(Level, String)> {
    let records = RECORDS.take();
    for (_, message) in &records {
        assert!(message.contains("service=memory"));
        assert!(message.contains("method=PUT"));
        assert!(!message.contains("example.com"));
        assert!(!message.contains("signature"));
        assert!(!message.contains("auth-secret"));
        assert!(!message.contains("cookie-secret"));
        assert!(!message.contains("request payload"));
    }
    records
}

#[test]
fn logs_http_response_status_and_forwards_body() {
    let body = HttpBody::new(stream::iter([Ok(Buffer::from("data"))]), Some(4));
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::CREATED;
    let operator = logging_operator(Ok(response));

    let response = block_on(operator.context().http_transport().send(signed_request())).unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.body().to_vec(), b"data");

    let records = take_http_records();
    let [started, finished] = records.as_slice() else {
        panic!("expected start and response records, got {records:?}");
    };
    assert_eq!(started.0, Level::Debug);
    assert!(started.1.ends_with("http::fetch started"));
    assert_eq!(finished.0, Level::Debug);
    assert!(finished.1.ends_with("status=201: http::fetch finished"));
}

#[test]
fn logs_http_transport_error_without_its_details() {
    let error = Error::new(
        ErrorKind::Unexpected,
        format!("could not reach {SIGNED_URL}"),
    )
    .set_temporary();
    let operator = logging_operator(Err(error));

    let result = block_on(operator.context().http_transport().fetch(signed_request()));
    let Err(error) = result else {
        panic!("expected the inner transport error");
    };
    assert!(error.is_temporary());
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(error.to_string().contains(SIGNED_URL));

    let records = take_http_records();
    let [started, failed] = records.as_slice() else {
        panic!("expected start and error records, got {records:?}");
    };
    assert_eq!(started.0, Level::Debug);
    assert!(started.1.ends_with("http::fetch started"));
    assert_eq!(failed.0, Level::Error);
    assert!(
        failed
            .1
            .ends_with("error_kind=Unexpected: http::fetch failed")
    );
}

#[test]
fn logs_http_body_error_when_polled() {
    let body = HttpBody::new(
        stream::iter([
            Ok(Buffer::from("data")),
            Err(Error::new(ErrorKind::PermissionDenied, AUTHORIZATION)),
        ]),
        None,
    );
    let operator = logging_operator(Ok(Response::new(body)));

    let result = block_on(operator.context().http_transport().fetch(signed_request()));
    let mut body = result.expect("expected an HTTP response").into_body();
    assert_eq!(take_http_records().len(), 2);
    assert_eq!(
        block_on(oio::ReadStream::read(&mut body)).unwrap().to_vec(),
        b"data"
    );
    assert!(take_http_records().is_empty());

    let error = block_on(oio::ReadStream::read(&mut body)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(error.to_string().contains(AUTHORIZATION));

    let records = take_http_records();
    let [failed] = records.as_slice() else {
        panic!("expected one body error record, got {records:?}");
    };
    assert_eq!(failed.0, Level::Warn);
    assert!(
        failed
            .1
            .ends_with("error_kind=PermissionDenied: http::fetch body failed")
    );
}
