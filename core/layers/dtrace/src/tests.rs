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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::stream;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use oio::ReadStream;

use super::*;

thread_local! {
    // Tokio's current-thread test runtime keeps each request and body on the test thread.
    static HTTP_PROBES: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn record_http_probe(name: &'static str) {
    HTTP_PROBES.with_borrow_mut(|probes| probes.push(name));
}

struct TestTransport(Mutex<Option<Result<Response<HttpBody>>>>);

impl HttpTransport for TestTransport {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        assert_eq!(request.method(), Method::PUT);
        assert_eq!(request.uri(), "https://example.com/object?signature=test");
        assert_eq!(request.headers()[http::header::AUTHORIZATION], "test");
        assert_eq!(request.body().to_vec(), b"payload");
        self.0.lock().unwrap().take().expect("one HTTP request")
    }
}

async fn fetch(response: Result<Response<HttpBody>>) -> Result<Response<HttpBody>> {
    let transport = HttpTransporter::new(TestTransport(Mutex::new(Some(response))));
    let context = OperationContext::new().with_http_transport(transport);
    let op = Operator::new(services::Memory::default())
        .unwrap()
        .layer(DtraceLayer::new())
        .with_context(context);
    let request = Request::put("https://example.com/object?signature=test")
        .header(http::header::AUTHORIZATION, "test")
        .body(Buffer::from("payload"))
        .unwrap();
    op.context().http_transport().fetch(request).await
}

#[tokio::test]
async fn forwards_http_response_without_reading_ahead() {
    let chunks_read = Arc::new(AtomicUsize::new(0));
    let chunks = stream::iter(["first", "second"].into_iter().map({
        let chunks_read = chunks_read.clone();
        move |chunk| {
            chunks_read.fetch_add(1, Ordering::Relaxed);
            Ok(Buffer::from(chunk))
        }
    }));
    let response = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(http::header::CONTENT_RANGE, "bytes 0-10/12")
        .body(HttpBody::new(chunks, Some(11)))
        .unwrap();

    let response = fetch(Ok(response)).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers()[http::header::CONTENT_RANGE],
        "bytes 0-10/12"
    );
    assert_eq!(chunks_read.load(Ordering::Relaxed), 0);
    HTTP_PROBES.with_borrow(|probes| {
        assert_eq!(probes.as_slice(), ["http_request_start", "http_request_ok"]);
    });

    let mut body = response.into_body();
    assert_eq!(body.read().await.unwrap().to_vec(), b"first");
    assert_eq!(chunks_read.load(Ordering::Relaxed), 1);
    assert_eq!(body.read().await.unwrap().to_vec(), b"second");
    assert!(body.read().await.unwrap().is_empty());
    HTTP_PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            [
                "http_request_start",
                "http_request_ok",
                "http_response_body_chunk",
                "http_response_body_chunk",
                "http_response_body_end",
            ]
        );
    });
}

#[tokio::test]
async fn forwards_http_error_status_and_body_error() {
    let body_error = Error::new(ErrorKind::Unexpected, "test body error").set_temporary();
    let expected = body_error.to_string();
    let response = Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(HttpBody::new(stream::iter([Err(body_error)]), None))
        .unwrap();

    let response = fetch(Ok(response)).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error = response.into_body().read().await.unwrap_err();
    assert_eq!(error.to_string(), expected);
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(error.is_temporary());
    HTTP_PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            [
                "http_request_start",
                "http_request_ok",
                "http_response_body_error",
            ]
        );
    });
}

#[tokio::test]
async fn preserves_response_body_size_validation() {
    let chunks = stream::iter([Ok(Buffer::from("short"))]);
    let response = Response::new(HttpBody::new(chunks, Some(10)));

    let mut body = fetch(Ok(response)).await.unwrap().into_body();
    assert_eq!(body.read().await.unwrap().to_vec(), b"short");
    let error = body.read().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(error.is_temporary());
}

#[tokio::test]
async fn forwards_http_transport_error() {
    let transport_error = Error::new(ErrorKind::PermissionDenied, "test transport error");
    let expected = transport_error.to_string();

    let error = fetch(Err(transport_error))
        .await
        .err()
        .expect("transport error");
    assert_eq!(error.to_string(), expected);
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    HTTP_PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            ["http_request_start", "http_request_error"]
        );
    });
}
