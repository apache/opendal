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

#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use http::Request;
use opendal_core::raw::oio::ReadStream;
use opendal_core::{Buffer, Error, ErrorKind, HttpTransporter};
use opendal_http_transport_reqwest::ReqwestTransport;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

const TIMEOUT: Duration = Duration::from_secs(10);
const SIGNED_PATH: &str = concat!("/object?access_token=", "fixture-value", "&version=3");
const REDACTED_PATH: &str = "/object?access_token=[REDACTED]&version=3";

fn transport() -> HttpTransporter {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()
        .unwrap();
    HttpTransporter::new(ReqwestTransport::new(client))
}

fn request(uri: &str) -> Request<Buffer> {
    Request::get(uri).body(Buffer::new()).unwrap()
}

fn assert_redacted_error(error: Error, endpoint: &str, message: &str, temporary: bool) {
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert_eq!(error.message(), message);
    assert_eq!(error.is_temporary(), temporary);
    let expected = format!("{endpoint}{REDACTED_PATH}");
    for diagnostic in [
        error.to_string(),
        format!("{error:?}"),
        format!("{error:#?}"),
    ] {
        assert!(diagnostic.contains(&expected), "{diagnostic}");
        assert!(!diagnostic.contains("fixture-value"), "{diagnostic}");
    }
}

async fn accept_request(listener: TcpListener) -> TcpStream {
    let (connection, _) = listener.accept().await.unwrap();
    let mut lines = BufReader::new(connection).lines();
    assert_eq!(
        lines.next_line().await.unwrap().unwrap(),
        format!("GET {SIGNED_PATH} HTTP/1.1")
    );
    while !lines.next_line().await.unwrap().unwrap().is_empty() {}
    lines.into_inner().into_inner()
}

#[tokio::test]
async fn redacts_invalid_url() {
    let Err(error) = transport().fetch(request(SIGNED_PATH)).await else {
        panic!("expected a relative URL to be rejected");
    };
    assert_redacted_error(error, "", "request url is invalid", false);
}

#[tokio::test]
async fn redacts_send_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async {
        drop(accept_request(listener).await);
    });

    let Err(error) = transport()
        .fetch(request(&format!("{endpoint}{SIGNED_PATH}")))
        .await
    else {
        panic!("expected a connection closed before headers to fail");
    };
    tokio::time::timeout(TIMEOUT, server)
        .await
        .unwrap()
        .unwrap();
    assert_redacted_error(error, &endpoint, "send http request", true);
}

#[tokio::test]
async fn redacts_body_read_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (close_connection, wait_to_close) = oneshot::channel();
    let server = tokio::spawn(async {
        let mut connection = accept_request(listener).await;
        connection
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\na")
            .await
            .unwrap();
        wait_to_close.await.unwrap();
    });

    let response = transport()
        .fetch(request(&format!("{endpoint}{SIGNED_PATH}")))
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    let mut body = response.into_body();
    assert_eq!(body.read().await.unwrap().to_bytes(), "a");
    close_connection.send(()).unwrap();
    let error = body.read().await.unwrap_err();
    tokio::time::timeout(TIMEOUT, server)
        .await
        .unwrap()
        .unwrap();
    assert_redacted_error(error, &endpoint, "read data from http response", true);
}
