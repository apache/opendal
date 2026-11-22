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
use opendal_core::Buffer;
use opendal_core::ErrorKind;
use opendal_core::HttpConfig;
use opendal_core::HttpTransport;
use opendal_core::raw::oio::ReadStream;
use opendal_http_transport_reqwest::ReqwestTransport;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;

const TIMEOUT: Duration = Duration::from_secs(10);

fn request(uri: &str) -> Request<Buffer> {
    Request::get(uri).body(Buffer::new()).unwrap()
}

#[test]
fn rejects_unsupported_proxy_scheme() {
    let config = HttpConfig {
        proxy: Some("ftp://127.0.0.1:7890".to_string()),
        ..Default::default()
    };
    let err = ReqwestTransport::default().configure(&config).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    assert_eq!(err.message(), "http proxy scheme is not supported");
}

#[test]
fn rejects_malformed_proxy() {
    let config = HttpConfig {
        proxy: Some("http://".to_string()),
        ..Default::default()
    };
    let err = ReqwestTransport::default().configure(&config).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    assert_eq!(err.message(), "http proxy is invalid");
}

#[tokio::test]
async fn sends_requests_through_configured_proxy() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (connection, _) = listener.accept().await.unwrap();
        let mut lines = BufReader::new(connection).lines();
        assert_eq!(
            lines.next_line().await.unwrap().unwrap(),
            "GET http://opendal.invalid/object HTTP/1.1"
        );
        while !lines.next_line().await.unwrap().unwrap().is_empty() {}
        let mut connection = lines.into_inner().into_inner();
        connection
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
    });

    let config = HttpConfig {
        proxy: Some(proxy),
        ..Default::default()
    };
    let transport = ReqwestTransport::default().configure(&config).unwrap();
    let response = transport
        .fetch(request("http://opendal.invalid/object"))
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = response.into_body().read_all().await.unwrap();
    assert_eq!(&body.to_bytes()[..], b"ok");

    tokio::time::timeout(TIMEOUT, server)
        .await
        .unwrap()
        .unwrap();
}
