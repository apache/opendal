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
use http::StatusCode;
use opendal_core::Buffer;
use opendal_core::ErrorKind;
use opendal_core::HttpBody;
use opendal_core::HttpTransporter;
use opendal_core::Result;
use opendal_core::raw::oio::ReadStream;
use opendal_http_transport_reqwest::ReqwestTransport;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;

const TIMEOUT: Duration = Duration::from_secs(10);

async fn fetch(raw_response: &'static [u8]) -> (StatusCode, Result<Buffer>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (connection, _) = listener.accept().await.unwrap();
        let mut lines = BufReader::new(connection).lines();
        while !lines.next_line().await.unwrap().unwrap().is_empty() {}
        let mut connection = lines.into_inner().into_inner();
        connection.write_all(raw_response).await.unwrap();
        connection.flush().await.unwrap();
    });

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()
        .unwrap();
    let transport = HttpTransporter::new(ReqwestTransport::new(client));
    let request = Request::get(&endpoint).body(Buffer::new()).unwrap();
    let response = tokio::time::timeout(TIMEOUT, transport.fetch(request))
        .await
        .expect("transport fetch timed out")
        .unwrap();
    let status = response.status();
    let mut body: HttpBody = response.into_body();
    let data = tokio::time::timeout(TIMEOUT, body.read_all())
        .await
        .expect("transport body read timed out");
    tokio::time::timeout(TIMEOUT, server)
        .await
        .expect("test server timed out")
        .unwrap();
    (status, data)
}

#[tokio::test]
async fn not_modified_ignores_content_length() {
    let (status, data) = fetch(b"HTTP/1.1 304 Not Modified\r\nContent-Length: 5\r\n\r\n").await;

    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(data.unwrap().is_empty());
}

#[tokio::test]
async fn ok_validates_content_length() {
    let (status, data) = fetch(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\na").await;

    assert_eq!(status, StatusCode::OK);
    // The HTTP client rejects the incomplete message before `HttpBody` reaches
    // the end of the stream, so the response must fail instead of being read
    // as an empty body.
    let error = data.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(error.is_temporary());
    assert!(
        error.message().contains("read data from http response"),
        "unexpected error: {error}"
    );
}
