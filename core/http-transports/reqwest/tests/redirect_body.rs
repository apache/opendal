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

use bytes::Bytes;
use http::Method;
use http::Request;
use http::StatusCode;
use http::header::CONTENT_LENGTH;
use http::header::LOCATION;
use opendal_core::Buffer;
use opendal_core::HttpTransporter;
use opendal_http_transport_reqwest::ReqwestTransport;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

const TIMEOUT: Duration = Duration::from_secs(10);

fn transport() -> HttpTransporter {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()
        .unwrap();
    HttpTransporter::new(ReqwestTransport::new(client))
}

/// Read one request and return its method, path, and body.
async fn read_request(listener: &TcpListener) -> (TcpStream, Method, String, Vec<u8>) {
    let (connection, _) = listener.accept().await.unwrap();
    let mut reader = BufReader::new(connection);

    let mut request_line = String::new();
    reader.read_line(&mut request_line).await.unwrap();
    let mut parts = request_line.trim_end().split(' ');
    let method = Method::from_bytes(parts.next().unwrap().as_bytes()).unwrap();
    let path = parts.next().unwrap().to_string();

    let mut content_length = 0;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).await.unwrap();
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':').expect("header must contain ':'");
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().unwrap();
        }
    }

    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).await.unwrap();
    (reader.into_inner(), method, path, body)
}

/// Serve `hops` redirects followed by `200 OK`, and return every request body.
async fn serve(
    listener: TcpListener,
    hops: usize,
    status: StatusCode,
    expected_body: Vec<u8>,
) -> Vec<Vec<u8>> {
    let mut bodies = Vec::with_capacity(hops + 1);
    for hop in 0..=hops {
        let (mut connection, method, path, body) = read_request(&listener).await;
        assert_eq!(method, Method::PUT);
        assert_eq!(path, format!("/hop{hop}"));
        assert_eq!(body, expected_body);
        bodies.push(body);

        let response = if hop < hops {
            format!(
                "HTTP/1.1 {} {}\r\n{LOCATION}: /hop{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                status.as_u16(),
                status.canonical_reason().unwrap(),
                hop + 1,
            )
        } else {
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_string()
        };
        connection.write_all(response.as_bytes()).await.unwrap();
        connection.shutdown().await.unwrap();
    }
    bodies
}

async fn assert_replays(body: Buffer, hops: usize, status: StatusCode) {
    let expected_body = body.to_bytes().to_vec();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(serve(listener, hops, status, expected_body));

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("{endpoint}/hop0"))
        .header(CONTENT_LENGTH, body.len())
        .body(body)
        .unwrap();
    let response = transport().fetch(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{status} redirect must resend the request body"
    );

    let bodies = server.await.unwrap();
    assert_eq!(bodies.len(), hops + 1);
}

fn contiguous_body() -> Buffer {
    Buffer::from(Bytes::from_static(b"redirect body"))
}

fn multi_chunk_body() -> Buffer {
    Buffer::from(vec![
        Bytes::from_static(b"redirect "),
        Bytes::from_static(b"body"),
    ])
}

#[tokio::test]
async fn temporary_redirect_replays_contiguous_body() {
    assert_replays(contiguous_body(), 1, StatusCode::TEMPORARY_REDIRECT).await;
}

#[tokio::test]
async fn permanent_redirect_replays_contiguous_body() {
    assert_replays(contiguous_body(), 1, StatusCode::PERMANENT_REDIRECT).await;
}

#[tokio::test]
async fn chained_temporary_redirects_replay_multi_chunk_body() {
    assert_replays(multi_chunk_body(), 2, StatusCode::TEMPORARY_REDIRECT).await;
}

#[tokio::test]
async fn chained_permanent_redirects_replay_multi_chunk_body() {
    assert_replays(multi_chunk_body(), 2, StatusCode::PERMANENT_REDIRECT).await;
}
