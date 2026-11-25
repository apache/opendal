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

use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use http::Method;
use http::Request;
use http::StatusCode;
use http::header::CONTENT_LENGTH;
use http::header::LOCATION;
use opendal_core::Buffer;
use opendal_core::HttpTransporter;
use opendal_core::raw::oio::ReadStream;
use opendal_http_transport_cyper::CyperTransport;

const TIMEOUT: Duration = Duration::from_secs(10);

/// Read one request and return its method, path, and body.
fn read_request(listener: &TcpListener) -> (TcpStream, Method, String, Vec<u8>) {
    let (mut connection, _) = listener.accept().unwrap();
    connection.set_read_timeout(Some(TIMEOUT)).unwrap();
    connection.set_write_timeout(Some(TIMEOUT)).unwrap();

    let (method, path, body) = {
        let mut reader = BufReader::new(&mut connection);
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        let mut parts = request_line.trim_end().split(' ');
        let method = Method::from_bytes(parts.next().unwrap().as_bytes()).unwrap();
        let path = parts.next().unwrap().to_string();

        let mut content_length = 0;
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
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
        reader.read_exact(&mut body).unwrap();
        (method, path, body)
    };

    (connection, method, path, body)
}

/// Serve `hops` redirects followed by `200 OK`, and return every request body.
fn serve(
    hops: usize,
    status: StatusCode,
    expected_body: Vec<u8>,
) -> (String, thread::JoinHandle<Vec<Vec<u8>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let mut bodies = Vec::with_capacity(hops + 1);
        for hop in 0..=hops {
            let (mut connection, method, path, body) = read_request(&listener);
            assert_eq!(method, Method::PUT);
            assert_eq!(path, format!("/hop{hop}"));
            assert_eq!(body, expected_body);
            bodies.push(body);

            if hop < hops {
                write!(
                    connection,
                    "HTTP/1.1 {} {}\r\n{LOCATION}: /hop{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    status.as_u16(),
                    status.canonical_reason().unwrap(),
                    hop + 1,
                )
                .unwrap();
            } else {
                connection
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .unwrap();
            }
            connection.flush().unwrap();
        }
        bodies
    });
    (endpoint, server)
}

fn assert_replays(body: Buffer, hops: usize, status: StatusCode) {
    let expected_body = body.to_bytes().to_vec();
    let (endpoint, server) = serve(hops, status, expected_body);

    let transport = HttpTransporter::new(CyperTransport::new());
    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("{endpoint}/hop0"))
        .header(CONTENT_LENGTH, body.len())
        .body(body)
        .unwrap();
    let runtime = compio::runtime::Runtime::new().unwrap();
    let (response_status, response_body) = runtime
        .block_on(compio::time::timeout(TIMEOUT, async {
            let response = transport.fetch(request).await.unwrap();
            let status = response.status();
            let mut body = response.into_body();
            (status, body.read_all().await.unwrap().to_bytes())
        }))
        .expect("transport timed out");
    assert_eq!(
        response_status,
        StatusCode::OK,
        "{status} redirect must resend the request body"
    );
    assert_eq!(response_body, b"ok"[..]);

    let bodies = server.join().unwrap();
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

#[test]
fn temporary_redirect_replays_contiguous_body() {
    assert_replays(contiguous_body(), 1, StatusCode::TEMPORARY_REDIRECT);
}

#[test]
fn permanent_redirect_replays_contiguous_body() {
    assert_replays(contiguous_body(), 1, StatusCode::PERMANENT_REDIRECT);
}

#[test]
fn chained_temporary_redirects_replay_multi_chunk_body() {
    assert_replays(multi_chunk_body(), 2, StatusCode::TEMPORARY_REDIRECT);
}

#[test]
fn chained_permanent_redirects_replay_multi_chunk_body() {
    assert_replays(multi_chunk_body(), 2, StatusCode::PERMANENT_REDIRECT);
}
