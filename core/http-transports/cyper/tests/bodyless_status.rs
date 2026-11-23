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
use std::io::Write;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use compio::runtime::Runtime;
use http::Request;
use http::StatusCode;
use opendal_core::Buffer;
use opendal_core::ErrorKind;
use opendal_core::HttpBody;
use opendal_core::HttpTransporter;
use opendal_core::Result;
use opendal_core::raw::oio::ReadStream;
use opendal_http_transport_cyper::CyperTransport;

const TIMEOUT: Duration = Duration::from_secs(10);

fn fetch(raw_response: &'static [u8]) -> (StatusCode, Result<Buffer>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        let mut lines = BufReader::new(&mut connection).lines();
        while !lines.next().unwrap().unwrap().is_empty() {}
        connection.write_all(raw_response).unwrap();
        connection.flush().unwrap();
    });

    let transport = HttpTransporter::new(CyperTransport::new());
    let request = Request::get(format!("{endpoint}/object"))
        .body(Buffer::new())
        .unwrap();
    let runtime = Runtime::new().unwrap();
    let (status, data) = runtime
        .block_on(compio::time::timeout(TIMEOUT, async {
            let response = transport.fetch(request).await.unwrap();
            let status = response.status();
            let mut body: HttpBody = response.into_body();
            (status, body.read_all().await)
        }))
        .expect("transport timed out");
    server.join().unwrap();
    (status, data)
}

#[test]
fn not_modified_ignores_content_length() {
    let (status, data) = fetch(b"HTTP/1.1 304 Not Modified\r\nContent-Length: 5\r\n\r\n");

    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(data.unwrap().is_empty());
}

#[test]
fn no_content_ignores_content_length() {
    let (status, data) = fetch(b"HTTP/1.1 204 No Content\r\nContent-Length: 5\r\n\r\n");

    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(data.unwrap().is_empty());
}

#[test]
fn ok_validates_content_length() {
    let (status, data) = fetch(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\na");

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
