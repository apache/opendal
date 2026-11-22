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

use futures::channel::oneshot;
use http::Request;
use opendal_core::Buffer;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::HttpTransporter;
use opendal_core::Operator;
use opendal_core::raw::oio::ReadStream;
use opendal_http_transport_cyper::CyperTransport;
use opendal_service_http::Http;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn operator_read_on_compio() {
    let (endpoint, server) = serve("/object", |connection| {
        connection
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello Cyper",
            )
            .unwrap();
    });
    let transport = HttpTransporter::new(CyperTransport::new());
    let op = Operator::new(Http::default().endpoint(&endpoint)).unwrap();
    let context = op.base_context().with_http_transport(transport);
    let op = op.with_context(context);

    let runtime = compio::runtime::Runtime::new().unwrap();
    let body = runtime
        .block_on(compio::time::timeout(TEST_TIMEOUT, op.read("object")))
        .expect("operator read timed out")
        .unwrap();
    assert_eq!(body.to_bytes(), b"hello Cyper"[..]);
    server.join().unwrap();
}

#[test]
fn drop_unread_body_from_another_thread() {
    let (disconnected, wait_for_disconnect) = oneshot::channel();
    let (endpoint, server) = serve("/object", move |connection| {
        connection
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n")
            .unwrap();
        // An unfinished HTTP/1.1 response cannot go back into the connection pool.
        // The server keeps the body pending until the transport closes the socket.
        assert_eq!(connection.read(&mut [0]).unwrap(), 0);
        disconnected.send(()).unwrap();
    });
    let transport = HttpTransporter::new(CyperTransport::new());
    let request = Request::get(format!("{endpoint}/object"))
        .body(Buffer::new())
        .unwrap();

    let runtime = compio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        compio::time::timeout(TEST_TIMEOUT, async {
            let body = transport.fetch(request).await.unwrap().into_body();
            thread::spawn(move || drop(body)).join().unwrap();
            wait_for_disconnect.await.unwrap();
        })
        .await
        .expect("transport did not release the unread response");
    });
    server.join().unwrap();
}

const SIGNED_PATH: &str = concat!("/object?access_token=", "fixture-value", "&version=3");
const REDACTED_PATH: &str = "/object?access_token=[REDACTED]&version=3";

#[test]
fn redacts_invalid_url() {
    let transport = HttpTransporter::new(CyperTransport::new());
    let runtime = compio::runtime::Runtime::new().unwrap();
    let Err(error) = runtime.block_on(transport.fetch(request(SIGNED_PATH))) else {
        panic!("expected a relative URL to be rejected");
    };
    assert_redacted_error(error, "", "request url is invalid", false);
}

#[test]
fn redacts_send_error() {
    let (endpoint, server) = serve(SIGNED_PATH, |_| {});
    let transport = HttpTransporter::new(CyperTransport::new());
    let runtime = compio::runtime::Runtime::new().unwrap();
    let Err(error) = runtime
        .block_on(compio::time::timeout(
            TEST_TIMEOUT,
            transport.fetch(request(&format!("{endpoint}{SIGNED_PATH}"))),
        ))
        .unwrap()
    else {
        panic!("expected a connection closed before headers to fail");
    };
    server.join().unwrap();
    assert_redacted_error(error, &endpoint, "send http request", true);
}

#[test]
fn redacts_body_read_error() {
    let (close_connection, wait_to_close) = std::sync::mpsc::channel();
    let (endpoint, server) = serve(SIGNED_PATH, move |connection| {
        connection
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\na")
            .unwrap();
        wait_to_close.recv_timeout(TEST_TIMEOUT).unwrap();
    });
    let transport = HttpTransporter::new(CyperTransport::new());
    let runtime = compio::runtime::Runtime::new().unwrap();
    let error = runtime.block_on(async {
        compio::time::timeout(TEST_TIMEOUT, async {
            let response = transport
                .fetch(request(&format!("{endpoint}{SIGNED_PATH}")))
                .await
                .unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            let mut body = response.into_body();
            assert_eq!(body.read().await.unwrap().to_bytes(), "a");
            close_connection.send(()).unwrap();
            body.read().await.unwrap_err()
        })
        .await
        .unwrap()
    });
    server.join().unwrap();
    assert_redacted_error(error, &endpoint, "read data from http response", true);
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

fn serve(
    path: &'static str,
    response: impl FnOnce(&mut TcpStream) + Send + 'static,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        connection.set_read_timeout(Some(TEST_TIMEOUT)).unwrap();
        connection.set_write_timeout(Some(TEST_TIMEOUT)).unwrap();

        let mut lines = BufReader::new(&mut connection).lines();
        assert_eq!(
            lines.next().unwrap().unwrap(),
            format!("GET {path} HTTP/1.1")
        );
        assert!(lines.any(|line| line.unwrap().is_empty()));
        response(&mut connection);
    });
    (endpoint, server)
}
