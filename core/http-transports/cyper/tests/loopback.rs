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
use opendal_core::HttpTransporter;
use opendal_core::Operator;
use opendal_http_transport_cyper::CyperTransport;
use opendal_service_http::Http;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn operator_read_on_compio() {
    let (endpoint, server) = serve(|connection| {
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
    let (endpoint, server) = serve(move |connection| {
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

fn serve(
    response: impl FnOnce(&mut TcpStream) + Send + 'static,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        connection.set_read_timeout(Some(TEST_TIMEOUT)).unwrap();
        connection.set_write_timeout(Some(TEST_TIMEOUT)).unwrap();

        let mut lines = BufReader::new(&mut connection).lines();
        assert_eq!(lines.next().unwrap().unwrap(), "GET /object HTTP/1.1");
        assert!(lines.any(|line| line.unwrap().is_empty()));
        response(&mut connection);
    });
    (endpoint, server)
}
