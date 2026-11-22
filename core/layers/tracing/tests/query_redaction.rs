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

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http::{Request, StatusCode};
use opendal_core::{Buffer, HttpTransporter, Operator, services};
use opendal_http_transport_reqwest::ReqwestTransport;
use opendal_layer_tracing::TracingLayer;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn traces_redacted_uri_while_sending_original_request() {
    let output = Capture::default();
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("opendal_layer_tracing=debug")
        .with_span_events(FmtSpan::NEW)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let path = concat!("/object?sig=", "fixture-value", "&sv=2024-08-04");
    let server = async {
        let (mut connection, _) = listener.accept().await.unwrap();
        let mut lines = BufReader::new(&mut connection).lines();
        assert_eq!(
            lines.next_line().await.unwrap().unwrap(),
            format!("GET {path} HTTP/1.1")
        );
        while !lines.next_line().await.unwrap().unwrap().is_empty() {}
        connection
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    };

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let transport = HttpTransporter::new(ReqwestTransport::new(client));
    let operator = Operator::new(services::Memory::default()).unwrap();
    let context = operator.base_context().with_http_transport(transport);
    let operator = operator.with_context(context).layer(TracingLayer::new());
    let request = Request::get(format!("{endpoint}{path}"))
        .body(Buffer::new())
        .unwrap();

    let ((), response) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(server, operator.context().http_transport().send(request))
    })
    .await
    .unwrap();
    assert_eq!(response.unwrap().status(), StatusCode::OK);

    let output = output.0.lock().unwrap();
    let output = std::str::from_utf8(&output).unwrap();
    assert!(output.contains("http::fetch"), "{output}");
    assert!(!output.contains("fixture-value"), "{output}");
    assert!(
        output.contains(&format!("{endpoint}/object?sig=[REDACTED]&sv=2024-08-04")),
        "{output}"
    );
}
