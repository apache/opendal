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

use std::error::Error as _;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use http::{Request, Response};
use opendal_core::raw::BoxedStaticFuture;
use opendal_core::{Buffer, Error, ErrorKind, Execute, Executor, HttpBody};
use opendal_core::{HttpTransport, HttpTransporter, Operator, OperatorRegistry, Result};
use opendal_http_transport_reqwest::register_http_context;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn load<const N: usize>(options: [(&str, &str); N]) -> Result<Operator> {
    let registry = OperatorRegistry::get();
    register_http_context(registry);
    registry.load(("memory", options))
}

fn request(uri: &str) -> Request<Buffer> {
    Request::get(uri).body(Buffer::new()).unwrap()
}

fn register_fixture_context(name: &str) -> Executor {
    struct Marker;

    impl HttpTransport for Marker {
        async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = HttpBody::new(
                futures::stream::iter([Ok(Buffer::from("original"))]),
                Some(8),
            );
            Ok(Response::new(body))
        }
    }

    struct FixtureExecutor;

    impl Execute for FixtureExecutor {
        fn execute(&self, future: BoxedStaticFuture<()>) {
            tokio::spawn(future);
        }
    }

    let executor = Executor::with(FixtureExecutor);
    let fixture_executor = executor.clone();
    // The fixture slot sorts before `http`, providing the factory's input context.
    OperatorRegistry::get().register_context(name, move |ctx, _| {
        Ok(ctx
            .with_http_transport(HttpTransporter::new(Marker))
            .with_executor(fixture_executor.clone()))
    });
    executor
}

#[tokio::test]
async fn unknown_options_preserve_the_incoming_context() -> Result<()> {
    let executor = register_fixture_context("fixture_unknown");
    let op = load([
        ("context.fixture_unknown.enabled", "true"),
        ("context.http.unrecognized", ""),
    ])?;
    assert!(Arc::ptr_eq(
        &op.context().executor().clone().into_inner(),
        &executor.into_inner(),
    ));
    let response = op
        .context()
        .http_transport()
        .send(request("/object"))
        .await?;
    assert_eq!(response.into_body().to_vec(), b"original");
    Ok(())
}

#[tokio::test]
async fn proxy_and_bypass_apply_per_request_with_the_configured_user_agent() -> Result<()> {
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let direct_url = format!("http://{}/object", origin.local_addr().unwrap());
    let proxied_url = format!(
        "http://127.0.0.2:{}/object",
        origin.local_addr().unwrap().port()
    );
    let expected_proxy_line = format!("GET {proxied_url} HTTP/1.1");
    let servers = tokio::spawn(async move {
        let (mut conn, line, agent) = read_request(proxy).await;
        assert_eq!(line, expected_proxy_line);
        assert_eq!(agent.as_deref(), Some("opendal-fixture/1"));
        conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nproxy")
            .await
            .unwrap();
        drop(conn);

        let (mut conn, line, agent) = read_request(origin).await;
        assert_eq!(line, "GET /object HTTP/1.1");
        assert_eq!(agent.as_deref(), Some("opendal-fixture/1"));
        conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\ndirect")
            .await
            .unwrap();
    });

    let executor = register_fixture_context("fixture_proxy");
    let op = load([
        ("context.fixture_proxy.enabled", "true"),
        ("context.http.proxy", &proxy_url),
        ("context.http.no_proxy", "127.0.0.1/32"),
        ("context.http.user_agent", "opendal-fixture/1"),
        ("context.http.connect_timeout", "2s"),
        ("context.http.read_timeout", "2s"),
        ("context.http.timeout", "PT5S"),
    ])?;
    assert!(Arc::ptr_eq(
        &op.context().executor().clone().into_inner(),
        &executor.into_inner(),
    ));
    let transport = op.context().http_transport();
    assert_eq!(
        transport
            .send(request(&proxied_url))
            .await?
            .into_body()
            .to_vec(),
        b"proxy"
    );
    assert_eq!(
        transport
            .send(request(&direct_url))
            .await?
            .into_body()
            .to_vec(),
        b"direct"
    );
    tokio::time::timeout(TEST_TIMEOUT, servers)
        .await
        .unwrap()
        .unwrap();
    Ok(())
}

#[tokio::test]
async fn read_and_total_timeouts_apply_to_the_response_body() -> Result<()> {
    for option in ["context.http.read_timeout", "context.http.timeout"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut conn, line, _) = read_request(listener).await;
            assert_eq!(line, "GET http://127.0.0.2/object HTTP/1.1");
            conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\na")
                .await
                .unwrap();
            pending::<()>().await;
            drop(conn);
        });

        let op = load([("context.http.proxy", &proxy_url), (option, "500ms")])?;
        let result = tokio::time::timeout(
            TEST_TIMEOUT,
            op.context()
                .http_transport()
                .send(request("http://127.0.0.2/object")),
        )
        .await;
        server.abort();
        let error = result
            .expect("configured deadline did not fire")
            .unwrap_err();
        assert_timeout(&error, "read data from http response");
    }
    Ok(())
}

#[cfg(any(feature = "rustls", feature = "native-tls"))]
#[tokio::test]
async fn connect_timeout_limits_the_proxy_tunnel_handshake() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", listener.local_addr().unwrap());
    let (ready, connected) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (conn, line, _) = read_request(listener).await;
        assert_eq!(line, "CONNECT 127.0.0.2:443 HTTP/1.1");
        ready.send(()).unwrap();
        pending::<()>().await;
        drop(conn);
    });

    let op = load([
        ("context.http.proxy", &proxy_url),
        ("context.http.connect_timeout", "500ms"),
    ])?;
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        op.context()
            .http_transport()
            .send(request("https://127.0.0.2/object")),
    )
    .await;
    server.abort();
    assert!(connected.await.is_ok(), "the proxy did not receive CONNECT");
    let error = result
        .expect("configured connect timeout did not fire")
        .unwrap_err();
    assert_timeout(&error, "send http request");
    Ok(())
}

fn assert_timeout(error: &Error, message: &str) {
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert_eq!(error.message(), message);
    let source = error
        .source()
        .unwrap()
        .downcast_ref::<reqwest::Error>()
        .unwrap();
    assert!(source.is_timeout(), "{error:?}");
}

async fn read_request(listener: TcpListener) -> (TcpStream, String, Option<String>) {
    let (conn, _) = listener.accept().await.unwrap();
    let mut lines = BufReader::new(conn).lines();
    let request = lines.next_line().await.unwrap().unwrap();
    let mut user_agent = None;
    while let Some(line) = lines.next_line().await.unwrap() {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').unwrap();
        if name.eq_ignore_ascii_case("user-agent") {
            user_agent = Some(value.trim().to_owned());
        }
    }
    (lines.into_inner().into_inner(), request, user_agent)
}
