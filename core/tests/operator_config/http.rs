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

#[cfg(all(feature = "layers-retry", feature = "layers-timeout"))]
#[tokio::test]
async fn configured_timeout_retry_order_matches_typed_layers() -> anyhow::Result<()> {
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use http::{Request, Response, StatusCode};
    use opendal::layers::{RetryLayer, TimeoutLayer};
    use opendal::raw::BoxedStaticFuture;
    use opendal::{Buffer, Execute, Executor, HttpBody, HttpTransport, HttpTransporter};
    use opendal::{Operator, Result};

    struct RetryOnce(AtomicUsize);

    impl HttpTransport for RetryOnce {
        async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
            let status = if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            };
            Ok(Response::builder()
                .status(status)
                .header(http::header::CONTENT_LENGTH, "0")
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        }
    }

    struct DeadlineRecorder(Arc<AtomicUsize>);

    impl Execute for DeadlineRecorder {
        fn execute(&self, future: BoxedStaticFuture<()>) {
            tokio::spawn(future);
        }

        fn sleep(&self, _: Duration) -> Option<BoxedStaticFuture<()>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Some(Box::pin(pending()))
        }
    }

    opendal::init_default_registry();
    let endpoint = "http://127.0.0.1:9";
    let build = |layers| {
        Operator::via_iter(
            "http",
            [
                ("endpoint".to_owned(), endpoint.to_owned()),
                ("layers".to_owned(), layers),
                ("layer.retry.max_times".to_owned(), "1".to_owned()),
                ("layer.retry.min_delay".to_owned(), "1ms".to_owned()),
            ],
        )
    };
    let typed = Operator::new(opendal::services::Http::default().endpoint(endpoint))?
        .layer(TimeoutLayer::new())
        .layer(
            RetryLayer::new()
                .with_max_times(1)
                .with_min_delay(Duration::from_millis(1)),
        );
    let op = build("timeout,retry".to_owned())?;
    let reversed = build("retry,timeout".to_owned())?;

    for (op, deadlines) in [(typed, 2), (op, 2), (reversed, 1)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let ctx = op
            .base_context()
            .with_http_transport(HttpTransporter::new(RetryOnce(AtomicUsize::new(0))))
            .with_executor(Executor::with(DeadlineRecorder(calls.clone())));
        let op = op.with_context(ctx);
        op.stat("object").await?;
        assert_eq!(calls.load(Ordering::Relaxed), deadlines);
    }
    Ok(())
}

#[cfg(all(
    feature = "layers-concurrent-limit",
    feature = "layers-tracing",
    feature = "layers-retry",
    any(
        feature = "http-transport-reqwest-native-tls",
        feature = "http-transport-reqwest-rustls"
    ),
))]
mod proxy_replay {
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use http::{Request, Response};
    use opendal::{
        Buffer, Executor, HttpBody, HttpTransport, HttpTransporter, Operator, OperatorRegistry,
        Result,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use tracing::{Subscriber, span};
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;

    struct CountFetchSpans(Arc<AtomicUsize>);

    impl<S: Subscriber> tracing_subscriber::Layer<S> for CountFetchSpans {
        fn on_new_span(&self, attrs: &span::Attributes<'_>, _: &span::Id, _: Context<'_, S>) {
            if attrs.metadata().name() == "http::fetch" {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    struct ObserveTransport {
        inner: HttpTransporter,
        requests: Arc<AtomicUsize>,
    }

    impl HttpTransport for ObserveTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            self.inner.fetch(req).await
        }
    }

    #[tokio::test]
    async fn configured_proxy_and_layers_survive_context_replay() -> anyhow::Result<()> {
        #[cfg(not(feature = "auto-register-services"))]
        opendal::install_default();

        let spans = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(CountFetchSpans(spans.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);
        let dispatches = Arc::new(AtomicUsize::new(0));
        let requests = dispatches.clone();
        // This context sorts after `http`, placing the observer below all layers.
        OperatorRegistry::get().register_context("observe", move |ctx, _| {
            let transport = ObserveTransport {
                inner: ctx.http_transport().clone(),
                requests: requests.clone(),
            };
            Ok(ctx.with_http_transport(HttpTransporter::new(transport)))
        });
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let proxy = format!("http://{}", listener.local_addr()?);
        let op = Operator::from_uri((
            "http://127.0.0.2",
            [
                ("context.http.proxy", proxy.as_str()),
                ("context.observe.requests", "on"),
                ("layers", "concurrent_limit,tracing,retry"),
                ("layer.concurrent_limit.permits", "2"),
                ("layer.concurrent_limit.http_permits", "1"),
                ("layer.retry.max_times", "0"),
            ],
        ))?;
        let client = async {
            assert_eq!(op.read("before").await?.to_bytes(), "fixture");
            assert_eq!(spans.load(Ordering::Relaxed), 1);
            assert_eq!(dispatches.load(Ordering::Relaxed), 1);

            let ctx = op.base_context().with_executor(Executor::with(()));
            let op = op.with_context(ctx);

            let req = Request::get("http://127.0.0.2/hold").body(Buffer::new())?;
            let held = op.context().http_transport().fetch(req).await?;
            assert_eq!(spans.load(Ordering::Relaxed), 2);
            let mut read = pin!(op.read("after"));
            // Poll once and observe dispatch below the HTTP limiter. The held
            // response keeps its permit until its body is released.
            assert!(futures::poll!(&mut read).is_pending());
            assert_eq!(dispatches.load(Ordering::Relaxed), 2);
            assert_eq!(spans.load(Ordering::Relaxed), 3);
            drop(held);
            assert_eq!(read.await?.to_bytes(), "fixture");
            assert_eq!(dispatches.load(Ordering::Relaxed), 3);
            assert_eq!(spans.load(Ordering::Relaxed), 3);
            Ok(())
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::try_join!(serve_proxy(listener), client)
        })
        .await??;
        Ok(())
    }

    async fn serve_proxy(listener: TcpListener) -> anyhow::Result<()> {
        for path in ["before", "hold", "after"] {
            let (mut stream, _) = listener.accept().await?;
            let mut lines = BufReader::new(&mut stream).lines();
            assert_eq!(
                lines.next_line().await?.unwrap(),
                format!("GET http://127.0.0.2/{path} HTTP/1.1")
            );
            while let Some(line) = lines.next_line().await? {
                if line.is_empty() {
                    break;
                }
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\nfixture",
                )
                .await?;
        }
        Ok(())
    }
}
