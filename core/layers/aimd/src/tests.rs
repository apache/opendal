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

use std::collections::HashMap;
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;

use futures::{channel::oneshot, executor::block_on, poll, stream};
use http::header::{DATE, RETRY_AFTER};
use http::{Request, Response, StatusCode};
use opendal_core::raw::oio::ReadStream;
use opendal_core::*;

use super::*;

#[test]
fn invalid_configuration_is_rejected() {
    let builder = AimdLayer::builder();
    let mut configs = vec![
        builder.clone().initial_rate(0.0),
        builder.clone().min_rate(-1.0),
        builder.clone().initial_rate(6000.0),
        builder.clone().initial_rate(2000.0).min_rate(3000.0),
        builder.clone().min_rate(10.0).max_rate(1.0),
        builder.clone().decrease_factor(1.0),
        builder.clone().additive_increment(0.0),
        builder.clone().burst(0),
        builder.clone().window(Duration::ZERO),
        builder.clone().window(Duration::MAX),
        builder.clone().min_rate(f64::MIN_POSITIVE),
    ];
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        configs.extend([
            builder.clone().initial_rate(value),
            builder.clone().min_rate(value),
            builder.clone().max_rate(value),
            builder.clone().additive_increment(value),
            builder.clone().decrease_factor(value),
        ]);
    }
    for config in configs {
        assert_eq!(config.build().unwrap_err().kind(), ErrorKind::ConfigInvalid);
    }
}

#[test]
fn implicit_initial_rate_respects_the_configured_range() {
    let layer = AimdLayer::builder().max_rate(10.0).build().unwrap();
    assert_eq!(layer.buckets.read.rate(), 10.0);
    let layer = AimdLayer::builder().min_rate(3000.0).build().unwrap();
    assert_eq!(layer.buckets.read.rate(), 3000.0);
}

fn builder() -> AimdLayerBuilder {
    AimdLayer::builder()
        .initial_rate(0.001)
        .min_rate(0.001)
        .max_rate(100.0)
        .additive_increment(10.0)
        .burst(1)
        .window(Duration::from_secs(3600))
        .with_sleep(|_| std::future::pending::<()>())
}

#[derive(Clone, Default)]
struct CountingTransport(Arc<std::sync::atomic::AtomicUsize>);

impl HttpTransport for CountingTransport {
    async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Response::new(HttpBody::new(
            stream::iter([Ok(Buffer::from("a")), Ok(Buffer::from("b"))]),
            Some(2),
        )))
    }
}

#[test]
fn unmarked_and_presign_requests_bypass_admission() {
    block_on(async {
        let layer = builder().build().unwrap();
        let counting = CountingTransport::default();
        let transport = AimdTransport {
            inner: HttpTransporter::new(counting.clone()),
            buckets: layer.buckets.clone(),
            sleep: layer.sleep.clone(),
        };
        layer.buckets.read.acquire(&*layer.sleep).await;
        layer.buckets.write.acquire(&*layer.sleep).await;
        layer.buckets.delete.acquire(&*layer.sleep).await;
        layer.buckets.list.acquire(&*layer.sleep).await;
        transport.fetch(Request::new(Buffer::new())).await.unwrap();
        transport
            .fetch(
                Request::builder()
                    .extension(Operation::Presign)
                    .body(Buffer::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(counting.0.load(std::sync::atomic::Ordering::Relaxed), 2);
    });
}

#[test]
fn every_operation_uses_its_expected_budget() {
    block_on(async {
        let layer = builder().build().unwrap();
        let transport = AimdTransport {
            inner: HttpTransporter::new(CountingTransport::default()),
            buckets: layer.buckets.clone(),
            sleep: layer.sleep.clone(),
        };
        for (bucket, operations) in [
            (&layer.buckets.read, vec![Operation::Read, Operation::Stat]),
            (
                &layer.buckets.write,
                vec![
                    Operation::Write,
                    Operation::Copy,
                    Operation::Compose,
                    Operation::Rename,
                    Operation::Restore,
                    Operation::CreateDir,
                ],
            ),
            (&layer.buckets.delete, vec![Operation::Delete]),
            (&layer.buckets.list, vec![Operation::List]),
        ] {
            bucket.acquire(&*layer.sleep).await;
            for operation in operations {
                let req = Request::builder()
                    .extension(operation)
                    .body(Buffer::new())
                    .unwrap();
                let mut pending = Box::pin(transport.fetch(req));
                assert!(
                    poll!(&mut pending).is_pending(),
                    "{operation} must use this budget"
                );
            }
        }
    });
}

#[test]
fn memory_operations_are_not_paced() {
    block_on(async {
        let layer = builder().build().unwrap();
        layer.buckets.read.acquire(&*layer.sleep).await;
        layer.buckets.write.acquire(&*layer.sleep).await;
        let op = Operator::new(opendal_core::services::Memory::default())
            .unwrap()
            .layer(layer);
        op.write("file", "value").await.unwrap();
        assert_eq!(op.read("file").await.unwrap().to_bytes().as_ref(), b"value");
    });
}

#[test]
fn cloned_layers_and_context_replay_share_request_admission() {
    block_on(async {
        let sleeps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = sleeps.clone();
        let layer = builder()
            .with_sleep(move |duration| {
                assert!(duration > Duration::ZERO);
                calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                std::future::pending::<()>()
            })
            .build()
            .unwrap();
        let counting = CountingTransport::default();
        let context =
            OperationContext::new().with_http_transport(HttpTransporter::new(counting.clone()));
        let first = Operator::new(opendal_core::services::Memory::default())
            .unwrap()
            .with_context(context.clone())
            .layer(layer.clone());
        let second = Operator::new(opendal_core::services::Memory::default())
            .unwrap()
            .layer(layer.clone())
            .with_context(context.clone());
        let mut response = first
            .context()
            .http_transport()
            .fetch(
                Request::builder()
                    .extension(Operation::Read)
                    .body(Buffer::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .body_mut()
                .read_all()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"ab"
        );

        let mut blocked = Box::pin(
            second.context().http_transport().fetch(
                Request::builder()
                    .extension(Operation::Read)
                    .body(Buffer::new())
                    .unwrap(),
            ),
        );
        assert!(poll!(&mut blocked).is_pending());
        drop(blocked);

        let replayed = first.with_context(context);
        let mut blocked = Box::pin(
            replayed.context().http_transport().fetch(
                Request::builder()
                    .extension(Operation::Read)
                    .body(Buffer::new())
                    .unwrap(),
            ),
        );
        assert!(
            poll!(&mut blocked).is_pending(),
            "replay must not reset the bucket"
        );
        assert_eq!(counting.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(sleeps.load(std::sync::atomic::Ordering::Relaxed), 2);
    });
}

#[tokio::test]
async fn default_timer_wakes_request_admission() {
    let start = opendal_core::raw::Instant::now();
    let layer = AimdLayer::builder()
        .initial_rate(100.0)
        .max_rate(100.0)
        .burst(1)
        .build()
        .unwrap();
    let counting = CountingTransport::default();
    let context =
        OperationContext::new().with_http_transport(HttpTransporter::new(counting.clone()));
    let op = Operator::new(opendal_core::services::Memory::default())
        .unwrap()
        .with_context(context)
        .layer(layer);
    for _ in 0..2 {
        op.context()
            .http_transport()
            .fetch(
                Request::builder()
                    .extension(Operation::Read)
                    .body(Buffer::new())
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    assert!(start.elapsed() >= Duration::from_millis(10));
    assert_eq!(counting.0.load(std::sync::atomic::Ordering::Relaxed), 2);
}

type TestResponse = Response<&'static str>;

#[derive(Clone, Default)]
struct ControlledTransport {
    calls: Arc<Mutex<Vec<String>>>,
    responses: Arc<Mutex<HashMap<&'static str, oneshot::Receiver<TestResponse>>>>,
}

impl ControlledTransport {
    fn hold_response(&self, path: &'static str) -> oneshot::Sender<TestResponse> {
        let (send, recv) = oneshot::channel();
        assert!(self.responses.lock().unwrap().insert(path, recv).is_none());
        send
    }

    fn respond(&self, path: &'static str, response: TestResponse) {
        self.hold_response(path).send(response).unwrap();
    }

    fn with_layer(&self, layer: AimdLayer) -> HttpTransporter {
        let context =
            OperationContext::new().with_http_transport(HttpTransporter::new(self.clone()));
        Operator::new(opendal_core::services::Memory::default())
            .unwrap()
            .with_context(context)
            .layer(layer)
            .context()
            .http_transport()
            .clone()
    }
}

impl HttpTransport for ControlledTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        let path = req.uri().path();
        self.calls.lock().unwrap().push(path.to_owned());
        let recv = self.responses.lock().unwrap().remove(path);
        let response = match recv {
            Some(recv) => recv.await.unwrap(),
            None => Response::new("ok"),
        };
        Ok(response.map(|body| {
            HttpBody::new(
                stream::iter([Ok(Buffer::from(body))]),
                Some(body.len() as u64),
            )
        }))
    }
}

fn fixed_rate_layer() -> AimdLayer {
    // Less than one token refills during the pauses below, so premature token
    // deductions remain visible when the deadline expires.
    AimdLayer::builder()
        .min_rate(0.01)
        .max_rate(0.01)
        .burst(1)
        .build()
        .unwrap()
}

fn request(path: &'static str, op: Operation) -> Request<Buffer> {
    Request::builder()
        .uri(path)
        .extension(op)
        .body(Buffer::new())
        .unwrap()
}

fn retry_response(status: StatusCode, hint: &'static str, body: &'static str) -> TestResponse {
    Response::builder()
        .status(status)
        .header(RETRY_AFTER, hint)
        .body(body)
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn retry_after_pauses_all_classes_on_clones_of_the_same_layer() {
    let wire = ControlledTransport::default();
    let layer = fixed_rate_layer();
    let first = wire.with_layer(layer.clone());
    let second = wire.with_layer(layer);
    let independent_wire = ControlledTransport::default();
    let independent = independent_wire.with_layer(fixed_rate_layer());
    wire.respond(
        "/read",
        retry_response(StatusCode::TOO_MANY_REQUESTS, "33", "original 429 body"),
    );

    let mut read = Box::pin(first.fetch(request("/read", Operation::Read)));
    let Poll::Ready(Ok(mut response)) = poll!(&mut read) else {
        panic!("the 429 response must be returned without waiting for cooldown");
    };
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()[RETRY_AFTER], "33");
    assert_eq!(
        response.body_mut().read_all().await.unwrap().to_bytes(),
        "original 429 body"
    );

    let mut requests = [
        Box::pin(second.fetch(request("/write", Operation::Write))),
        Box::pin(second.fetch(request("/delete", Operation::Delete))),
        Box::pin(second.fetch(request("/list", Operation::List))),
    ];
    for request in &mut requests {
        assert!(poll!(request).is_pending());
    }
    let mut independent_list = Box::pin(independent.fetch(request("/list", Operation::List)));
    assert!(matches!(poll!(&mut independent_list), Poll::Ready(Ok(_))));
    assert_eq!(*independent_wire.calls.lock().unwrap(), ["/list"]);

    tokio::time::advance(Duration::from_millis(32_999)).await;
    for request in &mut requests {
        assert!(poll!(request).is_pending());
    }
    assert_eq!(*wire.calls.lock().unwrap(), ["/read"]);

    tokio::time::advance(Duration::from_millis(2)).await;
    for request in &mut requests {
        assert!(matches!(poll!(request), Poll::Ready(Ok(_))));
    }
    assert_eq!(
        *wire.calls.lock().unwrap(),
        ["/read", "/write", "/delete", "/list"]
    );

    let mut stat = Box::pin(first.fetch(request("/stat", Operation::Stat)));
    assert!(
        poll!(&mut stat).is_pending(),
        "the read budget still needs a token"
    );
}

#[tokio::test(start_paused = true)]
async fn retry_after_extends_the_wait_without_changing_fifo_or_cancellation() {
    let wire = ControlledTransport::default();
    let transport = wire.with_layer(fixed_rate_layer());
    let extend_response = wire.hold_response("/extend");
    let shorter_response = wire.hold_response("/shorter");
    let mut extend = Box::pin(transport.fetch(request("/extend", Operation::Write)));
    let mut shorter = Box::pin(transport.fetch(request("/shorter", Operation::Delete)));
    assert!(poll!(&mut extend).is_pending());
    assert!(poll!(&mut shorter).is_pending());
    wire.respond(
        "/read",
        retry_response(StatusCode::TOO_MANY_REQUESTS, "33", "read throttled"),
    );
    transport
        .fetch(request("/read", Operation::Read))
        .await
        .unwrap();

    let mut timed_out = Box::pin(tokio::time::timeout(
        Duration::from_secs(5),
        transport.fetch(request("/timeout", Operation::List)),
    ));
    let mut first = Box::pin(transport.fetch(request("/first", Operation::List)));
    let mut second = Box::pin(transport.fetch(request("/second", Operation::List)));
    assert!(poll!(&mut timed_out).is_pending());
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut second).is_pending());

    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(matches!(poll!(&mut timed_out), Poll::Ready(Err(_))));
    drop(timed_out);
    assert!(poll!(&mut first).is_pending());

    tokio::time::advance(Duration::from_secs(4)).await;
    extend_response
        .send(
            Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(DATE, "Sun, 06 Nov 1994 08:49:37 GMT")
                .header(RETRY_AFTER, "Sun, 06 Nov 1994 08:50:17 GMT")
                .body("original 503 body")
                .unwrap(),
        )
        .unwrap();
    let Poll::Ready(Ok(mut response)) = poll!(&mut extend) else {
        panic!("the 503 response must be returned without waiting for cooldown");
    };
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers()[RETRY_AFTER],
        "Sun, 06 Nov 1994 08:50:17 GMT"
    );
    assert_eq!(
        response.body_mut().read_all().await.unwrap().to_bytes(),
        "original 503 body"
    );

    shorter_response
        .send(retry_response(
            StatusCode::TOO_MANY_REQUESTS,
            "1",
            "still throttled",
        ))
        .unwrap();
    assert!(matches!(poll!(&mut shorter), Poll::Ready(Ok(_))));

    tokio::time::advance(Duration::from_secs(24)).await;
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut second).is_pending());
    assert_eq!(
        *wire.calls.lock().unwrap(),
        ["/extend", "/shorter", "/read"]
    );

    tokio::time::advance(Duration::from_millis(15_999)).await;
    assert!(poll!(&mut first).is_pending());
    tokio::time::advance(Duration::from_millis(2)).await;
    assert!(
        poll!(&mut second).is_pending(),
        "the first live waiter goes first"
    );
    assert!(matches!(poll!(&mut first), Poll::Ready(Ok(_))));
    assert!(
        poll!(&mut second).is_pending(),
        "the list budget only had one token"
    );
    assert_eq!(
        *wire.calls.lock().unwrap(),
        ["/extend", "/shorter", "/read", "/first"]
    );

    tokio::time::advance(Duration::from_millis(100_001)).await;
    assert!(matches!(poll!(&mut second), Poll::Ready(Ok(_))));
    assert_eq!(
        *wire.calls.lock().unwrap(),
        ["/extend", "/shorter", "/read", "/first", "/second"]
    );
}

#[tokio::test(start_paused = true)]
async fn retry_after_requires_a_marked_429_or_503_and_usable_value() {
    let wire = ControlledTransport::default();
    let transport = wire.with_layer(fixed_rate_layer());
    for (op, status, hint, path) in [
        (None, StatusCode::TOO_MANY_REQUESTS, "33", "/unmarked"),
        (Some(Operation::Read), StatusCode::OK, "33", "/ok"),
        (
            Some(Operation::Write),
            StatusCode::TOO_MANY_REQUESTS,
            "+33",
            "/invalid",
        ),
        (
            Some(Operation::Delete),
            StatusCode::SERVICE_UNAVAILABLE,
            "18446744073709551615",
            "/overflow",
        ),
    ] {
        wire.respond(path, retry_response(status, hint, "response"));
        let mut req = Request::builder().uri(path).body(Buffer::new()).unwrap();
        if let Some(op) = op {
            req.extensions_mut().insert(op);
        }
        let mut response = Box::pin(transport.fetch(req));
        let Poll::Ready(Ok(response)) = poll!(&mut response) else {
            panic!("unexpected cooldown before {path}");
        };
        assert_eq!(response.status(), status);
    }
    let mut list = Box::pin(transport.fetch(request("/list", Operation::List)));
    assert!(matches!(poll!(&mut list), Poll::Ready(Ok(_))));
    assert_eq!(
        *wire.calls.lock().unwrap(),
        ["/unmarked", "/ok", "/invalid", "/overflow", "/list"]
    );
}
