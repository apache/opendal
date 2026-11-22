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

use std::pin::pin;
use std::sync::Mutex;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;

use futures::channel::oneshot;
use futures::future;
use futures::task::noop_waker_ref;
use http::StatusCode;

use super::*;

struct GatedTransport {
    response: Mutex<Option<oneshot::Receiver<Response<HttpBody>>>>,
}

impl HttpTransport for GatedTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        assert_eq!(req.uri().path(), "/object");
        let response = self.response.lock().unwrap().take().unwrap();
        Ok(response.instrument_await("inner transport").await.unwrap())
    }
}

#[test]
fn http_spans_cover_request_and_lazy_body() {
    let (send_response, response) = oneshot::channel();
    let transport = HttpTransporter::new(GatedTransport {
        response: Mutex::new(Some(response)),
    });
    let op = Operator::new(services::Memory::default())
        .unwrap()
        .with_context(OperationContext::from_parts(transport, Executor::default()))
        .layer(AwaitTreeLayer::new());
    let transport = op.context().http_transport();
    let registry = Registry::new(Default::default());
    let mut cx = Context::from_waker(noop_waker_ref());

    let request = Request::get("https://example.com/object")
        .body(Buffer::new())
        .unwrap();
    let root = registry.register("request", "request");
    let mut fetch = pin!(root.instrument(transport.fetch(request)));
    assert!(fetch.as_mut().poll(&mut cx).is_pending());
    let tree = registry.get("request").unwrap().to_string();
    assert!(tree.contains("  opendal::http::fetch ["), "{tree}");
    assert!(tree.contains("    inner transport ["), "{tree}");

    let (send_body, body) = oneshot::channel::<Buffer>();
    let stream = stream::once(async { Ok(body.await.unwrap()) });
    let response = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .body(HttpBody::new(Box::pin(stream), Some(3)))
        .unwrap();
    assert!(send_response.send(response).is_ok());
    let Poll::Ready(Ok(response)) = fetch.as_mut().poll(&mut cx) else {
        panic!("fetch should finish while the response body is still pending");
    };
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);

    let mut body = response.into_body();
    let root = registry.register("reader", "reader");
    let mut read = pin!(root.instrument(body.to_buffer()));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    let tree = registry.get("reader").unwrap().to_string();
    assert!(tree.contains("  opendal::http::body ["), "{tree}");

    send_body.send(Buffer::from("abc")).unwrap();
    let Poll::Ready(Ok(buffer)) = read.as_mut().poll(&mut cx) else {
        panic!("body should finish once the chunk is available");
    };
    assert_eq!(buffer.to_vec(), b"abc");
}

struct QueuedExecutor {
    tasks: mpsc::SyncSender<BoxedStaticFuture<()>>,
}

impl Execute for QueuedExecutor {
    fn execute(&self, task: BoxedStaticFuture<()>) {
        self.tasks.try_send(task).unwrap();
    }

    fn try_execute(&self, task: BoxedStaticFuture<()>) -> Result<()> {
        self.tasks
            .try_send(task)
            .map_err(|_| Error::new(ErrorKind::RateLimited, "executor queue is full"))
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        Some(Box::pin(future::pending()))
    }

    fn sleep(&self, duration: Duration) -> Option<BoxedStaticFuture<()>> {
        (duration == Duration::from_secs(3))
            .then(|| Box::pin(future::ready(())) as BoxedStaticFuture<()>)
    }
}

#[test]
fn executor_preserves_scheduling_rejection_and_timers() {
    let (queue, tasks) = mpsc::sync_channel(2);
    let executor = Executor::with(QueuedExecutor { tasks: queue });
    let op = Operator::new(services::Memory::default())
        .unwrap()
        .with_context(OperationContext::new().with_executor(executor))
        .layer(AwaitTreeLayer::new());
    let executor = op.context().executor().inner();
    let registry = Registry::new(Default::default());
    let mut cx = Context::from_waker(noop_waker_ref());

    let (finish_execute, execute) = oneshot::channel::<()>();
    let (finish_submit, submit) = oneshot::channel::<()>();
    let mut parent = pin!(
        registry
            .register("submitter", "submitter")
            .instrument(async {
                executor.execute(Box::pin(async {
                    execute.instrument_await("executed work").await.unwrap();
                }));
                executor.try_execute(Box::pin(async {
                    submit.instrument_await("submitted work").await.unwrap();
                }))?;
                let err = executor
                    .try_execute(Box::pin(future::ready(())))
                    .unwrap_err();
                assert_eq!(err.kind(), ErrorKind::RateLimited);
                Ok::<(), Error>(())
            })
    );
    assert!(matches!(parent.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));

    let mut executed = tasks.try_recv().unwrap();
    let mut submitted = tasks.try_recv().unwrap();
    assert!(executed.as_mut().poll(&mut cx).is_pending());
    assert!(submitted.as_mut().poll(&mut cx).is_pending());
    let trees: Vec<_> = registry
        .collect_anonymous()
        .into_iter()
        .map(|tree| tree.to_string())
        .collect();
    assert_eq!(trees.len(), 2);
    for work in ["executed work", "submitted work"] {
        assert!(
            trees.iter().any(|tree| {
                tree.starts_with("opendal::executor::task [") && tree.contains(work)
            }),
            "{trees:?}"
        );
    }
    finish_execute.send(()).unwrap();
    finish_submit.send(()).unwrap();
    assert!(executed.as_mut().poll(&mut cx).is_ready());
    assert!(submitted.as_mut().poll(&mut cx).is_ready());

    assert!(
        executor
            .timeout()
            .unwrap()
            .as_mut()
            .poll(&mut cx)
            .is_pending()
    );
    assert!(executor.sleep(Duration::from_secs(2)).is_none());
    let mut sleep = executor.sleep(Duration::from_secs(3)).unwrap();
    assert!(sleep.as_mut().poll(&mut cx).is_ready());
}
