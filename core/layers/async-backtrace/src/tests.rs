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

use std::future;
use std::future::Future;
use std::pin::pin;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::*;

#[derive(Default)]
struct TransportPolls {
    fetch: AtomicUsize,
    body: AtomicUsize,
}

struct SuspendingTransport(Arc<TransportPolls>);

impl HttpTransport for SuspendingTransport {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        assert_eq!(request.uri().path(), "/object");
        future::poll_fn(|cx| {
            if self.0.fetch.fetch_add(1, Ordering::Relaxed) == 0 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;

        let polls = self.0.clone();
        let stream =
            futures::stream::poll_fn(move |cx| match polls.body.fetch_add(1, Ordering::Relaxed) {
                0 => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                1 => Poll::Ready(Some(Ok(Buffer::from("body")))),
                _ => Poll::Ready(None),
            });
        Ok(http::Response::new(HttpBody::new(stream, Some(4))))
    }
}

#[test]
fn frames_suspended_http_fetch_and_body_read() {
    let polls = Arc::new(TransportPolls::default());
    let context = OperationContext::default()
        .with_http_transport(HttpTransporter::new(SuspendingTransport(polls.clone())));
    let op = Operator::new(services::Memory::default())
        .unwrap()
        .layer(AsyncBacktraceLayer::new())
        .with_context(context);
    let transport = op.context().http_transport();
    let request = http::Request::get("https://example.com/object")
        .body(Buffer::new())
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());

    let mut fetch = pin!(transport.fetch(request));
    assert!(fetch.as_mut().poll(&mut cx).is_pending());
    assert_eq!(polls.fetch.load(Ordering::Relaxed), 1);
    let dump = async_backtrace::taskdump_tree(true);
    assert!(dump.contains("HttpTransport>::fetch"), "{dump}");
    let Poll::Ready(Ok(response)) = fetch.poll(&mut cx) else {
        panic!("fetch did not return the response");
    };
    assert_eq!(polls.body.load(Ordering::Relaxed), 0);
    let mut body = response.into_body();

    {
        let mut read = pin!(async_backtrace::frame!(body.to_buffer()));
        assert!(read.as_mut().poll(&mut cx).is_pending());
        assert_eq!(polls.body.load(Ordering::Relaxed), 1);
        let dump = async_backtrace::taskdump_tree(true);
        assert!(
            dump.contains("AsyncBacktraceHttpTransport::read_body"),
            "{dump}"
        );
    }
    let dump = async_backtrace::taskdump_tree(true);
    assert!(
        !dump.contains("AsyncBacktraceHttpTransport::read_body"),
        "{dump}"
    );

    let Poll::Ready(Ok(data)) = pin!(body.to_buffer()).poll(&mut cx) else {
        panic!("body could not be read after cancellation");
    };
    assert_eq!(data.to_vec(), b"body");
}

#[derive(Default)]
struct ExecutorState {
    executed: Mutex<Option<BoxedStaticFuture<()>>>,
    submitted: Mutex<Option<BoxedStaticFuture<()>>>,
    sleep_duration: Mutex<Option<Duration>>,
}

struct QueuedExecutor(Arc<ExecutorState>);

impl Execute for QueuedExecutor {
    fn execute(&self, task: BoxedStaticFuture<()>) {
        *self.0.executed.lock().unwrap() = Some(task);
    }

    fn try_execute(&self, task: BoxedStaticFuture<()>) -> Result<()> {
        let mut submitted = self.0.submitted.lock().unwrap();
        if submitted.is_some() {
            return Err(Error::new(
                ErrorKind::RateLimited,
                "submission queue is full",
            ));
        }
        *submitted = Some(task);
        Ok(())
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        Some(Box::pin(future::ready(())))
    }

    fn sleep(&self, duration: Duration) -> Option<BoxedStaticFuture<()>> {
        *self.0.sleep_duration.lock().unwrap() = Some(duration);
        Some(Box::pin(future::ready(())))
    }
}

#[test]
fn frames_tasks_and_preserves_executor_behavior() {
    let state = Arc::new(ExecutorState::default());
    let context =
        OperationContext::default().with_executor(Executor::with(QueuedExecutor(state.clone())));
    let op = Operator::new(services::Memory::default())
        .unwrap()
        .with_context(context)
        .layer(AsyncBacktraceLayer::new());
    let executor = op.context().executor().inner();

    executor.execute(Box::pin(future::pending()));
    executor.try_execute(Box::pin(future::pending())).unwrap();
    let error = executor
        .try_execute(Box::pin(future::pending()))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::RateLimited);

    let mut executed = state.executed.lock().unwrap().take().unwrap();
    let mut submitted = state.submitted.lock().unwrap().take().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(executed.as_mut().poll(&mut cx).is_pending());
    assert!(submitted.as_mut().poll(&mut cx).is_pending());
    let dump = async_backtrace::taskdump_tree(true);
    assert!(dump.contains("Execute>::execute"), "{dump}");
    assert!(dump.contains("Execute>::try_execute"), "{dump}");

    assert!(
        executor
            .timeout()
            .unwrap()
            .as_mut()
            .poll(&mut cx)
            .is_ready()
    );
    let duration = Duration::from_secs(2);
    assert!(
        executor
            .sleep(duration)
            .unwrap()
            .as_mut()
            .poll(&mut cx)
            .is_ready()
    );
    assert_eq!(*state.sleep_duration.lock().unwrap(), Some(duration));
}
