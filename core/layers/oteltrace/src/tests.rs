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
use std::iter;
use std::sync::Mutex;

use futures::stream;
use opendal_service_http::Http;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;

use super::*;

struct RecordingTransport;

impl HttpTransport for RecordingTransport {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        assert_eq!(request.method(), http::Method::GET);
        assert_eq!(request.uri().path(), "/object");
        TraceContext::current()
            .span()
            .add_event("request fetched", vec![]);

        let body = stream::iter(iter::once_with(|| {
            TraceContext::current()
                .span()
                .add_event("body polled", vec![]);
            Ok(Buffer::from("data"))
        }));
        Ok(http::Response::new(HttpBody::new(body, Some(4))))
    }
}

#[tokio::test]
async fn traces_http_fetch_and_body_under_read_operation() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let previous_provider = global::tracer_provider();
    global::set_tracer_provider(provider.clone());

    let context =
        OperationContext::default().with_http_transport(HttpTransporter::new(RecordingTransport));
    let op = Operator::new(Http::default().endpoint("https://example.com"))
        .unwrap()
        .layer(OtelTraceLayer::new())
        .with_context(context);
    let result = op.read("object").await;
    global::set_tracer_provider(previous_provider);
    assert_eq!(result.unwrap().to_vec(), b"data");

    let spans = exporter.get_finished_spans().unwrap();
    let span = |name| {
        spans
            .iter()
            .find(|span| span.name == name)
            .unwrap_or_else(|| panic!("missing {name} span"))
    };
    let read = span("read");
    let reader = span("reader.open");
    let fetch = span("http::fetch");
    assert_eq!(reader.parent_span_id, read.span_context.span_id());
    assert_eq!(fetch.parent_span_id, reader.span_context.span_id());
    assert_eq!(fetch.span_context.trace_id(), read.span_context.trace_id());
    let events: Vec<_> = fetch
        .events
        .iter()
        .map(|event| event.name.as_ref())
        .collect();
    assert_eq!(events, ["request fetched", "body polled"]);
}

#[derive(Clone, Default)]
struct QueuedExecutor {
    task: Arc<Mutex<Option<BoxedStaticFuture<()>>>>,
}

impl Execute for QueuedExecutor {
    fn execute(&self, task: BoxedStaticFuture<()>) {
        *self.task.lock().unwrap() = Some(task);
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        Some(Box::pin(future::ready(())))
    }
}

#[tokio::test]
async fn executor_propagates_submission_context_to_task() {
    let inner = QueuedExecutor::default();
    let context = OperationContext::default().with_executor(Executor::with(inner.clone()));
    let op = Operator::new(services::Memory::default())
        .unwrap()
        .with_context(context)
        .layer(OtelTraceLayer::new());
    let executor = op.context().executor().clone().into_inner();

    let provider = SdkTracerProvider::builder().build();
    let tracer = provider.tracer("test");
    {
        let parent = TraceContext::current_with_span(tracer.start("parent"));
        let expected = parent.span().span_context().clone();
        let _guard = parent.attach();
        executor.execute(Box::pin(async move {
            let current = TraceContext::current();
            assert_eq!(*current.span().span_context(), expected);
        }));
    }

    let task = inner.task.lock().unwrap().take().unwrap();
    task.await;
    executor.timeout().unwrap().await;
}
