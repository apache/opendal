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
use std::time::Duration;

use futures::stream;
use opendal_service_http::Http;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::SpanData;

use super::*;

fn collect_spans<T>(test: impl Future<Output = T>) -> (T, Vec<SpanData>) {
    // Tests that replace the global tracer provider must run serially.
    static PROVIDER_LOCK: Mutex<()> = Mutex::new(());
    let _lock = PROVIDER_LOCK.lock().unwrap();
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let previous_provider = global::tracer_provider();
    global::set_tracer_provider(provider);
    let result = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(test);
    let spans = exporter.get_finished_spans().unwrap();
    global::set_tracer_provider(previous_provider);
    (result, spans)
}

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

#[test]
fn traces_http_fetch_and_body_under_read_operation() {
    let (result, spans) = collect_spans(async {
        let context = OperationContext::default()
            .with_http_transport(HttpTransporter::new(RecordingTransport));
        let op = Operator::new(Http::default().endpoint("https://example.com"))?
            .layer(OtelTraceLayer::new())
            .with_context(context);
        op.read("object").await
    });
    assert_eq!(result.unwrap().to_vec(), b"data");

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

#[test]
fn traces_operator_delete_and_deleter_in_their_creation_contexts() {
    let (result, spans) = collect_spans(async {
        let memory = Operator::new(services::Memory::default())?;
        memory.write("single", "data").await?;
        memory.write("queued", "data").await?;
        let op = memory.clone().layer(OtelTraceLayer::new());
        let tracer = global::tracer("test");

        let single = TraceContext::new().with_span(tracer.start("test.single"));
        op.delete("single").with_context(single).await?;

        let create = TraceContext::new().with_span(tracer.start("test.deleter"));
        let mut deleter = op.deleter().with_context(create).await?;
        deleter.delete("queued").await?;
        deleter.close().await?;
        drop(deleter);

        Ok::<_, Error>([
            memory.exists("single").await?,
            memory.exists("queued").await?,
        ])
    });
    assert_eq!(result.unwrap(), [false, false]);

    let child = |parent: &SpanData, name| {
        spans
            .iter()
            .find(|span| span.name == name && span.parent_span_id == parent.span_context.span_id())
            .unwrap_or_else(|| panic!("missing {name} span under {}", parent.name))
    };
    for (parent_name, path) in [("test.single", "single"), ("test.deleter", "queued")] {
        let parent = spans.iter().find(|span| span.name == parent_name).unwrap();
        let delete = child(parent, "delete");
        let entry = child(delete, "deleter.delete");
        let close = child(delete, "deleter.close");
        for span in [delete, entry, close] {
            assert_eq!(span.span_context.trace_id(), parent.span_context.trace_id());
        }
        assert!(entry.attributes.contains(&KeyValue::new("path", path)));
        assert!(
            entry
                .attributes
                .iter()
                .any(|attr| attr.key.as_str() == "args")
        );
    }
}

struct FailingDeleter {
    delete_error: Option<Error>,
    close_error: Option<Error>,
}

impl oio::Delete for FailingDeleter {
    async fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
        Err(self.delete_error.take().unwrap())
    }

    async fn close(&mut self) -> Result<()> {
        Err(self.close_error.take().unwrap())
    }
}

#[test]
fn records_deleter_failures_without_changing_errors() {
    let context = OperationContext::default();
    let create_error = Service::delete(&(), &context).unwrap_err();
    let delete_error =
        Error::new(ErrorKind::PermissionDenied, "delete rejected").with_context("path", "object");
    let close_error = Error::new(ErrorKind::RateLimited, "flush rejected").set_temporary();
    let expected = [&delete_error, &close_error, &create_error]
        .map(|err| (err.kind(), err.to_string(), err.is_temporary()));

    let (results, spans) = collect_spans(async {
        let service = OtelTraceLayer::new().apply_service(Arc::new(()));
        let create = service.delete(&context).map(|_| ());
        let mut deleter = OtelTraceWrapper::new(
            TraceContext::new(),
            FailingDeleter {
                delete_error: Some(delete_error),
                close_error: Some(close_error),
            },
        );
        let delete = oio::Delete::delete(&mut deleter, "object", OpDelete::default()).await;
        let close = oio::Delete::close(&mut deleter).await;
        [delete, close, create]
    });

    for ((name, result), (kind, message, temporary)) in
        ["deleter.delete", "deleter.close", "delete"]
            .into_iter()
            .zip(results)
            .zip(expected)
    {
        let err = result.expect_err(name);
        assert_eq!(err.kind(), kind);
        assert_eq!(err.to_string(), message);
        assert_eq!(err.is_temporary(), temporary);

        let span = spans.iter().find(|span| span.name == name).unwrap();
        assert_eq!(span.status, Status::error(kind.into_static()), "{name}");
        let event = span
            .events
            .iter()
            .find(|event| event.name == "exception")
            .unwrap_or_else(|| panic!("missing exception event on {name}"));
        assert!(
            event
                .attributes
                .contains(&KeyValue::new("exception.message", message))
        );
    }
}

#[derive(Clone, Default)]
struct QueuedExecutor {
    executed_task: Arc<Mutex<Option<BoxedStaticFuture<()>>>>,
    submitted_task: Arc<Mutex<Option<BoxedStaticFuture<()>>>>,
}

impl Execute for QueuedExecutor {
    fn execute(&self, task: BoxedStaticFuture<()>) {
        *self.executed_task.lock().unwrap() = Some(task);
    }

    fn try_execute(&self, task: BoxedStaticFuture<()>) -> Result<()> {
        *self.submitted_task.lock().unwrap() = Some(task);
        Ok(())
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        Some(Box::pin(future::ready(())))
    }

    fn sleep(&self, _duration: Duration) -> Option<BoxedStaticFuture<()>> {
        Some(Box::pin(future::ready(())))
    }
}

#[test]
fn executor_delegates_fallible_submission_to_inner_executor() {
    let context = OperationContext::default().with_executor(Executor::with(()));
    let context = OtelTraceLayer::new().apply_context(Arc::new(()), context);

    let err = context
        .executor()
        .inner()
        .try_execute(Box::pin(async {}))
        .expect_err("an unconfigured executor must reject the task");
    assert_eq!(err.kind(), ErrorKind::Unexpected);
}

#[test]
fn executor_delegates_sleep_to_inner_executor() {
    let context =
        OperationContext::default().with_executor(Executor::with(QueuedExecutor::default()));
    let context = OtelTraceLayer::new().apply_context(Arc::new(()), context);

    assert!(
        context
            .executor()
            .inner()
            .sleep(Duration::from_millis(1))
            .is_some(),
        "the inner executor provides timers"
    );
}

#[tokio::test]
async fn executor_try_execute_propagates_submission_context_to_task() {
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
        executor
            .try_execute(Box::pin(async move {
                let current = TraceContext::current();
                assert_eq!(*current.span().span_context(), expected);
            }))
            .unwrap();
    }

    let task = inner
        .submitted_task
        .lock()
        .unwrap()
        .take()
        .expect("try_execute should be delegated directly");
    assert!(
        inner.executed_task.lock().unwrap().is_none(),
        "try_execute must not fall back to execute"
    );
    task.await;
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

    let task = inner.executed_task.lock().unwrap().take().unwrap();
    assert!(
        inner.submitted_task.lock().unwrap().is_none(),
        "execute must not be delegated through try_execute"
    );
    task.await;
    executor.timeout().unwrap().await;
}

#[derive(Debug)]
struct CopyComposeService;

impl Service for CopyComposeService {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = RecordingCopier;
    type Composer = RecordingComposer;

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("mock", "/", "mock")
    }

    fn capability(&self) -> Capability {
        Capability {
            copy: true,
            compose: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unimplemented!()
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        unimplemented!()
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        unimplemented!()
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unimplemented!()
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unimplemented!()
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        unimplemented!()
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unimplemented!()
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unimplemented!()
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        TraceContext::current()
            .span()
            .add_event("copier.create", vec![]);
        Ok(RecordingCopier { done: false })
    }

    fn compose(
        &self,
        _ctx: &OperationContext,
        _to: &str,
        _args: OpCompose,
    ) -> Result<Self::Composer> {
        TraceContext::current()
            .span()
            .add_event("composer.create", vec![]);
        Ok(RecordingComposer { count: 0 })
    }
}

struct RecordingCopier {
    done: bool,
}

impl oio::Copy for RecordingCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        tokio::task::yield_now().await;
        TraceContext::current()
            .span()
            .add_event("copier.next", vec![]);
        let tracer = global::tracer("test");
        let _child = tracer.start("mock.copier.next");
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            Ok(Some(12))
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        tokio::task::yield_now().await;
        TraceContext::current()
            .span()
            .add_event("copier.close", vec![]);
        let tracer = global::tracer("test");
        let _child = tracer.start("mock.copier.close");
        Ok(MetadataBuilder::file(12).build())
    }

    async fn abort(&mut self) -> Result<()> {
        tokio::task::yield_now().await;
        TraceContext::current()
            .span()
            .add_event("copier.abort", vec![]);
        let tracer = global::tracer("test");
        let _child = tracer.start("mock.copier.abort");
        Ok(())
    }
}

struct RecordingComposer {
    count: u64,
}

impl oio::Compose for RecordingComposer {
    async fn compose(&mut self, _path: &str, _args: OpRead) -> Result<()> {
        tokio::task::yield_now().await;
        TraceContext::current()
            .span()
            .add_event("composer.compose", vec![]);
        let tracer = global::tracer("test");
        let _child = tracer.start("mock.composer.compose");
        self.count += 1;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        tokio::task::yield_now().await;
        TraceContext::current()
            .span()
            .add_event("composer.close", vec![]);
        let tracer = global::tracer("test");
        let _child = tracer.start("mock.composer.close");
        Ok(MetadataBuilder::file(self.count).build())
    }
}

#[test]
fn traces_copy_and_compose_across_stateful_bodies() {
    let (result, spans) = collect_spans(async {
        let service = OtelTraceLayer::new().apply_service(Arc::new(CopyComposeService));
        let ctx = OperationContext::default();
        let tracer = global::tracer("test");

        let copy_cx = TraceContext::new().with_span(tracer.start("test.copy"));
        let mut copier = {
            let _guard = copy_cx.attach();
            service.copy(&ctx, "src", "dst", OpCopy::default())?
        };
        assert_eq!(oio::Copy::next(&mut copier).await?, Some(12));
        assert_eq!(oio::Copy::next(&mut copier).await?, None);
        let copy_meta = oio::Copy::close(&mut copier).await?;
        drop(copier);

        let abort_cx = TraceContext::new().with_span(tracer.start("test.copy_abort"));
        let mut aborted = {
            let _guard = abort_cx.attach();
            service.copy(&ctx, "src", "dst2", OpCopy::default())?
        };
        oio::Copy::abort(&mut aborted).await?;
        drop(aborted);

        let compose_cx = TraceContext::new().with_span(tracer.start("test.compose"));
        let mut composer = {
            let _guard = compose_cx.attach();
            service.compose(&ctx, "out", OpCompose::default())?
        };
        oio::Compose::compose(&mut composer, "part1", OpRead::default()).await?;
        let compose_meta = oio::Compose::close(&mut composer).await?;
        drop(composer);

        Ok::<_, Error>((copy_meta.content_length(), compose_meta.content_length()))
    });
    assert_eq!(result.unwrap(), (12, 1));

    let child = |parent: &SpanData, name| {
        spans
            .iter()
            .find(|span| span.name == name && span.parent_span_id == parent.span_context.span_id())
            .unwrap_or_else(|| panic!("missing {name} span under {}", parent.name))
    };

    let copy_root = spans.iter().find(|s| s.name == "test.copy").unwrap();
    let copy_span = child(copy_root, "copy");
    assert!(copy_span.attributes.contains(&KeyValue::new("from", "src")));
    assert!(copy_span.attributes.contains(&KeyValue::new("to", "dst")));
    let copy_events: Vec<_> = copy_span.events.iter().map(|e| e.name.as_ref()).collect();
    assert_eq!(
        copy_events,
        [
            "copier.create",
            "copier.next",
            "copier.next",
            "copier.close"
        ]
    );
    child(copy_span, "mock.copier.next");
    child(copy_span, "mock.copier.close");

    let abort_root = spans.iter().find(|s| s.name == "test.copy_abort").unwrap();
    let abort_span = child(abort_root, "copy");
    let abort_events: Vec<_> = abort_span.events.iter().map(|e| e.name.as_ref()).collect();
    assert_eq!(abort_events, ["copier.create", "copier.abort"]);
    child(abort_span, "mock.copier.abort");

    let compose_root = spans.iter().find(|s| s.name == "test.compose").unwrap();
    let compose_span = child(compose_root, "compose");
    assert!(
        compose_span
            .attributes
            .contains(&KeyValue::new("to", "out"))
    );
    let compose_events: Vec<_> = compose_span
        .events
        .iter()
        .map(|e| e.name.as_ref())
        .collect();
    assert_eq!(
        compose_events,
        ["composer.create", "composer.compose", "composer.close"]
    );
    child(compose_span, "mock.composer.compose");
    child(compose_span, "mock.composer.close");
}
