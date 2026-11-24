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

use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use fastrace::collector::Config;
use fastrace::collector::SpanRecord;
use fastrace::collector::TestReporter;
use futures::StreamExt;
use http::Method;
use http::Request;
use http::Response;
use http::header::CONTENT_LENGTH;
use opendal_service_http::Http;

use super::*;

fn collect_spans<T>(test: impl Future<Output = T>) -> (T, Vec<SpanRecord>) {
    // Fastrace has a single reporter per process, so serialize reporter swaps.
    static REPORTER_LOCK: Mutex<()> = Mutex::new(());
    let _lock = REPORTER_LOCK.lock().unwrap();
    let (reporter, spans) = TestReporter::new();
    fastrace::set_reporter(reporter, Config::default());
    let output = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(test);
    fastrace::flush();
    (output, spans.lock().clone())
}

fn child_span<'a>(spans: &'a [SpanRecord], parent: &SpanRecord, name: &str) -> &'a SpanRecord {
    spans
        .iter()
        .find(|span| {
            span.name == name
                && span.trace_id == parent.trace_id
                && span.parent_id == parent.span_id
        })
        .unwrap_or_else(|| panic!("missing child {name} of {} in {spans:?}", parent.name))
}

struct TestTransport {
    body_polled: Arc<AtomicBool>,
}

impl HttpTransport for TestTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        tokio::task::yield_now().await;
        let _span = LocalSpan::enter_with_local_parent("test.transport.fetch");
        assert_eq!(req.method(), Method::GET);
        assert_eq!(req.uri().path(), "/file");

        let body_polled = self.body_polled.clone();
        let body = futures::stream::iter([Ok(Buffer::from("data"))]).inspect(move |_| {
            let _span = LocalSpan::enter_with_local_parent("test.transport.body");
            body_polled.store(true, Ordering::Relaxed);
        });
        Ok(Response::builder()
            .header(CONTENT_LENGTH, "4")
            .body(HttpBody::new(body, Some(4)))
            .unwrap())
    }
}

#[test]
fn http_request_and_lazy_body_share_the_operation_trace() {
    let ((), spans) = collect_spans(async {
        async {
            let body_polled = Arc::new(AtomicBool::new(false));
            let transport = HttpTransporter::new(TestTransport {
                body_polled: body_polled.clone(),
            });
            let op = Operator::new(Http::default().endpoint("http://example.test"))
                .unwrap()
                .with_context(OperationContext::new().with_http_transport(transport))
                .layer(FastraceLayer::new());
            let reader = op.reader("file").await.unwrap();
            let mut stream = reader.into_bytes_stream(..).await.unwrap();

            assert_eq!(stream.metadata().await.unwrap().content_length(), 4);
            assert!(!body_polled.load(Ordering::Relaxed));
            assert_eq!(stream.next().await.unwrap().unwrap(), "data");
            assert!(stream.next().await.is_none());
        }
        .in_span(Span::root("test.read", SpanContext::random()))
        .await;
    });

    let root = spans.iter().find(|span| span.name == "test.read").unwrap();
    let reader = child_span(&spans, root, Operation::Read.into_static());
    let open = child_span(&spans, reader, Operation::Read.into_static());
    let http = child_span(&spans, open, "http::fetch");
    child_span(&spans, http, "test.transport.fetch");
    child_span(&spans, http, "test.transport.body");
}

#[test]
fn http_body_span_ends_at_eof_on_error_or_on_drop() {
    let (bodies, spans) = collect_spans(async {
        async {
            let mut finished = FastraceHttpStream {
                inner: futures::stream::empty(),
                span: Some(Span::enter_with_local_parent("test.body.eof")),
            };
            let mut failed = FastraceHttpStream {
                inner: futures::stream::iter([Err(Error::new(
                    ErrorKind::Unexpected,
                    "test body error",
                ))]),
                span: Some(Span::enter_with_local_parent("test.body.error")),
            };
            let dropped = FastraceHttpStream {
                inner: futures::stream::pending::<Result<Buffer>>(),
                span: Some(Span::enter_with_local_parent("test.body.drop")),
            };
            assert!(finished.next().await.is_none());
            assert!(failed.next().await.unwrap().is_err());
            drop(dropped);

            // Keep both terminal streams alive while collecting finished spans.
            (finished, failed)
        }
        .in_span(Span::root("test.body", SpanContext::random()))
        .await
    });

    let root = spans.iter().find(|span| span.name == "test.body").unwrap();
    for name in ["test.body.eof", "test.body.error", "test.body.drop"] {
        child_span(&spans, root, name);
    }
    drop(bodies);
}

struct QueuedExecutor {
    execute: mpsc::Sender<BoxedStaticFuture<()>>,
    try_execute: mpsc::Sender<BoxedStaticFuture<()>>,
}

impl Execute for QueuedExecutor {
    fn execute(&self, task: BoxedStaticFuture<()>) {
        self.execute.send(task).unwrap();
    }

    fn try_execute(&self, task: BoxedStaticFuture<()>) -> Result<()> {
        self.try_execute
            .send(task)
            .map_err(|_| Error::new(ErrorKind::Unexpected, "test task queue is closed"))
    }
}

#[test]
fn executor_propagates_the_submitters_trace() {
    let ((), spans) = collect_spans(async {
        let (execute, executed) = mpsc::channel();
        let (try_execute, submitted) = mpsc::channel();
        let executor = Executor::with(QueuedExecutor {
            execute,
            try_execute,
        });
        let op = Operator::new(services::Memory::default())
            .unwrap()
            .with_context(OperationContext::new().with_executor(executor))
            .layer(FastraceLayer::new());
        let executor = op.context().executor().inner();
        let root = Span::root("test.submit", SpanContext::random());
        {
            let _parent = root.set_local_parent();
            executor.execute(Box::pin(async {
                tokio::task::yield_now().await;
                let _span = LocalSpan::enter_with_local_parent("test.task.execute");
            }));
            executor
                .try_execute(Box::pin(async {
                    tokio::task::yield_now().await;
                    let _span = LocalSpan::enter_with_local_parent("test.task.try_execute");
                }))
                .unwrap();
        }

        // Poll both accepted tasks after the submitting span leaves local scope.
        executed.try_recv().unwrap().await;
        submitted.try_recv().unwrap().await;
        drop(submitted);
        let error = executor
            .try_execute(Box::pin(async {}))
            .expect_err("the earlier executor should reject this task");
        assert!(error.to_string().contains("test task queue is closed"));
    });

    let root = spans
        .iter()
        .find(|span| span.name == "test.submit")
        .unwrap();
    for name in ["test.task.execute", "test.task.try_execute"] {
        let task = spans.iter().find(|span| span.name == name).unwrap();
        let submitted = spans
            .iter()
            .find(|span| span.span_id == task.parent_id && span.trace_id == task.trace_id)
            .unwrap();
        assert_eq!(submitted.name, "executor::execute");
        assert_eq!(submitted.parent_id, root.span_id);
        assert_eq!(submitted.trace_id, root.trace_id);
    }
}

#[derive(Debug)]
struct ComposeTestService;

impl Service for ComposeTestService {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = TestComposer;

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("mock", "/", "mock")
    }

    fn capability(&self) -> Capability {
        Capability {
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

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
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

    fn compose(
        &self,
        _ctx: &OperationContext,
        _to: &str,
        _args: OpCompose,
    ) -> Result<Self::Composer> {
        let _span = LocalSpan::enter_with_local_parent("test.compose.create");
        Ok(TestComposer { count: 0 })
    }
}

struct TestComposer {
    count: u64,
}

impl oio::Compose for TestComposer {
    async fn compose(&mut self, _path: &str, _args: OpRead) -> Result<()> {
        tokio::task::yield_now().await;
        let _span = LocalSpan::enter_with_local_parent("test.composer.compose");
        self.count += 1;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        tokio::task::yield_now().await;
        let _span = LocalSpan::enter_with_local_parent("test.composer.close");
        Ok(MetadataBuilder::file(self.count).build())
    }
}

#[test]
fn composer_operations_inherit_compose_span() {
    let (meta, spans) = collect_spans(async {
        async {
            let service = FastraceLayer::new().apply_service(Arc::new(ComposeTestService));
            let ctx = OperationContext::default();
            let mut composer = service.compose(&ctx, "dst", OpCompose::default()).unwrap();
            oio::Compose::compose(&mut composer, "part1", OpRead::default())
                .await
                .unwrap();
            oio::Compose::close(&mut composer).await.unwrap()
        }
        .in_span(Span::root("test.compose", SpanContext::random()))
        .await
    });
    assert_eq!(meta.content_length(), 1);

    let root = spans
        .iter()
        .find(|span| span.name == "test.compose")
        .unwrap();
    let compose = child_span(&spans, root, Operation::Compose.into_static());
    child_span(&spans, compose, "test.compose.create");

    for leaf in ["test.composer.compose", "test.composer.close"] {
        let leaf_span = spans
            .iter()
            .find(|span| span.name == leaf)
            .unwrap_or_else(|| panic!("missing {leaf} in {spans:?}"));
        let op_span = spans
            .iter()
            .find(|span| span.span_id == leaf_span.parent_id && span.trace_id == leaf_span.trace_id)
            .unwrap();
        assert_eq!(op_span.name, Operation::Compose.into_static());
        assert_eq!(op_span.parent_id, compose.span_id);
        assert_eq!(op_span.trace_id, root.trace_id);
    }
}
