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

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Once;

use futures::executor::block_on;
use futures::stream;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;

use super::*;

const SIGNED_URL: &str = "https://example.com/object?signature=query-secret";
const AUTHORIZATION: &str = "Bearer auth-secret";
const COOKIE: &str = "session=cookie-secret";

thread_local! {
    static HTTP_RECORDS: RefCell<Vec<(Level, String)>> = const { RefCell::new(Vec::new()) };
    static SERVICE_RECORDS: RefCell<Vec<(Level, String)>> = const { RefCell::new(Vec::new()) };
}

struct RecordingLogger;

impl log::Log for RecordingLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        matches!(
            metadata.target(),
            "opendal::services" | "opendal::services::http"
        )
    }

    fn log(&self, record: &log::Record<'_>) {
        match record.target() {
            "opendal::services::http" => {
                HTTP_RECORDS.with_borrow_mut(|records| {
                    records.push((record.level(), record.args().to_string()));
                });
            }
            "opendal::services" => {
                SERVICE_RECORDS.with_borrow_mut(|records| {
                    records.push((record.level(), record.args().to_string()));
                });
            }
            _ => {}
        }
    }

    fn flush(&self) {}
}

fn init_logger() {
    static INIT_LOGGER: Once = Once::new();
    INIT_LOGGER.call_once(|| {
        log::set_logger(&RecordingLogger).unwrap();
        log::set_max_level(log::LevelFilter::Debug);
    });
}

struct FixedTransport(Mutex<Option<Result<Response<HttpBody>>>>);

impl HttpTransport for FixedTransport {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        assert_eq!(request.method(), http::Method::PUT);
        assert_eq!(request.uri(), SIGNED_URL);
        assert_eq!(request.headers()[header::AUTHORIZATION], AUTHORIZATION);
        assert_eq!(request.headers()[header::COOKIE], COOKIE);
        assert_eq!(request.body().to_vec(), b"request payload");
        self.0.lock().unwrap().take().unwrap()
    }
}

fn logging_operator(result: Result<Response<HttpBody>>) -> Operator {
    init_logger();
    let transport = HttpTransporter::new(FixedTransport(Mutex::new(Some(result))));
    Operator::new(services::Memory::default())
        .unwrap()
        .layer(LoggingLayer::default())
        .with_context(OperationContext::default().with_http_transport(transport))
}

fn signed_request() -> Request<Buffer> {
    Request::put(SIGNED_URL)
        .header(header::AUTHORIZATION, AUTHORIZATION)
        .header(header::COOKIE, COOKIE)
        .body(Buffer::from("request payload"))
        .unwrap()
}

fn take_http_records() -> Vec<(Level, String)> {
    let records = HTTP_RECORDS.take();
    for (_, message) in &records {
        assert!(message.contains("service=memory"));
        assert!(message.contains("method=PUT"));
        assert!(!message.contains("example.com"));
        assert!(!message.contains("signature"));
        assert!(!message.contains("auth-secret"));
        assert!(!message.contains("cookie-secret"));
        assert!(!message.contains("request payload"));
    }
    records
}

fn take_service_records() -> Vec<(Level, String)> {
    SERVICE_RECORDS.take()
}

#[test]
fn logs_http_response_status_and_forwards_body() {
    let body = HttpBody::new(stream::iter([Ok(Buffer::from("data"))]), Some(4));
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::CREATED;
    let operator = logging_operator(Ok(response));

    let response = block_on(operator.context().http_transport().send(signed_request())).unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.body().to_vec(), b"data");

    let records = take_http_records();
    let [started, finished] = records.as_slice() else {
        panic!("expected start and response records, got {records:?}");
    };
    assert_eq!(started.0, Level::Debug);
    assert!(started.1.ends_with("http::fetch started"));
    assert_eq!(finished.0, Level::Debug);
    assert!(finished.1.ends_with("status=201: http::fetch finished"));
}

#[test]
fn logs_http_transport_error_without_its_details() {
    let error = Error::new(
        ErrorKind::Unexpected,
        format!("could not reach {SIGNED_URL}"),
    )
    .set_temporary();
    let operator = logging_operator(Err(error));

    let result = block_on(operator.context().http_transport().fetch(signed_request()));
    let Err(error) = result else {
        panic!("expected the inner transport error");
    };
    assert!(error.is_temporary());
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(error.to_string().contains(SIGNED_URL));

    let records = take_http_records();
    let [started, failed] = records.as_slice() else {
        panic!("expected start and error records, got {records:?}");
    };
    assert_eq!(started.0, Level::Debug);
    assert!(started.1.ends_with("http::fetch started"));
    assert_eq!(failed.0, Level::Error);
    assert!(
        failed
            .1
            .ends_with("error_kind=Unexpected: http::fetch failed")
    );
}

#[test]
fn logs_http_body_error_when_polled() {
    let body = HttpBody::new(
        stream::iter([
            Ok(Buffer::from("data")),
            Err(Error::new(ErrorKind::PermissionDenied, AUTHORIZATION)),
        ]),
        None,
    );
    let operator = logging_operator(Ok(Response::new(body)));

    let result = block_on(operator.context().http_transport().fetch(signed_request()));
    let mut body = result.expect("expected an HTTP response").into_body();
    assert_eq!(take_http_records().len(), 2);
    assert_eq!(
        block_on(oio::ReadStream::read(&mut body)).unwrap().to_vec(),
        b"data"
    );
    assert!(take_http_records().is_empty());

    let error = block_on(oio::ReadStream::read(&mut body)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(error.to_string().contains(AUTHORIZATION));

    let records = take_http_records();
    let [failed] = records.as_slice() else {
        panic!("expected one body error record, got {records:?}");
    };
    assert_eq!(failed.0, Level::Warn);
    assert!(
        failed
            .1
            .ends_with("error_kind=PermissionDenied: http::fetch body failed")
    );
}

/// One event recorded by [`RecordingInterceptor`].
#[derive(Debug)]
struct LogEvent {
    operation: Operation,
    context: Vec<(String, String)>,
    message: String,
    err: Option<String>,
}

/// Interceptor that stores every logged event for later assertion.
#[derive(Debug, Clone, Default)]
struct RecordingInterceptor {
    events: Arc<Mutex<Vec<LogEvent>>>,
}

impl RecordingInterceptor {
    fn events(&self) -> Vec<LogEvent> {
        std::mem::take(&mut *self.events.lock().expect("events lock poisoned"))
    }
}

impl LoggingInterceptor for RecordingInterceptor {
    fn log(
        &self,
        _: &ServiceInfo,
        operation: Operation,
        context: &[(&str, &str)],
        message: &str,
        err: Option<&Error>,
    ) {
        let event = LogEvent {
            operation,
            context: context
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            message: message.to_string(),
            err: err.map(ToString::to_string),
        };
        self.events
            .lock()
            .expect("events lock poisoned")
            .push(event);
    }
}

/// Composer that records accepted sources and fails on demand.
#[derive(Debug, Default)]
struct MockComposer {
    accepted: Vec<String>,
    fail_compose: bool,
    fail_close: bool,
}

impl oio::Compose for MockComposer {
    async fn compose(&mut self, path: &str, _: OpRead) -> Result<()> {
        if self.fail_compose {
            return Err(Error::new(ErrorKind::Unexpected, "mock compose failed"));
        }
        self.accepted.push(path.to_string());
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if self.fail_close {
            return Err(Error::new(ErrorKind::Unexpected, "mock close failed"));
        }
        Ok(MetadataBuilder::file(self.accepted.len() as u64).build())
    }
}

/// Copier that reports fixed progress and fails on close on demand.
#[derive(Debug, Default)]
struct MockCopier {
    remaining: Option<usize>,
    fail_close: bool,
}

impl oio::Copy for MockCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        Ok(self.remaining.take())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if self.fail_close {
            return Err(Error::new(ErrorKind::Unexpected, "mock copy close failed"));
        }
        Ok(MetadataBuilder::file(12).build())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Writer that rejects unbounded `copy_from` ranges with an error.
#[derive(Debug, Default)]
struct MockWriter;

impl oio::Write for MockWriter {
    async fn write(&mut self, _: Buffer) -> Result<()> {
        Ok(())
    }

    async fn copy_from(&mut self, _: &str, _: OpRead, range: BytesRange) -> Result<()> {
        if range.size().is_none() {
            return Err(Error::new(
                ErrorKind::RangeNotSatisfied,
                "unbounded copy_from range",
            ));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        Ok(MetadataBuilder::file(0).build())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Service that supports `write`, `compose`, and `copy` for logging tests.
#[derive(Debug, Default)]
struct MockService {
    fail_init: bool,
    fail_compose: bool,
    fail_close: bool,
    copy_bytes: Option<usize>,
}

impl MockService {
    fn unsupported<T>() -> Result<T> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

impl Service for MockService {
    type Reader = ();
    type Writer = MockWriter;
    type Lister = ();
    type Deleter = ();
    type Copier = MockCopier;
    type Composer = MockComposer;

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("mock")
    }

    fn capability(&self) -> Capability {
        Capability::default()
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Self::unsupported()
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        Self::unsupported()
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        Self::unsupported()
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        Ok(MockWriter)
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        Self::unsupported()
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        Self::unsupported()
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Ok(MockCopier {
            remaining: self.copy_bytes,
            fail_close: self.fail_close,
        })
    }

    fn compose(&self, _: &OperationContext, _: &str, _: OpCompose) -> Result<Self::Composer> {
        if self.fail_init {
            return Err(Error::new(ErrorKind::Unexpected, "mock init failed"));
        }
        Ok(MockComposer {
            fail_compose: self.fail_compose,
            fail_close: self.fail_close,
            ..Default::default()
        })
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        Self::unsupported()
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Self::unsupported()
    }
}

/// Wrap `service` with `LoggingLayer` the same way `Operator` does.
fn layered_service(interceptor: &RecordingInterceptor, service: MockService) -> Servicer {
    LoggingLayer::new(interceptor.clone()).apply_service(Arc::new(service))
}

fn assert_context(event: &LogEvent, key: &str, value: &str) {
    assert!(
        event.context.iter().any(|(k, v)| k == key && v == value),
        "expected context {key}={value}, got {:?}",
        event.context
    );
}

#[test]
fn logs_compose_full_lifecycle() {
    let interceptor = RecordingInterceptor::default();
    let srv = layered_service(&interceptor, MockService::default());
    let ctx = OperationContext::default();

    let mut composer = srv
        .compose(&ctx, "dest", OpCompose::default())
        .expect("compose must succeed");
    block_on(composer.compose_dyn("src1", OpRead::default()))
        .expect("first source must be accepted");
    block_on(composer.compose_dyn("src2", OpRead::default()))
        .expect("second source must be accepted");
    block_on(composer.close_dyn()).expect("close must succeed");

    let events = interceptor.events();
    assert_eq!(events.len(), 5, "unexpected events: {events:?}");
    for event in &events {
        assert_eq!(event.operation, Operation::Compose);
        assert_context(event, "to", "dest");
    }

    assert_eq!(events[0].message, "started");
    assert_eq!(events[1].message, "created composer");

    assert_eq!(events[2].message, "accepted source");
    assert_context(&events[2], "from", "src1");
    assert_context(&events[2], "composed", "1");

    assert_eq!(events[3].message, "accepted source");
    assert_context(&events[3], "from", "src2");
    assert_context(&events[3], "composed", "2");

    assert_eq!(events[4].message, "close succeeded");
    assert_context(&events[4], "composed", "2");
}

#[test]
fn logs_compose_source_failure_and_preserves_error() {
    let interceptor = RecordingInterceptor::default();
    let srv = layered_service(
        &interceptor,
        MockService {
            fail_compose: true,
            ..Default::default()
        },
    );
    let ctx = OperationContext::default();

    let mut composer = srv
        .compose(&ctx, "dest", OpCompose::default())
        .expect("compose must succeed");
    let err =
        block_on(composer.compose_dyn("src1", OpRead::default())).expect_err("compose must fail");

    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(err.to_string().contains("mock compose failed"));

    let events = interceptor.events();
    let event = events
        .iter()
        .find(|event| event.message == "failed")
        .expect("a failed event must be logged");
    assert_eq!(event.operation, Operation::Compose);
    assert_context(event, "to", "dest");
    assert_context(event, "from", "src1");
    assert!(
        event
            .err
            .as_deref()
            .unwrap_or("")
            .contains("mock compose failed")
    );
}

#[test]
fn logs_compose_close_failure_and_preserves_error() {
    let interceptor = RecordingInterceptor::default();
    let srv = layered_service(
        &interceptor,
        MockService {
            fail_close: true,
            ..Default::default()
        },
    );
    let ctx = OperationContext::default();

    let mut composer = srv
        .compose(&ctx, "dest", OpCompose::default())
        .expect("compose must succeed");
    block_on(composer.compose_dyn("src1", OpRead::default())).expect("source must be accepted");
    let err = block_on(composer.close_dyn()).expect_err("close must fail");

    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(err.to_string().contains("mock close failed"));

    let events = interceptor.events();
    let event = events
        .iter()
        .find(|event| event.message == "close failed")
        .expect("a close failed event must be logged");
    assert_eq!(event.operation, Operation::Compose);
    assert_context(event, "to", "dest");
    assert_context(event, "composed", "1");
    assert!(
        event
            .err
            .as_deref()
            .unwrap_or("")
            .contains("mock close failed")
    );
}

#[test]
fn logs_compose_init_failure_and_preserves_error() {
    let interceptor = RecordingInterceptor::default();
    let srv = layered_service(
        &interceptor,
        MockService {
            fail_init: true,
            ..Default::default()
        },
    );
    let ctx = OperationContext::default();

    let err = match srv.compose(&ctx, "dest", OpCompose::default()) {
        Err(err) => err,
        Ok(_) => panic!("compose must fail"),
    };

    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(err.to_string().contains("mock init failed"));

    let events = interceptor.events();
    assert_eq!(events.len(), 2, "unexpected events: {events:?}");
    assert_eq!(events[0].operation, Operation::Compose);
    assert_eq!(events[0].message, "started");
    assert_context(&events[0], "to", "dest");
    assert_eq!(events[1].operation, Operation::Compose);
    assert_eq!(events[1].message, "failed");
    assert_context(&events[1], "to", "dest");
    assert!(
        events[1]
            .err
            .as_deref()
            .unwrap_or("")
            .contains("mock init failed")
    );
}

#[test]
fn default_interceptor_logs_expected_error_once_at_debug_level() {
    init_logger();
    let operator = Operator::new(services::Memory::default())
        .unwrap()
        .layer(LoggingLayer::default());

    let _ = take_service_records();
    let err = block_on(operator.stat("missing")).expect_err("stat missing path must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let records = take_service_records();
    let [started, failed] = records.as_slice() else {
        panic!("expected started and failed records without duplicate debug log, got {records:?}");
    };
    assert_eq!(started.0, Level::Debug);
    assert!(started.1.contains("service=memory"));
    assert!(started.1.contains("path=missing"));
    assert!(started.1.ends_with(": stat started"));

    assert_eq!(failed.0, Level::Warn);
    assert!(failed.1.contains("service=memory"));
    assert!(failed.1.contains("path=missing"));
    assert!(failed.1.contains(": stat failed "));
    assert!(failed.1.contains("NotFound"));
}

#[test]
fn default_interceptor_logs_unexpected_error_once_and_success_at_debug_level() {
    init_logger();
    let srv = LoggingLayer::default().apply_service(Arc::new(MockService {
        fail_init: true,
        ..Default::default()
    }));
    let ctx = OperationContext::default();

    let _ = take_service_records();
    let err = match srv.compose(&ctx, "dest", OpCompose::default()) {
        Err(err) => err,
        Ok(_) => panic!("compose must fail"),
    };
    assert_eq!(err.kind(), ErrorKind::Unexpected);

    let records = take_service_records();
    let [started, failed] = records.as_slice() else {
        panic!("expected started and failed records without duplicate debug log, got {records:?}");
    };
    assert_eq!(started.0, Level::Debug);
    assert!(started.1.ends_with(": compose started"));
    assert_eq!(failed.0, Level::Error);
    assert!(failed.1.contains(": compose failed "));
    assert!(failed.1.contains("mock init failed"));

    let operator = Operator::new(services::Memory::default())
        .unwrap()
        .layer(LoggingLayer::default());
    block_on(operator.write("present", "hello")).unwrap();
    let _ = take_service_records();
    block_on(operator.stat("present")).unwrap();

    let records = take_service_records();
    let [started, finished] = records.as_slice() else {
        panic!("expected started and finished debug records, got {records:?}");
    };
    assert_eq!(started.0, Level::Debug);
    assert!(started.1.ends_with(": stat started"));
    assert_eq!(finished.0, Level::Debug);
    assert!(finished.1.ends_with(": stat finished"));
}

#[test]
fn logs_copy_close_success() {
    let interceptor = RecordingInterceptor::default();
    let srv = layered_service(
        &interceptor,
        MockService {
            copy_bytes: Some(12),
            ..Default::default()
        },
    );
    let ctx = OperationContext::default();

    let mut copier = srv
        .copy(&ctx, "src", "dest", OpCopy::default())
        .expect("copy must succeed");
    assert_eq!(
        block_on(copier.next_dyn()).expect("progress step must succeed"),
        Some(12)
    );
    assert_eq!(
        block_on(copier.next_dyn()).expect("completion step must succeed"),
        None
    );
    let meta = block_on(copier.close_dyn()).expect("close must succeed");
    assert_eq!(meta.content_length(), 12);

    let events = interceptor.events();
    assert_eq!(events.len(), 4, "unexpected events: {events:?}");
    for event in &events {
        assert_eq!(event.operation, Operation::Copy);
        assert_context(event, "from", "src");
        assert_context(event, "to", "dest");
    }

    assert_eq!(events[0].message, "started");
    assert_eq!(events[1].message, "created copier");
    assert_eq!(events[2].message, "finished");
    assert_context(&events[2], "copied", "12");
    assert_eq!(events[3].message, "close succeeded");
    assert_context(&events[3], "copied", "12");
    assert!(events[3].err.is_none());
}

#[test]
fn logs_copy_close_failure_and_preserves_error() {
    let interceptor = RecordingInterceptor::default();
    let srv = layered_service(
        &interceptor,
        MockService {
            copy_bytes: Some(12),
            fail_close: true,
            ..Default::default()
        },
    );
    let ctx = OperationContext::default();

    let mut copier = srv
        .copy(&ctx, "src", "dest", OpCopy::default())
        .expect("copy must succeed");
    assert_eq!(
        block_on(copier.next_dyn()).expect("progress step must succeed"),
        Some(12)
    );
    assert_eq!(
        block_on(copier.next_dyn()).expect("completion step must succeed"),
        None
    );
    let err = block_on(copier.close_dyn()).expect_err("close must fail");
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(err.to_string().contains("mock copy close failed"));

    let events = interceptor.events();
    let event = events
        .iter()
        .find(|event| event.message == "close failed")
        .expect("a close failed event must be logged");
    assert_eq!(event.operation, Operation::Copy);
    assert_context(event, "from", "src");
    assert_context(event, "to", "dest");
    assert_context(event, "copied", "12");
    assert!(
        event
            .err
            .as_deref()
            .unwrap_or("")
            .contains("mock copy close failed")
    );
}

#[test]
fn logs_writer_copy_from_unbounded_range_without_panicking() {
    let interceptor = RecordingInterceptor::default();
    let srv = layered_service(&interceptor, MockService::default());
    let ctx = OperationContext::default();

    let mut writer = srv
        .write(&ctx, "dest", OpWrite::default())
        .expect("write must succeed");
    block_on(writer.copy_from_dyn("src", OpRead::default(), BytesRange::from(0..12)))
        .expect("bounded copy_from must succeed");
    let err = block_on(writer.copy_from_dyn("src", OpRead::default(), BytesRange::from(0..)))
        .expect_err("unbounded copy_from must return inner error");

    assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
    assert!(err.to_string().contains("unbounded copy_from range"));

    let events = interceptor.events();
    let event = events
        .iter()
        .find(|event| event.message == "failed")
        .expect("a failed event must be logged");
    assert_eq!(event.operation, Operation::Write);
    assert_context(event, "path", "dest");
    assert_context(event, "source", "src");
    assert_context(event, "written", "12");
    assert!(
        event.context.iter().all(|(key, _)| key != "size"),
        "unbounded copy_from must not log a fabricated size"
    );
    assert!(
        event
            .err
            .as_deref()
            .unwrap_or("")
            .contains("unbounded copy_from range")
    );
}
