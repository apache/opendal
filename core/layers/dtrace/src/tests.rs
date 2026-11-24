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
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::stream;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use oio::ReadStream;

use super::*;

thread_local! {
    // Tokio's current-thread test runtime keeps operation futures on the test thread.
    static PROBES: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    static PROBE_ARGS: RefCell<Vec<(&'static str, Vec<isize>)>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn record_probe(name: &'static str, args: &[isize]) {
    PROBES.with_borrow_mut(|probes| probes.push(name));
    PROBE_ARGS.with_borrow_mut(|probes| probes.push((name, args.to_vec())));
}

struct TestTransport(Mutex<Option<Result<Response<HttpBody>>>>);

impl HttpTransport for TestTransport {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        assert_eq!(request.method(), Method::PUT);
        assert_eq!(request.uri(), "https://example.com/object?signature=test");
        assert_eq!(request.headers()[http::header::AUTHORIZATION], "test");
        assert_eq!(request.body().to_vec(), b"payload");
        self.0.lock().unwrap().take().expect("one HTTP request")
    }
}

async fn fetch(response: Result<Response<HttpBody>>) -> Result<Response<HttpBody>> {
    let transport = HttpTransporter::new(TestTransport(Mutex::new(Some(response))));
    let context = OperationContext::new().with_http_transport(transport);
    let op = Operator::new(services::Memory::default())
        .unwrap()
        .layer(DtraceLayer::new())
        .with_context(context);
    let request = Request::put("https://example.com/object?signature=test")
        .header(http::header::AUTHORIZATION, "test")
        .body(Buffer::from("payload"))
        .unwrap();
    op.context().http_transport().fetch(request).await
}

#[tokio::test]
async fn forwards_http_response_without_reading_ahead() {
    let chunks_read = Arc::new(AtomicUsize::new(0));
    let chunks = stream::iter(["first", "second"].into_iter().map({
        let chunks_read = chunks_read.clone();
        move |chunk| {
            chunks_read.fetch_add(1, Ordering::Relaxed);
            Ok(Buffer::from(chunk))
        }
    }));
    let response = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(http::header::CONTENT_RANGE, "bytes 0-10/12")
        .body(HttpBody::new(chunks, Some(11)))
        .unwrap();

    let response = fetch(Ok(response)).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers()[http::header::CONTENT_RANGE],
        "bytes 0-10/12"
    );
    assert_eq!(chunks_read.load(Ordering::Relaxed), 0);
    PROBES.with_borrow(|probes| {
        assert_eq!(probes.as_slice(), ["http_request_start", "http_request_ok"]);
    });

    let mut body = response.into_body();
    assert_eq!(body.read().await.unwrap().to_vec(), b"first");
    assert_eq!(chunks_read.load(Ordering::Relaxed), 1);
    assert_eq!(body.read().await.unwrap().to_vec(), b"second");
    assert!(body.read().await.unwrap().is_empty());
    PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            [
                "http_request_start",
                "http_request_ok",
                "http_response_body_chunk",
                "http_response_body_chunk",
                "http_response_body_end",
            ]
        );
    });
}

#[tokio::test]
async fn forwards_http_error_status_and_body_error() {
    let body_error = Error::new(ErrorKind::Unexpected, "test body error").set_temporary();
    let expected = body_error.to_string();
    let response = Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(HttpBody::new(stream::iter([Err(body_error)]), None))
        .unwrap();

    let response = fetch(Ok(response)).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error = response.into_body().read().await.unwrap_err();
    assert_eq!(error.to_string(), expected);
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(error.is_temporary());
    PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            [
                "http_request_start",
                "http_request_ok",
                "http_response_body_error",
            ]
        );
    });
}

#[tokio::test]
async fn preserves_response_body_size_validation() {
    let chunks = stream::iter([Ok(Buffer::from("short"))]);
    let response = Response::new(HttpBody::new(chunks, Some(10)));

    let mut body = fetch(Ok(response)).await.unwrap().into_body();
    assert_eq!(body.read().await.unwrap().to_vec(), b"short");
    let error = body.read().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(error.is_temporary());
}

#[tokio::test]
async fn forwards_http_transport_error() {
    let transport_error = Error::new(ErrorKind::PermissionDenied, "test transport error");
    let expected = transport_error.to_string();

    let error = fetch(Err(transport_error))
        .await
        .err()
        .expect("transport error");
    assert_eq!(error.to_string(), expected);
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            ["http_request_start", "http_request_error"]
        );
    });
}

#[test]
fn reports_nul_probe_arguments_as_empty_c_strings() {
    assert_eq!(probe_cstring("dir/file").to_bytes(), b"dir/file");
    assert_eq!(probe_cstring("dir\0/file").to_bytes(), b"");
}

#[tokio::test]
async fn handles_paths_with_nul_bytes() -> Result<()> {
    let op = Operator::new(services::Memory::default())?.layer(DtraceLayer::new());

    let source = "a\0b";
    let destination = "c\0d";
    let nested = "dir\0/file\0";

    op.write(source, "data").await?;
    assert_eq!(op.read(source).await?.to_vec(), b"data");
    assert!(op.stat(source).await?.is_file());

    op.write(nested, "nested").await?;
    let entries = op.list("dir\0/").await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path(), nested);

    let error = op.copy(source, destination).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    let error = op.rename(source, destination).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    op.create_dir("created\0/").await?;

    let error = op
        .presign_read(source, std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    op.delete(source).await?;
    assert!(!op.exists(source).await?);
    op.delete_with("dir\0/").recursive(true).await?;
    assert!(!op.exists(nested).await?);

    Ok(())
}

#[tokio::test]
async fn traces_lister_entries_and_end_of_stream() -> Result<()> {
    let base = Operator::new(services::Memory::default())?;
    base.write("dir/file", "data").await?;
    let op = base.layer(DtraceLayer::new());

    let mut lister = op.lister("dir/").await?;
    assert_eq!(lister.next().await.transpose()?.unwrap().path(), "dir/file");
    assert!(lister.next().await.is_none());
    PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            [
                "list_start",
                "list_end",
                "lister_next_start",
                "lister_next_ok",
                "lister_next_start",
                "lister_next_ok",
            ]
        );
    });
    Ok(())
}

#[tokio::test]
async fn traces_service_and_io_operations() -> Result<()> {
    let op = Operator::new(services::Memory::default())?.layer(DtraceLayer::new());

    op.create_dir("dir/").await?;
    op.write("dir/file", "data").await?;
    assert!(op.stat("dir/file").await?.is_file());
    assert_eq!(op.read("dir/file").await?.to_vec(), b"data");
    let _ = op.copy("dir/file", "dir/copy").await;
    let _ = op
        .presign_read("dir/file", std::time::Duration::from_secs(60))
        .await;

    PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            [
                "create_dir_start",
                "create_dir_end",
                "write_start",
                "write_end",
                "writer_write_start",
                "writer_write_ok",
                "writer_close_start",
                "writer_close_ok",
                "stat_start",
                "stat_end",
                "read_start",
                "read_end",
                "reader_read_start",
                "reader_read_ok",
                "reader_read_start",
                "reader_read_ok",
                "reader_read_start",
                "reader_read_ok",
                "copy_start",
                "copy_end",
                "presign_start",
                "presign_end",
            ]
        );
    });
    Ok(())
}

#[tokio::test]
async fn traces_deleter_and_preserves_results() -> Result<()> {
    let base = Operator::new(services::Memory::default())?;
    base.write("dir/file", "data").await?;
    let nul_path = "file\0name";
    base.write(nul_path, "data").await?;
    let op = base.clone().layer(DtraceLayer::new());

    let mut deleter = op.deleter().await?;
    let error = deleter.delete("dir/").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert_eq!(error.message(), "directory not empty");
    let options = options::DeleteOptions {
        recursive: true,
        ..Default::default()
    };
    deleter.delete(("dir/".to_owned(), options)).await?;
    deleter.delete(nul_path).await?;
    deleter.close().await?;
    assert!(!base.exists("dir/file").await?);
    assert!(!base.exists(nul_path).await?);
    PROBES.with_borrow(|probes| {
        assert_eq!(
            probes.as_slice(),
            [
                "delete_start",
                "delete_end",
                "deleter_delete_start",
                "deleter_delete_error",
                "deleter_delete_start",
                "deleter_delete_ok",
                "deleter_delete_start",
                "deleter_delete_ok",
                "deleter_close_start",
                "deleter_close_ok",
            ]
        );
    });
    Ok(())
}

#[derive(Debug)]
struct MockWriteService {
    abort_error: Mutex<Option<Error>>,
}

impl Service for MockWriteService {
    type Reader = ();
    type Writer = MockWriter;
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("mock", "/", "mock")
    }

    fn capability(&self) -> Capability {
        Capability {
            write: true,
            write_can_copy_from: true,
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

    fn write(&self, _ctx: &OperationContext, _path: &str, _args: OpWrite) -> Result<Self::Writer> {
        Ok(MockWriter {
            abort_error: self.abort_error.lock().unwrap().take(),
        })
    }
}

struct MockWriter {
    abort_error: Option<Error>,
}

impl oio::Write for MockWriter {
    async fn write(&mut self, _bs: Buffer) -> Result<()> {
        Ok(())
    }

    async fn copy_from(&mut self, _path: &str, _args: OpRead, _range: BytesRange) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        Ok(MetadataBuilder::file(0).build())
    }

    async fn abort(&mut self) -> Result<()> {
        match self.abort_error.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

#[tokio::test]
async fn traces_writer_write_length_and_abort_probes() -> Result<()> {
    let ctx = OperationContext::default();

    let ok_service = DtraceLayer::new().apply_service(Arc::new(MockWriteService {
        abort_error: Mutex::new(None),
    }));
    let mut writer = ok_service.write(&ctx, "dir/file", OpWrite::default())?;
    oio::Write::write(&mut writer, Buffer::from("hello world")).await?;
    oio::Write::copy_from(
        &mut writer,
        "dir/src",
        OpRead::default(),
        BytesRange::new(0, Some(42)),
    )
    .await?;
    oio::Write::copy_from(
        &mut writer,
        "dir/src",
        OpRead::default(),
        BytesRange::from(..),
    )
    .await?;
    oio::Write::abort(&mut writer).await?;

    let err_service = DtraceLayer::new().apply_service(Arc::new(MockWriteService {
        abort_error: Mutex::new(Some(Error::new(ErrorKind::Unexpected, "abort failed"))),
    }));
    let mut failing_writer = err_service.write(&ctx, "dir/file", OpWrite::default())?;
    let err = oio::Write::abort(&mut failing_writer).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);

    PROBE_ARGS.with_borrow(|probes| {
        let names: Vec<_> = probes.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            names,
            [
                "write_start",
                "write_end",
                "writer_write_start",
                "writer_write_ok",
                "writer_write_start",
                "writer_write_ok",
                "writer_write_start",
                "writer_write_ok",
                "writer_abort_start",
                "writer_abort_ok",
                "write_start",
                "write_end",
                "writer_abort_start",
                "writer_abort_error",
            ]
        );

        let write_ok_lengths: Vec<_> = probes
            .iter()
            .filter(|(name, _)| *name == "writer_write_ok")
            .map(|(_, args)| {
                assert_eq!(args.len(), 2, "writer_write_ok must pass (path, length)");
                assert_ne!(args[0], 0);
                args[1]
            })
            .collect();
        assert_eq!(write_ok_lengths, [11, 42, 0]);
    });
    Ok(())
}
