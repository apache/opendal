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

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use opendal_core::raw::oio::Read as _;
use opendal_core::raw::oio::Write as _;
use opendal_core::raw::*;
use opendal_core::*;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::body::Body;
use tonic::codegen::{Service as TowerService, http};
use tonic::server::{ClientStreamingService, Grpc, ServerStreamingService, UnaryService};
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status, Streaming};
use tonic_prost::ProstCodec;

use crate::GcsGrpc;
use crate::generated::google::storage::v2::*;

const ALIGNMENT: usize = 256 * 1024;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
enum WriteObjectAction {
    /// Persist all streamed bytes; finalize if finish_write is set.
    Normal,
    /// Persist only the first `n` bytes of the chunk and return the new persisted size.
    PartialPersist(usize),
    /// Return a fixed PersistedSize without advancing stored session bytes.
    PersistedSize(i64),
    /// Fail the WriteObject RPC with the given status.
    Fail(tonic::Code, &'static str),
}

#[derive(Debug, Clone)]
enum QueryStatusAction {
    Normal,
    NotFound,
}

#[derive(Debug, Clone)]
struct RecordedWriteCall {
    write_offset: i64,
    bytes_len: usize,
    finish_write: bool,
}

#[derive(Default)]
struct MockStorageState {
    next_upload_seq: usize,
    write_actions: VecDeque<WriteObjectAction>,
    query_actions: VecDeque<QueryStatusAction>,

    sessions: HashMap<String, (String, Vec<u8>)>,
    objects: HashMap<String, Vec<u8>>,

    start_calls: usize,
    write_calls: Vec<RecordedWriteCall>,
    query_calls: Vec<String>,
    cancel_calls: Vec<String>,
    read_calls: Vec<ReadObjectRequest>,
    get_calls: Vec<GetObjectRequest>,
    delete_calls: Vec<DeleteObjectRequest>,
    rewrite_calls: Vec<RewriteObjectRequest>,
}

impl MockStorageState {
    fn total_object_rpc_calls(&self) -> usize {
        self.read_calls.len()
            + self.get_calls.len()
            + self.delete_calls.len()
            + self.rewrite_calls.len()
    }
}

#[derive(Clone)]
struct MockStorageService {
    state: Arc<Mutex<MockStorageState>>,
}

impl MockStorageService {
    async fn handle_start_resumable_write(
        &self,
        req: Request<StartResumableWriteRequest>,
    ) -> std::result::Result<Response<StartResumableWriteResponse>, Status> {
        let mut state = self.state.lock().unwrap();
        state.start_calls += 1;
        state.next_upload_seq += 1;
        let upload_id = format!("upload-{}", state.next_upload_seq);
        let name = req
            .into_inner()
            .write_object_spec
            .and_then(|spec| spec.resource)
            .map(|obj| obj.name)
            .unwrap_or_default();
        state.sessions.insert(upload_id.clone(), (name, Vec::new()));
        Ok(Response::new(StartResumableWriteResponse { upload_id }))
    }

    async fn handle_write_object(
        &self,
        req: Request<Streaming<WriteObjectRequest>>,
    ) -> std::result::Result<Response<WriteObjectResponse>, Status> {
        let mut stream = req.into_inner();
        let mut upload_id: Option<String> = None;
        let mut spec_object_name: Option<String> = None;
        let mut first_offset: Option<i64> = None;
        let mut finish_write = false;
        let mut payload = Vec::new();

        while let Some(msg) = stream.next().await {
            let msg = msg?;
            if first_offset.is_none() {
                first_offset = Some(msg.write_offset);
            }
            match msg.first_message {
                Some(write_object_request::FirstMessage::UploadId(id)) => {
                    upload_id = Some(id);
                }
                Some(write_object_request::FirstMessage::WriteObjectSpec(spec)) => {
                    spec_object_name = spec.resource.map(|obj| obj.name);
                }
                None => {}
            }
            if let Some(write_object_request::Data::ChecksummedData(data)) = msg.data {
                payload.extend_from_slice(&data.content);
            }
            if msg.finish_write {
                finish_write = true;
            }
        }

        let write_offset = first_offset.unwrap_or(0);
        let mut state = self.state.lock().unwrap();
        state.write_calls.push(RecordedWriteCall {
            write_offset,
            bytes_len: payload.len(),
            finish_write,
        });

        let action = state
            .write_actions
            .pop_front()
            .unwrap_or(WriteObjectAction::Normal);
        match action {
            WriteObjectAction::Fail(code, msg) => Err(Status::new(code, msg)),
            WriteObjectAction::PersistedSize(size) => Ok(Response::new(WriteObjectResponse {
                write_status: Some(write_object_response::WriteStatus::PersistedSize(size)),
            })),
            WriteObjectAction::PartialPersist(n) => {
                let Some(upload_id) = upload_id else {
                    return Err(Status::invalid_argument(
                        "partial persist requires upload_id",
                    ));
                };
                let Some((_, buf)) = state.sessions.get_mut(&upload_id) else {
                    return Err(Status::not_found("upload session not found"));
                };
                if buf.len() as i64 != write_offset {
                    return Err(Status::out_of_range("write_offset mismatch"));
                }
                let take = n.min(payload.len());
                buf.extend_from_slice(&payload[..take]);
                Ok(Response::new(WriteObjectResponse {
                    write_status: Some(write_object_response::WriteStatus::PersistedSize(
                        buf.len() as i64,
                    )),
                }))
            }
            WriteObjectAction::Normal => {
                if let Some(upload_id) = upload_id {
                    let Some((name, buf)) = state.sessions.get_mut(&upload_id) else {
                        return Err(Status::not_found("upload session not found"));
                    };
                    if buf.len() as i64 != write_offset {
                        return Err(Status::out_of_range("write_offset mismatch"));
                    }
                    buf.extend_from_slice(&payload);
                    let new_size = buf.len() as i64;
                    if finish_write {
                        let name = name.clone();
                        let data = buf.clone();
                        state.sessions.remove(&upload_id);
                        state.objects.insert(name.clone(), data);
                        Ok(Response::new(WriteObjectResponse {
                            write_status: Some(write_object_response::WriteStatus::Resource(
                                Object {
                                    name,
                                    size: new_size,
                                    generation: 1,
                                    ..Default::default()
                                },
                            )),
                        }))
                    } else {
                        Ok(Response::new(WriteObjectResponse {
                            write_status: Some(write_object_response::WriteStatus::PersistedSize(
                                new_size,
                            )),
                        }))
                    }
                } else {
                    let name = spec_object_name.unwrap_or_default();
                    let size = payload.len() as i64;
                    state.objects.insert(name.clone(), payload);
                    Ok(Response::new(WriteObjectResponse {
                        write_status: Some(write_object_response::WriteStatus::Resource(Object {
                            name,
                            size,
                            generation: 1,
                            ..Default::default()
                        })),
                    }))
                }
            }
        }
    }

    async fn handle_query_write_status(
        &self,
        req: Request<QueryWriteStatusRequest>,
    ) -> std::result::Result<Response<QueryWriteStatusResponse>, Status> {
        let upload_id = req.into_inner().upload_id;
        let mut state = self.state.lock().unwrap();
        state.query_calls.push(upload_id.clone());

        match state
            .query_actions
            .pop_front()
            .unwrap_or(QueryStatusAction::Normal)
        {
            QueryStatusAction::NotFound => {
                state.sessions.remove(&upload_id);
                Err(Status::not_found("upload session expired"))
            }
            QueryStatusAction::Normal => {
                let Some((_, buf)) = state.sessions.get(&upload_id) else {
                    return Err(Status::not_found("upload session not found"));
                };
                Ok(Response::new(QueryWriteStatusResponse {
                    write_status: Some(query_write_status_response::WriteStatus::PersistedSize(
                        buf.len() as i64,
                    )),
                }))
            }
        }
    }

    async fn handle_cancel_resumable_write(
        &self,
        req: Request<CancelResumableWriteRequest>,
    ) -> std::result::Result<Response<CancelResumableWriteResponse>, Status> {
        let upload_id = req.into_inner().upload_id;
        let mut state = self.state.lock().unwrap();
        state.cancel_calls.push(upload_id.clone());
        state.sessions.remove(&upload_id);
        Ok(Response::new(CancelResumableWriteResponse {}))
    }

    async fn handle_read_object(
        &self,
        req: Request<ReadObjectRequest>,
    ) -> std::result::Result<
        Response<
            Pin<
                Box<
                    dyn futures::Stream<Item = std::result::Result<ReadObjectResponse, Status>>
                        + Send,
                >,
            >,
        >,
        Status,
    > {
        let req = req.into_inner();
        let mut state = self.state.lock().unwrap();
        state.read_calls.push(req.clone());

        let Some(content) = state.objects.get(&req.object).cloned() else {
            return Err(Status::not_found("object not found"));
        };
        let len = content.len() as i64;
        let start = if req.read_offset < 0 {
            (len + req.read_offset).max(0) as usize
        } else if req.read_offset > len || (req.read_offset == len && len > 0) {
            return Err(Status::out_of_range("read_offset out of range"));
        } else {
            req.read_offset as usize
        };
        let end = if req.read_limit == 0 {
            content.len()
        } else {
            start
                .saturating_add(req.read_limit as usize)
                .min(content.len())
        };
        let slice = Bytes::copy_from_slice(&content[start..end]);
        let response = ReadObjectResponse {
            checksummed_data: Some(ChecksummedData { content: slice }),
        };
        let stream: Pin<
            Box<dyn futures::Stream<Item = std::result::Result<ReadObjectResponse, Status>> + Send>,
        > = Box::pin(futures::stream::iter(vec![Ok(response)]));
        Ok(Response::new(stream))
    }

    async fn handle_get_object(
        &self,
        req: Request<GetObjectRequest>,
    ) -> std::result::Result<Response<Object>, Status> {
        let req = req.into_inner();
        let mut state = self.state.lock().unwrap();
        state.get_calls.push(req.clone());
        let Some(content) = state.objects.get(&req.object) else {
            return Err(Status::not_found("object not found"));
        };
        Ok(Response::new(Object {
            name: req.object,
            size: content.len() as i64,
            generation: if req.generation > 0 {
                req.generation
            } else {
                1
            },
            ..Default::default()
        }))
    }

    async fn handle_delete_object(
        &self,
        req: Request<DeleteObjectRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        let req = req.into_inner();
        let mut state = self.state.lock().unwrap();
        state.delete_calls.push(req.clone());
        state.objects.remove(&req.object);
        Ok(Response::new(()))
    }

    async fn handle_rewrite_object(
        &self,
        req: Request<RewriteObjectRequest>,
    ) -> std::result::Result<Response<RewriteResponse>, Status> {
        let req = req.into_inner();
        let mut state = self.state.lock().unwrap();
        state.rewrite_calls.push(req.clone());
        let Some(content) = state.objects.get(&req.source_object).cloned() else {
            return Err(Status::not_found("source object not found"));
        };
        let size = content.len() as i64;
        state.objects.insert(req.destination_name.clone(), content);
        Ok(Response::new(RewriteResponse {
            done: true,
            rewrite_token: String::new(),
            resource: Some(Object {
                name: req.destination_name,
                size,
                generation: 2,
                ..Default::default()
            }),
        }))
    }
}

struct UnaryHandler<F>(F);
impl<Req, Res, Fut, F> UnaryService<Req> for UnaryHandler<F>
where
    F: FnMut(Request<Req>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<Response<Res>, Status>>,
{
    type Response = Res;
    type Future = Fut;

    fn call(&mut self, request: Request<Req>) -> Self::Future {
        (self.0)(request)
    }
}

struct ClientStreamHandler<F>(F);
impl<Req, Res, Fut, F> ClientStreamingService<Req> for ClientStreamHandler<F>
where
    F: FnMut(Request<Streaming<Req>>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<Response<Res>, Status>>,
{
    type Response = Res;
    type Future = Fut;

    fn call(&mut self, request: Request<Streaming<Req>>) -> Self::Future {
        (self.0)(request)
    }
}

struct ServerStreamHandler<F>(F);
impl<Req, Res, S, Fut, F> ServerStreamingService<Req> for ServerStreamHandler<F>
where
    F: FnMut(Request<Req>) -> Fut,
    S: futures::Stream<Item = std::result::Result<Res, Status>>,
    Fut: std::future::Future<Output = std::result::Result<Response<S>, Status>>,
{
    type Response = Res;
    type ResponseStream = S;
    type Future = Fut;

    fn call(&mut self, request: Request<Req>) -> Self::Future {
        (self.0)(request)
    }
}

impl TowerService<http::Request<Body>> for MockStorageService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Self::Response, Self::Error>>
                + Send,
        >,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move {
            let response = match req.uri().path() {
                "/google.storage.v2.Storage/StartResumableWrite" => {
                    let mut grpc = Grpc::new(ProstCodec::default());
                    grpc.unary(
                        UnaryHandler(move |r| {
                            let svc = svc.clone();
                            async move { svc.handle_start_resumable_write(r).await }
                        }),
                        req,
                    )
                    .await
                }
                "/google.storage.v2.Storage/WriteObject" => {
                    let mut grpc = Grpc::new(ProstCodec::default());
                    grpc.client_streaming(
                        ClientStreamHandler(move |r| {
                            let svc = svc.clone();
                            async move { svc.handle_write_object(r).await }
                        }),
                        req,
                    )
                    .await
                }
                "/google.storage.v2.Storage/QueryWriteStatus" => {
                    let mut grpc = Grpc::new(ProstCodec::default());
                    grpc.unary(
                        UnaryHandler(move |r| {
                            let svc = svc.clone();
                            async move { svc.handle_query_write_status(r).await }
                        }),
                        req,
                    )
                    .await
                }
                "/google.storage.v2.Storage/CancelResumableWrite" => {
                    let mut grpc = Grpc::new(ProstCodec::default());
                    grpc.unary(
                        UnaryHandler(move |r| {
                            let svc = svc.clone();
                            async move { svc.handle_cancel_resumable_write(r).await }
                        }),
                        req,
                    )
                    .await
                }
                "/google.storage.v2.Storage/ReadObject" => {
                    let mut grpc = Grpc::new(ProstCodec::default());
                    grpc.server_streaming(
                        ServerStreamHandler(move |r| {
                            let svc = svc.clone();
                            async move { svc.handle_read_object(r).await }
                        }),
                        req,
                    )
                    .await
                }
                "/google.storage.v2.Storage/GetObject" => {
                    let mut grpc = Grpc::new(ProstCodec::default());
                    grpc.unary(
                        UnaryHandler(move |r| {
                            let svc = svc.clone();
                            async move { svc.handle_get_object(r).await }
                        }),
                        req,
                    )
                    .await
                }
                "/google.storage.v2.Storage/DeleteObject" => {
                    let mut grpc = Grpc::new(ProstCodec::default());
                    grpc.unary(
                        UnaryHandler(move |r| {
                            let svc = svc.clone();
                            async move { svc.handle_delete_object(r).await }
                        }),
                        req,
                    )
                    .await
                }
                "/google.storage.v2.Storage/RewriteObject" => {
                    let mut grpc = Grpc::new(ProstCodec::default());
                    grpc.unary(
                        UnaryHandler(move |r| {
                            let svc = svc.clone();
                            async move { svc.handle_rewrite_object(r).await }
                        }),
                        req,
                    )
                    .await
                }
                path => Status::unimplemented(format!("unhandled RPC {path}")).into_http(),
            };
            Ok(response)
        })
    }
}

struct MockServer {
    endpoint: String,
    state: Arc<Mutex<MockStorageState>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl MockServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockStorageState::default()));
        let svc = MockStorageService {
            state: state.clone(),
        };
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = Server::builder()
                .serve_with_incoming_shutdown(svc, TcpIncoming::from(listener), async {
                    let _ = rx.await;
                })
                .await;
        });
        Self {
            endpoint: format!("http://{addr}"),
            state,
            shutdown: Some(tx),
        }
    }

    fn builder(&self) -> GcsGrpc {
        GcsGrpc::default()
            .bucket("test-bucket")
            .endpoint(&self.endpoint)
            .skip_signature()
    }

    fn operator(&self) -> Operator {
        Operator::new(self.builder()).unwrap()
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

#[tokio::test]
async fn gcs_grpc_1_zero_progress_persisted_size_fails_with_temporary_error() {
    let server = MockServer::start().await;
    server
        .state
        .lock()
        .unwrap()
        .write_actions
        .push_back(WriteObjectAction::PersistedSize(0));

    let backend = server.builder().build().unwrap();
    let mut writer = backend
        .write(&OperationContext::default(), "obj", OpWrite::default())
        .unwrap();

    // First write stays buffered on the one-shot path.
    writer
        .write(Buffer::from(Bytes::from_static(b"head")))
        .await
        .unwrap();

    // Second write triggers resumable upload of 256 KiB; server reports PersistedSize(0).
    let chunk = Buffer::from(vec![b'a'; ALIGNMENT]);
    let err = tokio::time::timeout(TEST_TIMEOUT, writer.write(chunk.clone()))
        .await
        .expect("drive_pending must not spin forever on zero-progress PersistedSize")
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(err.is_temporary());

    {
        let state = server.state.lock().unwrap();
        assert_eq!(state.write_calls.len(), 1);
        assert_eq!(state.write_calls[0].write_offset, 0);
        assert_eq!(state.write_calls[0].bytes_len, ALIGNMENT);
        assert!(!state.write_calls[0].finish_write);
        assert!(state.query_calls.is_empty());
    }

    // Retrying with the same buffer succeeds cleanly without a redundant QueryWriteStatus.
    tokio::time::timeout(TEST_TIMEOUT, writer.write(chunk))
        .await
        .unwrap()
        .unwrap();
    let meta = tokio::time::timeout(TEST_TIMEOUT, writer.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meta.content_length(), (ALIGNMENT + 4) as u64);

    let state = server.state.lock().unwrap();
    assert!(state.query_calls.is_empty());
    let stored = state.objects.get("obj").unwrap();
    assert_eq!(stored.len(), ALIGNMENT + 4);
    assert_eq!(&stored[..4], b"head");
    assert!(stored[4..].iter().all(|&b| b == b'a'));
}

#[tokio::test]
async fn gcs_grpc_1_zero_progress_persisted_size_on_non_empty_final_write_fails() {
    let server = MockServer::start().await;
    {
        let mut state = server.state.lock().unwrap();
        // First resumable chunk succeeds normally; final non-empty write makes zero progress.
        state.write_actions.push_back(WriteObjectAction::Normal);
        state
            .write_actions
            .push_back(WriteObjectAction::PersistedSize(ALIGNMENT as i64));
    }

    let backend = server.builder().build().unwrap();
    let mut writer = backend
        .write(&OperationContext::default(), "final", OpWrite::default())
        .unwrap();

    writer
        .write(Buffer::from(Bytes::from_static(b"tail")))
        .await
        .unwrap();
    writer
        .write(Buffer::from(vec![b'b'; ALIGNMENT]))
        .await
        .unwrap();

    let err = tokio::time::timeout(TEST_TIMEOUT, writer.close())
        .await
        .expect("close must not spin forever on zero-progress PersistedSize")
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(err.is_temporary());
    assert_eq!(server.state.lock().unwrap().write_calls.len(), 2);

    // Retrying close now succeeds and finalizes the object.
    let meta = tokio::time::timeout(TEST_TIMEOUT, writer.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meta.content_length(), (ALIGNMENT + 4) as u64);
    assert_eq!(
        server
            .state
            .lock()
            .unwrap()
            .objects
            .get("final")
            .unwrap()
            .len(),
        ALIGNMENT + 4
    );
}

#[tokio::test]
async fn gcs_grpc_1_query_not_found_after_persisted_offset_fails_permanently() {
    let server = MockServer::start().await;
    {
        let mut state = server.state.lock().unwrap();
        state.write_actions.push_back(WriteObjectAction::Normal);
        state.write_actions.push_back(WriteObjectAction::Fail(
            tonic::Code::Unavailable,
            "transient",
        ));
        state.query_actions.push_back(QueryStatusAction::NotFound);
    }

    let backend = server.builder().build().unwrap();
    let mut writer = backend
        .write(&OperationContext::default(), "lost", OpWrite::default())
        .unwrap();

    writer
        .write(Buffer::from(Bytes::from_static(b"0")))
        .await
        .unwrap();
    writer
        .write(Buffer::from(vec![b'a'; ALIGNMENT]))
        .await
        .unwrap();

    let chunk2 = Buffer::from(vec![b'b'; ALIGNMENT]);
    let err = writer.write(chunk2.clone()).await.unwrap_err();
    assert!(err.is_temporary());

    // QueryWriteStatus returns NotFound at write_offset == ALIGNMENT (> 0): must fail without restarting.
    let err = writer.write(chunk2).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(!err.is_temporary());
    assert_eq!(server.state.lock().unwrap().start_calls, 1);
}

#[tokio::test]
async fn gcs_grpc_2_close_after_failed_write_does_not_commit_unacknowledged_bytes() {
    let server = MockServer::start().await;
    server
        .state
        .lock()
        .unwrap()
        .write_actions
        .push_back(WriteObjectAction::Fail(
            tonic::Code::Unavailable,
            "transient",
        ));

    // Exercise through Operator so CompleteLayer is also in the stack.
    let op = server.operator();
    let mut writer = op.writer("uncommitted").await.unwrap();

    writer.write(Bytes::from_static(b"first")).await.unwrap();
    let chunk2 = Bytes::from(vec![b'z'; ALIGNMENT]);
    let err = writer.write(chunk2.clone()).await.unwrap_err();
    assert!(err.is_temporary());

    // Calling close() after a failed write MUST error without committing to GCS.
    let err = writer.close().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(!err.is_temporary());

    {
        let state = server.state.lock().unwrap();
        assert!(
            !state.objects.contains_key("uncommitted"),
            "failed write bytes must not be committed on close"
        );
        assert!(
            state.write_calls.iter().all(|call| !call.finish_write),
            "close must not send finish_write when a non-final write is pending"
        );
        assert_eq!(state.write_calls.len(), 1);
    }

    // Aborting cleans up the resumable upload session.
    writer.abort().await.unwrap();
    let state = server.state.lock().unwrap();
    assert_eq!(state.cancel_calls, vec!["upload-1".to_string()]);
    assert!(!state.objects.contains_key("uncommitted"));
}

#[tokio::test]
async fn gcs_grpc_2_write_with_mismatched_buffer_while_pending_errors_without_dropping() {
    let server = MockServer::start().await;
    server
        .state
        .lock()
        .unwrap()
        .write_actions
        .push_back(WriteObjectAction::Fail(
            tonic::Code::Unavailable,
            "transient",
        ));

    let backend = server.builder().build().unwrap();
    let mut writer = backend
        .write(&OperationContext::default(), "mismatch", OpWrite::default())
        .unwrap();

    writer
        .write(Buffer::from(Bytes::from_static(b"hdr")))
        .await
        .unwrap();

    let original = Buffer::from(vec![b'a'; ALIGNMENT]);
    let err = writer.write(original.clone()).await.unwrap_err();
    assert!(err.is_temporary());
    assert_eq!(server.state.lock().unwrap().write_calls.len(), 1);

    // Different content with the same length must fail without sending any RPC.
    let mut same_len_diff = vec![b'a'; ALIGNMENT];
    same_len_diff[ALIGNMENT - 1] = b'b';
    let err = writer.write(Buffer::from(same_len_diff)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(!err.is_temporary());

    // Different length must also fail without sending any RPC.
    let err = writer
        .write(Buffer::from(Bytes::from_static(b"short")))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(!err.is_temporary());
    assert_eq!(server.state.lock().unwrap().write_calls.len(), 1);

    // Retrying with the exact original buffer succeeds and commits the full payload.
    writer.write(original).await.unwrap();
    let meta = writer.close().await.unwrap();
    assert_eq!(meta.content_length(), (ALIGNMENT + 3) as u64);

    let state = server.state.lock().unwrap();
    let stored = state.objects.get("mismatch").unwrap();
    assert_eq!(stored.len(), ALIGNMENT + 3);
    assert_eq!(&stored[..3], b"hdr");
    assert!(stored[3..].iter().all(|&b| b == b'a'));
}

#[tokio::test]
async fn gcs_grpc_3_reader_handles_zero_length_large_ranges_and_suffixes() {
    let server = MockServer::start().await;
    server
        .state
        .lock()
        .unwrap()
        .objects
        .insert("data".to_string(), b"0123456789".to_vec());

    // Direct oio::Read tests for zero-length ranges (no RPC issued).
    let backend = server.builder().build().unwrap();
    let reader = backend
        .read(&OperationContext::default(), "data", OpRead::default())
        .unwrap();

    let (_, buf) = reader.read(BytesRange::suffix(0)).await.unwrap();
    assert!(buf.is_empty());
    let (_, buf) = reader.read(BytesRange::new(5, Some(0))).await.unwrap();
    assert!(buf.is_empty());
    assert!(
        server.state.lock().unwrap().read_calls.is_empty(),
        "zero-length reads must not issue unbounded ReadObject RPCs"
    );

    // Offset > i64::MAX fails with RangeNotSatisfied without issuing an RPC.
    let err = reader.read(BytesRange::from(u64::MAX..)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
    assert!(server.state.lock().unwrap().read_calls.is_empty());

    // size > i64::MAX and suffix > i64::MAX read to EOF / full object.
    let (_, buf) = reader.read(BytesRange::from(0..=u64::MAX)).await.unwrap();
    assert_eq!(buf.to_bytes(), Bytes::from_static(b"0123456789"));

    let (_, buf) = reader
        .read(BytesRange::new(3, Some(i64::MAX as u64 + 1)))
        .await
        .unwrap();
    assert_eq!(buf.to_bytes(), Bytes::from_static(b"3456789"));

    let (_, buf) = reader.read(BytesRange::suffix(u64::MAX)).await.unwrap();
    assert_eq!(buf.to_bytes(), Bytes::from_static(b"0123456789"));

    let (_, buf) = reader.read(BytesRange::suffix(4)).await.unwrap();
    assert_eq!(buf.to_bytes(), Bytes::from_static(b"6789"));

    {
        let state = server.state.lock().unwrap();
        let observed: Vec<(i64, i64)> = state
            .read_calls
            .iter()
            .map(|req| (req.read_offset, req.read_limit))
            .collect();
        assert_eq!(observed, vec![(0, 0), (3, 0), (0, 0), (-4, 0)]);
    }

    // End-to-end Operator reads with size > i64::MAX (via limit) and suffix > i64::MAX.
    let op = server.operator();
    let full = op.read_with("data").limit(u64::MAX).await.unwrap();
    assert_eq!(full.to_bytes(), Bytes::from_static(b"0123456789"));
    let tail = op
        .read_with("data")
        .range(3..)
        .limit(u64::MAX)
        .await
        .unwrap();
    assert_eq!(tail.to_bytes(), Bytes::from_static(b"3456789"));
    let suffix = op
        .read_with("data")
        .range(BytesRange::suffix(u64::MAX))
        .await
        .unwrap();
    assert_eq!(suffix.to_bytes(), Bytes::from_static(b"0123456789"));
}

#[tokio::test]
async fn gcs_grpc_4_non_positive_generation_rejected_before_rpc() {
    let server = MockServer::start().await;
    server
        .state
        .lock()
        .unwrap()
        .objects
        .insert("live".to_string(), b"important".to_vec());

    let op = server.operator();

    for invalid in ["0", "-1"] {
        let err = op.stat_with("live").version(invalid).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = op.read_with("live").version(invalid).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = op.delete_with("live").version(invalid).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = op
            .copy_with("live", "copied")
            .source_version(invalid)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    // None of the invalid generations may ever touch the server or delete the live object.
    {
        let state = server.state.lock().unwrap();
        assert_eq!(state.total_object_rpc_calls(), 0);
        assert!(state.objects.contains_key("live"));
    }

    // A valid positive generation reaches the server with generation == 42.
    let meta = op.stat_with("live").version("42").await.unwrap();
    assert_eq!(meta.version(), Some("42"));

    let data = op.read_with("live").version("42").await.unwrap();
    assert_eq!(data.to_bytes(), Bytes::from_static(b"important"));

    op.copy_with("live", "copied")
        .source_version("42")
        .await
        .unwrap();
    op.delete_with("live").version("42").await.unwrap();

    let state = server.state.lock().unwrap();
    assert_eq!(state.get_calls.len(), 1);
    assert_eq!(state.get_calls[0].generation, 42);
    assert_eq!(state.read_calls.len(), 1);
    assert_eq!(state.read_calls[0].generation, 42);
    assert_eq!(state.rewrite_calls.len(), 1);
    assert_eq!(state.rewrite_calls[0].source_generation, 42);
    assert_eq!(state.delete_calls.len(), 1);
    assert_eq!(state.delete_calls[0].generation, 42);
}

#[tokio::test]
async fn gcs_grpc_1_partial_progress_continues_and_stops_on_subsequent_zero_progress() {
    let server = MockServer::start().await;
    {
        let mut state = server.state.lock().unwrap();
        // First WriteObject persists 256 KiB of a 512 KiB chunk (real progress -> loop continues).
        state
            .write_actions
            .push_back(WriteObjectAction::PartialPersist(ALIGNMENT));
        // Second WriteObject makes zero progress at offset 256 KiB -> must fail with temporary error.
        state
            .write_actions
            .push_back(WriteObjectAction::PersistedSize(ALIGNMENT as i64));
    }

    let backend = server.builder().build().unwrap();
    let mut writer = backend
        .write(&OperationContext::default(), "partial", OpWrite::default())
        .unwrap();

    writer
        .write(Buffer::from(Bytes::from_static(b"p")))
        .await
        .unwrap();

    let chunk = Buffer::from(vec![b'k'; ALIGNMENT * 2]);
    let err = tokio::time::timeout(TEST_TIMEOUT, writer.write(chunk.clone()))
        .await
        .expect("drive_pending must stop when a later WriteObject makes zero progress")
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(err.is_temporary());

    {
        let state = server.state.lock().unwrap();
        assert_eq!(state.write_calls.len(), 2);
        assert_eq!(state.write_calls[0].write_offset, 0);
        assert_eq!(state.write_calls[0].bytes_len, ALIGNMENT * 2);
        assert_eq!(state.write_calls[1].write_offset, ALIGNMENT as i64);
        assert_eq!(state.write_calls[1].bytes_len, ALIGNMENT);
    }

    // Retrying with the original buffer resumes from the already-persisted 256 KiB offset.
    writer.write(chunk).await.unwrap();
    let meta = writer.close().await.unwrap();
    assert_eq!(meta.content_length(), (ALIGNMENT * 2 + 1) as u64);
    assert_eq!(
        server
            .state
            .lock()
            .unwrap()
            .objects
            .get("partial")
            .unwrap()
            .len(),
        ALIGNMENT * 2 + 1
    );
}

#[tokio::test]
async fn gcs_grpc_2_close_retry_still_recovers_while_rejecting_write() {
    let server = MockServer::start().await;
    {
        let mut state = server.state.lock().unwrap();
        // Resumable chunk succeeds; final close fails transiently once.
        state.write_actions.push_back(WriteObjectAction::Normal);
        state.write_actions.push_back(WriteObjectAction::Fail(
            tonic::Code::Unavailable,
            "transient",
        ));
    }

    let backend = server.builder().build().unwrap();
    let mut writer = backend
        .write(
            &OperationContext::default(),
            "close-retry",
            OpWrite::default(),
        )
        .unwrap();

    writer
        .write(Buffer::from(Bytes::from_static(b"tail")))
        .await
        .unwrap();
    writer
        .write(Buffer::from(vec![b'm'; ALIGNMENT]))
        .await
        .unwrap();

    let err = writer.close().await.unwrap_err();
    assert!(err.is_temporary());

    // Calling write while close is pending must still fail cleanly.
    let err = writer
        .write(Buffer::from(Bytes::from_static(b"extra")))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(!err.is_temporary());

    // Retrying close queries status and finalizes cleanly.
    let meta = writer.close().await.unwrap();
    assert_eq!(meta.content_length(), (ALIGNMENT + 4) as u64);
    assert_eq!(
        server
            .state
            .lock()
            .unwrap()
            .objects
            .get("close-retry")
            .unwrap()
            .len(),
        ALIGNMENT + 4
    );
}
