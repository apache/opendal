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

use std::sync::Arc;

use http::StatusCode;

use super::core::parse_error;
use super::core::*;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

pub struct WebdavDeleter {
    core: Arc<WebdavCore>,
    ctx: OperationContext,
}

impl WebdavDeleter {
    pub fn new(core: Arc<WebdavCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::OneShotDelete for WebdavDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let resp = self.core.webdav_delete(&self.ctx, &path, &args).await?;

        let status = resp.status();
        match status {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND if args.if_match().is_some() => Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "delete precondition requires a live target",
            )),
            StatusCode::NOT_FOUND => Ok(()),
            // No `with_if_match` here: it maps SabreDAV's 412-on-missing to
            // NotFound, but delete reports an unmet precondition as
            // ConditionNotMatch.
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Delete")),
                resp,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Mutex;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::task::{Context, Poll};

    use futures::channel::oneshot;
    use futures::stream;
    use futures::task::noop_waker_ref;
    use http::{Method, Request, Response, StatusCode};

    use super::super::backend::WebdavBuilder;
    use super::*;

    struct FixedStatusTransport {
        status: StatusCode,
    }

    impl HttpTransport for FixedStatusTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            Ok(Response::builder()
                .status(self.status)
                .body(HttpBody::new(stream::empty(), Some(0)))
                .unwrap())
        }
    }

    fn operator_with_status(status: StatusCode) -> Result<Operator> {
        Ok(
            Operator::new(WebdavBuilder::default().endpoint("http://localhost:8080"))?
                .with_context(
                    OperationContext::new()
                        .with_http_transport(HttpTransporter::new(FixedStatusTransport { status })),
                ),
        )
    }

    #[tokio::test]
    async fn test_delete_accepts_no_content_and_not_found() -> Result<()> {
        for status in [StatusCode::NO_CONTENT, StatusCode::NOT_FOUND] {
            let op = operator_with_status(status)?;
            op.delete("file.txt").await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_maps_unauthorized_and_conditional_not_found() -> Result<()> {
        let err = operator_with_status(StatusCode::UNAUTHORIZED)?
            .delete("file.txt")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(!err.is_temporary());

        let err = operator_with_status(StatusCode::NOT_FOUND)?
            .delete_with("file.txt")
            .if_match("\"etag\"")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        Ok(())
    }

    type PendingDelete = (String, oneshot::Sender<StatusCode>);

    struct GatedTransport(Sender<PendingDelete>);

    impl HttpTransport for GatedTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            assert_eq!(req.method(), Method::DELETE);
            let (response, status) = oneshot::channel();
            self.0
                .send((req.uri().path().to_owned(), response))
                .unwrap();
            Ok(Response::builder()
                .status(status.await.unwrap())
                .body(HttpBody::new(stream::empty(), Some(0)))
                .unwrap())
        }
    }

    #[derive(Clone, Default)]
    struct ManualExecutor(Arc<Mutex<Vec<BoxedStaticFuture<()>>>>);

    impl Execute for ManualExecutor {
        fn execute(&self, task: BoxedStaticFuture<()>) {
            self.0.lock().unwrap().push(task);
        }
    }

    impl ManualExecutor {
        fn advance(&self) {
            let mut cx = Context::from_waker(noop_waker_ref());
            self.0
                .lock()
                .unwrap()
                .retain_mut(|task| task.as_mut().poll(&mut cx).is_pending());
        }
    }

    fn gated_deletes() -> (Operator, ManualExecutor, Receiver<PendingDelete>) {
        let (requests, log) = mpsc::channel();
        let executor = ManualExecutor::default();
        let op = Operator::new(WebdavBuilder::default().endpoint("http://localhost:8080"))
            .unwrap()
            .with_context(
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(GatedTransport(requests)))
                    .with_executor(Executor::with(executor.clone())),
            );
        (op, executor, log)
    }

    fn response_for(log: &Receiver<PendingDelete>, expected: &str) -> oneshot::Sender<StatusCode> {
        let (path, response) = log.try_recv().unwrap();
        assert_eq!(path, expected);
        response
    }

    #[tokio::test]
    async fn concurrent_delete_returns_completed_failure_before_accepting_input() -> Result<()> {
        let (op, executor, log) = gated_deletes();
        let mut deleter = op.deleter_with().concurrent(3).await?;
        deleter
            .delete_iter(["pending", "completed", "failed"])
            .await?;
        executor.advance();
        let pending = response_for(&log, "/pending");
        response_for(&log, "/completed")
            .send(StatusCode::NO_CONTENT)
            .unwrap();
        response_for(&log, "/failed")
            .send(StatusCode::FORBIDDEN)
            .unwrap();
        executor.advance();

        let mut cx = Context::from_waker(noop_waker_ref());
        {
            let mut delete = pin!(deleter.delete("unaccepted"));
            let Poll::Ready(Err(error)) = delete.as_mut().poll(&mut cx) else {
                panic!(
                    "delete must surface the completed error without waiting for the pending task"
                );
            };
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
            assert!(error.to_string().contains("path: failed"));
        }
        pending.send(StatusCode::NO_CONTENT).unwrap();
        let mut close = pin!(deleter.close());
        assert!(close.as_mut().poll(&mut cx).is_pending());
        executor.advance();
        response_for(&log, "/failed")
            .send(StatusCode::NO_CONTENT)
            .unwrap();
        executor.advance();
        assert!(matches!(close.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        assert!(log.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_close_resumes_canceled_waits_and_collects_all_failures() -> Result<()> {
        let (op, executor, log) = gated_deletes();
        let mut deleter = op.deleter_with().concurrent(2).await?;
        deleter.delete_iter(["first", "second"]).await?;
        executor.advance();
        let first = response_for(&log, "/first");
        let second = response_for(&log, "/second");
        let mut cx = Context::from_waker(noop_waker_ref());
        {
            let mut delete = pin!(deleter.delete("unaccepted"));
            assert!(delete.as_mut().poll(&mut cx).is_pending());
        }
        first.send(StatusCode::FORBIDDEN).unwrap();
        executor.advance();
        {
            let mut close = pin!(deleter.close());
            assert!(close.as_mut().poll(&mut cx).is_pending());
        }
        second.send(StatusCode::FORBIDDEN).unwrap();
        executor.advance();
        {
            let mut close = pin!(deleter.close());
            let Poll::Ready(Err(error)) = close.as_mut().poll(&mut cx) else {
                panic!("close must resume the original attempt and report its error");
            };
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
            assert!(error.to_string().contains("path: first"));
        }

        let mut retry = pin!(deleter.close());
        assert!(retry.as_mut().poll(&mut cx).is_pending());
        executor.advance();
        response_for(&log, "/first")
            .send(StatusCode::NO_CONTENT)
            .unwrap();
        response_for(&log, "/second")
            .send(StatusCode::NO_CONTENT)
            .unwrap();
        executor.advance();
        assert!(matches!(retry.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        assert!(log.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn dropping_concurrent_deleter_stops_queued_requests() -> Result<()> {
        let (op, executor, log) = gated_deletes();
        let mut deleter = op.deleter_with().concurrent(2).await?;
        deleter.delete("started").await?;
        executor.advance();
        let started = response_for(&log, "/started");
        deleter.delete("queued").await?;

        drop(deleter);
        executor.advance();
        assert!(started.send(StatusCode::NO_CONTENT).is_err());
        assert!(log.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn default_deleter_options_preserve_inline_one_shot_completion() -> Result<()> {
        let op = operator_with_status(StatusCode::FORBIDDEN)?;
        for mut deleter in [
            op.deleter().await?,
            op.deleter_with().await?,
            op.deleter_with().concurrent(0).await?,
            op.deleter_with().concurrent(1).await?,
        ] {
            let error = deleter.delete("file").await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
            deleter.close().await?;
        }
        Ok(())
    }
}
