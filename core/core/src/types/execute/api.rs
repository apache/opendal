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

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::Either;
use futures::future::poll_fn;
use futures::future::select;

use crate::Error;
use crate::ErrorKind;
use crate::Result;
use crate::raw::BoxedStaticFuture;
use crate::raw::MaybeSend;

/// Executes background tasks and optionally provides timers.
pub trait Execute: Send + Sync + 'static {
    /// Execute an async task in the background.
    ///
    /// The executor must keep polling accepted tasks until they complete.
    fn execute(&self, f: BoxedStaticFuture<()>);

    /// Submit an async task, returning an error if the executor rejects it.
    /// An executor that returns an error must leave the task unpolled.
    ///
    /// The default implementation forwards to [`Self::execute`]. Executors
    /// with fallible submission should override this method.
    fn try_execute(&self, f: BoxedStaticFuture<()>) -> Result<()> {
        self.execute(f);
        Ok(())
    }

    /// Return the configured timeout for an individual I/O task, if any.
    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        None
    }

    /// Return a future that completes after `duration`, if timers are supported.
    ///
    /// The timer must make progress independently of the executor's task queue.
    fn sleep(&self, _duration: Duration) -> Option<BoxedStaticFuture<()>> {
        None
    }
}

impl Execute for () {
    fn execute(&self, _: BoxedStaticFuture<()>) {
        panic!("concurrent tasks executed with no executor has been enabled")
    }

    fn try_execute(&self, _: BoxedStaticFuture<()>) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unexpected,
            "no executor is configured",
        ))
    }
}

/// The result of a background task. Dropping the handle cancels the task.
pub(crate) struct Task<T> {
    result: oneshot::Receiver<std::thread::Result<T>>,
}

impl<T: MaybeSend + 'static> Task<T> {
    pub(crate) fn submit<F>(executor: &dyn Execute, f: F) -> Result<Self>
    where
        F: Future<Output = T> + MaybeSend + 'static,
    {
        let (mut sender, result) = oneshot::channel();
        executor.try_execute(Box::pin(async move {
            let canceled = poll_fn(|cx| sender.poll_canceled(cx));
            let work = AssertUnwindSafe(f).catch_unwind();
            futures::pin_mut!(canceled, work);
            let outcome = match select(canceled, work).await {
                Either::Left(_) => return,
                Either::Right((outcome, _)) => outcome,
            };
            let _ = sender.send(outcome);
        }))?;
        Ok(Self { result })
    }
}

impl<T> Future for Task<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match futures::ready!(self.result.poll_unpin(cx)) {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(panic)) => resume_unwind(panic),
            Err(_) => Err(Error::new(
                ErrorKind::Unexpected,
                "executor dropped a background task before it completed",
            )),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AbandoningExecutor;

    impl Execute for AbandoningExecutor {
        fn execute(&self, _f: BoxedStaticFuture<()>) {}
    }

    #[tokio::test]
    async fn accepted_task_discarded_by_executor_returns_an_error() {
        let executor = AbandoningExecutor;
        assert!(executor.timeout().is_none());
        let task = Task::submit(&executor, async { 42 }).unwrap();
        let err = task.await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("dropped a background task"));
    }
}
