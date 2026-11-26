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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use futures::lock::Mutex;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::raw::oio::Delete;
use crate::raw::{OpDelete, Service, Servicer, oio};
use crate::types::Task;
use crate::{Error, OperationContext, Result};

pub(super) struct ConcurrentDeleter {
    ctx: OperationContext,
    service: Servicer,
    initial_deleter: Option<oio::Deleter>,
    concurrent: usize,
    batch_size: usize,
    pending: HashSet<(String, OpDelete)>,
    running: FuturesUnordered<RunningDelete>,
    failed: VecDeque<SharedDeleteTask>,
    closing: Option<CloseProgress>,
}

struct CloseProgress {
    // Retry only failures known when this close began, including after a canceled wait.
    retry: VecDeque<SharedDeleteTask>,
    error: Option<Error>,
}

impl ConcurrentDeleter {
    pub(super) fn new(
        ctx: OperationContext,
        service: Servicer,
        initial_deleter: oio::Deleter,
        concurrent: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            ctx,
            service,
            initial_deleter: Some(initial_deleter),
            concurrent,
            batch_size,
            pending: HashSet::new(),
            running: FuturesUnordered::new(),
            failed: VecDeque::new(),
            closing: None,
        }
    }

    fn dispatch_entries(&mut self, entries: VecDeque<(String, OpDelete)>) -> Result<()> {
        let state = DeleteTask {
            deleter: self.initial_deleter.take(),
            entries,
        };
        self.dispatch(Arc::new(Mutex::new(state)))
    }

    fn dispatch(&mut self, state: SharedDeleteTask) -> Result<()> {
        let work = state.clone();
        let ctx = self.ctx.clone();
        let service = self.service.clone();
        match self
            .ctx
            .executor()
            .execute(async move { work.lock().await.run(&ctx, &service).await })
        {
            Ok(task) => {
                self.running.push(RunningDelete { state, task });
                Ok(())
            }
            Err(error) => {
                self.failed.push_back(state);
                Err(error)
            }
        }
    }

    fn complete(&mut self, (state, result): (SharedDeleteTask, Result<()>)) -> Result<()> {
        result.inspect_err(|_| self.failed.push_back(state))
    }
}

impl oio::Delete for ConcurrentDeleter {
    async fn delete(&mut self, path: &str, mut args: OpDelete) -> Result<()> {
        if self.closing.is_some() {
            self.close().await?;
        }
        if self.running.len() == self.concurrent {
            let completed = self
                .running
                .next()
                .await
                .expect("all task slots are occupied");
            self.complete(completed)?;
        }
        while let Some(Some(completed)) = self.running.next().now_or_never() {
            self.complete(completed)?;
        }

        // Recursive deletion may expand into many entries inside a service layer.
        if self.batch_size == 1 || args.recursive() {
            return self.dispatch_entries(VecDeque::from([(path.to_owned(), args)]));
        }
        args.defer_batch_flush();
        self.pending.insert((path.to_owned(), args));
        if self.pending.len() == self.batch_size {
            let entries = self.pending.drain().collect();
            self.dispatch_entries(entries)?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.closing.is_none() {
            self.closing = Some(CloseProgress {
                retry: std::mem::take(&mut self.failed),
                error: None,
            });
        }
        loop {
            while self.running.len() < self.concurrent {
                let progress = self.closing.as_mut().expect("close has started");
                let result = if !self.pending.is_empty() {
                    // Flush the accepted tail before assigning spare capacity to replay.
                    let entries = self.pending.drain().collect();
                    self.dispatch_entries(entries)
                } else if let Some(state) = progress.retry.pop_front() {
                    self.dispatch(state)
                } else {
                    break;
                };
                if let Err(error) = result {
                    let progress = self.closing.as_mut().expect("close has started");
                    progress.error.get_or_insert(error);
                }
            }
            let Some(completed) = self.running.next().await else {
                break;
            };
            if let Err(error) = self.complete(completed) {
                let progress = self.closing.as_mut().expect("close has started");
                progress.error.get_or_insert(error);
            }
        }
        match self.closing.take().expect("close has started").error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

type SharedDeleteTask = Arc<Mutex<DeleteTask>>;

struct DeleteTask {
    // Retain the composed deleter so retrying close uses its failed batch entries.
    deleter: Option<oio::Deleter>,
    entries: VecDeque<(String, OpDelete)>,
}

impl DeleteTask {
    async fn run(&mut self, ctx: &OperationContext, service: &Servicer) -> Result<()> {
        if self.deleter.is_none() {
            self.deleter = Some(service.delete(ctx)?);
        }
        let deleter = self.deleter.as_mut().expect("task deleter is initialized");
        while let Some((path, args)) = self.entries.front() {
            deleter.delete(path, args.clone()).await?;
            self.entries.pop_front();
        }
        deleter.close().await
    }
}

struct RunningDelete {
    // The task may be discarded by the executor. Its state stays available for retry.
    state: SharedDeleteTask,
    task: Task<Result<()>>,
}

impl Future for RunningDelete {
    type Output = (SharedDeleteTask, Result<()>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(Pin::new(&mut self.task).poll(cx)).and_then(|result| result);
        Poll::Ready((self.state.clone(), result))
    }
}
