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

#![cfg(not(target_arch = "wasm32"))]

use std::cell::RefCell;
use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use compio::dispatcher::Dispatcher;
use compio::runtime::Runtime;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::future::Either;
use futures::future::select;
use futures_timer::Delay;
use opendal_core::raw::oio::{MultipartPart, MultipartWrite, MultipartWriter, Write};
use opendal_core::raw::{BoxedStaticFuture, ConcurrentTasks};
use opendal_core::{
    Buffer, ErrorKind, Execute, Executor, Metadata, MetadataBuilder, Operator, Result,
};
use opendal_executor_compio::CompioExecutor;
use opendal_layer_timeout::TimeoutLayer;
use opendal_layer_tracing::TracingLayer;

const WATCHDOG: Duration = Duration::from_secs(5);

fn dispatcher(concurrent: bool) -> Dispatcher {
    Dispatcher::builder()
        .worker_threads(NonZeroUsize::MIN)
        .concurrent(concurrent)
        .build()
        .unwrap()
}

fn executor(concurrent: bool) -> Executor {
    Executor::with(CompioExecutor::new(Arc::new(dispatcher(concurrent))))
}

async fn within<F: Future>(future: F) -> F::Output {
    futures::pin_mut!(future);
    match select(future, Delay::new(WATCHDOG)).await {
        Either::Left((output, _)) => output,
        Either::Right(_) => panic!("test operation stalled"),
    }
}

#[test]
fn timer_runs_while_the_only_worker_has_queued_tasks() {
    let executor = CompioExecutor::new(Arc::new(dispatcher(false)));
    let timer = executor.clone();
    let duration = Duration::from_millis(30);
    let (started_tx, started_rx) = oneshot::channel();
    let (finished_tx, finished_rx) = oneshot::channel();
    let (submitted_tx, submitted_rx) = oneshot::channel();

    executor
        .try_execute(Box::pin(async move {
            Runtime::with_current(|_| ());
            started_tx.send(()).unwrap();
            submitted_rx.await.unwrap();
            let start = Instant::now();
            timer.sleep(duration).unwrap().await;
            assert!(start.elapsed() >= duration);
            finished_tx.send(()).unwrap();
        }))
        .unwrap();
    block_on(within(started_rx)).unwrap();

    let (queued_tx, queued_rx) = oneshot::channel();
    executor
        .try_execute(Box::pin(async move {
            queued_tx.send(()).unwrap();
        }))
        .unwrap();
    submitted_tx.send(()).unwrap();
    block_on(within(finished_rx)).unwrap();
    block_on(within(queued_rx)).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn submission_reports_a_closed_dispatcher() {
    use compio::driver::{DriverType, ProactorBuilder};

    // The Linux driver rejects the Windows-only IOCP driver while starting the worker.
    let mut proactor = ProactorBuilder::new();
    proactor.driver_type(DriverType::IOCP);
    let dispatcher = Dispatcher::builder()
        .worker_threads(NonZeroUsize::MIN)
        .proactor_builder(proactor)
        .build()
        .unwrap();
    let executor = Executor::with(CompioExecutor::new(Arc::new(dispatcher)));
    block_on(within(async {
        loop {
            let mut tasks = ConcurrentTasks::new(executor.clone(), 2, 0, |()| {
                Box::pin(async { ((), Ok(())) })
            });
            if let Err(err) = tasks.execute(()).await {
                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert!(err.to_string().contains("compio dispatcher is unavailable"));
                break;
            }
            Delay::new(Duration::from_millis(1)).await;
        }
    }));
}

#[test]
fn concurrent_tasks_cancel_and_propagate_panics() {
    let executor = executor(true);
    let (started_tx, started_rx) = oneshot::channel();
    let (drop_tx, drop_rx) = oneshot::channel::<()>();
    let (unblock_tx, unblock_rx) = oneshot::channel();
    let (second_task_ran_tx, second_task_ran_rx) = oneshot::channel();
    let mut tasks = ConcurrentTasks::new(executor.clone(), 2, 0, |work: BoxedStaticFuture<()>| {
        Box::pin(async move {
            work.await;
            (Box::pin(async {}) as BoxedStaticFuture<()>, Ok(()))
        })
    });

    block_on(within(async {
        tasks
            .execute(Box::pin(async move {
                Runtime::with_current(|_| ());
                let _drop_tx = drop_tx;
                started_tx.send(()).unwrap();
                unblock_rx.await.unwrap();
                second_task_ran_tx.send(()).unwrap();
                pending::<()>().await;
            }))
            .await
            .unwrap();
        started_rx.await.unwrap();
        tasks
            .execute(Box::pin(async move {
                unblock_tx.send(()).unwrap();
            }))
            .await
            .unwrap();
        second_task_ran_rx.await.unwrap();
        tasks.clear();
        assert!(drop_rx.await.is_err());
    }));

    let mut tasks = ConcurrentTasks::new(
        executor,
        2,
        0,
        |()| -> BoxedStaticFuture<((), Result<()>)> {
            Box::pin(async move { panic!("background task panic") })
        },
    );
    block_on(within(async {
        tasks.execute(()).await.unwrap();
        let panic = AssertUnwindSafe(tasks.next())
            .catch_unwind()
            .await
            .unwrap_err();
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"background task panic"));
    }));
}

#[test]
fn dropping_the_last_executor_releases_the_worker() {
    thread_local! {
        static WORKER_EXIT: RefCell<Option<oneshot::Sender<()>>> = const { RefCell::new(None) };
    }
    let executor = executor(true);
    let (exit_tx, exit_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    executor
        .inner()
        .try_execute(Box::pin(async move {
            WORKER_EXIT.set(Some(exit_tx));
            ready_tx.send(()).unwrap();
        }))
        .unwrap();
    block_on(within(ready_rx)).unwrap();
    drop(executor);
    assert!(block_on(within(exit_rx)).is_err());
}

#[test]
fn memory_and_fs_operations_use_a_compio_executor() {
    let executor = executor(true);
    let memory = Operator::new(opendal_core::services::Memory::default()).unwrap();
    let context = memory.context().with_executor(executor.clone());
    let memory = memory
        .with_context(context)
        .layer(TimeoutLayer::default())
        .layer(TracingLayer::new());
    let payload = "abcdefgh";
    block_on(within(async {
        memory.write("file", payload).await.unwrap();
        assert_eq!(
            memory
                .read_with("file")
                .range(0..8)
                .chunk(2)
                .concurrent(2)
                .await
                .unwrap()
                .to_bytes(),
            payload
        );
    }));

    // The fs service uses Tokio for file I/O, so enter a Tokio handle on each
    // poll when driving its operation on a Compio worker.
    let tokio = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let handle = tokio.handle().clone();
    let root = tempfile::tempdir().unwrap();
    let fs = Operator::new(opendal_service_fs::Fs::default().root(root.path().to_str().unwrap()))
        .unwrap();
    let context = fs.context().with_executor(executor.clone());
    let fs = fs.with_context(context).layer(TimeoutLayer::default());
    let (tx, rx) = oneshot::channel();
    executor
        .inner()
        .try_execute(Box::pin(async move {
            Runtime::with_current(|_| ());
            let io = async move {
                fs.write("file", payload).await?;
                fs.read("file").await
            };
            futures::pin_mut!(io);
            let result = poll_fn(|cx| {
                let _guard = handle.enter();
                io.as_mut().poll(cx)
            })
            .await;
            tx.send(result).unwrap();
        }))
        .unwrap();
    assert_eq!(block_on(within(rx)).unwrap().unwrap().to_bytes(), payload);
    assert_eq!(
        std::fs::read(root.path().join("file")).unwrap(),
        payload.as_bytes()
    );
}

#[derive(Debug, Clone)]
struct MultipartService {
    first_part: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl MultipartWrite for MultipartService {
    async fn write_once(&self, _: u64, _: Buffer) -> Result<Metadata> {
        panic!("expected multipart upload")
    }

    async fn initiate_part(&self) -> Result<String> {
        Ok("upload".into())
    }

    async fn write_part(
        &self,
        _: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<MultipartPart> {
        Runtime::with_current(|_| ());
        if part_number == 0 {
            let first_attempt = self.first_part.lock().unwrap().take();
            if let Some(_drop_on_timeout) = first_attempt {
                pending::<()>().await;
            }
        }
        assert_eq!(
            body.to_bytes(),
            if part_number == 0 { "abcd" } else { "efgh" }
        );
        Ok(MultipartPart {
            part_number,
            etag: part_number.to_string(),
            checksum: None,
            size: Some(size),
        })
    }

    async fn complete_part(&self, _: &str, parts: &[MultipartPart]) -> Result<Metadata> {
        assert_eq!(
            parts
                .iter()
                .map(|part| part.part_number)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        Ok(MetadataBuilder::file(8).build())
    }

    async fn abort_part(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[test]
fn tracing_preserves_per_part_timeouts_and_retries() {
    for tracing_outermost in [false, true] {
        let (drop_tx, drop_rx) = oneshot::channel::<()>();
        let context = opendal_core::OperationContext::new().with_executor(executor(false));
        let op = Operator::from_parts(context, Arc::new(()));
        let service = MultipartService {
            first_part: Arc::new(Mutex::new(Some(drop_tx))),
        };
        let timeout = TimeoutLayer::new().with_io_timeout(Duration::from_millis(250));
        let tracing = TracingLayer::new();
        let op = if tracing_outermost {
            op.layer(timeout).layer(tracing)
        } else {
            op.layer(tracing).layer(timeout)
        };
        block_on(within(async {
            let mut writer = MultipartWriter::new(op.context().executor().clone(), service, 2);
            writer.write(Buffer::from("abcd")).await.unwrap();
            writer.write(Buffer::from("efgh")).await.unwrap();
            // No outer write or close is pending. Only part zero's own deadline can cancel it.
            assert!(drop_rx.await.is_err());
            let err = writer.close().await.unwrap_err();
            assert!(err.is_temporary());
            assert!(err.to_string().contains("write part timeout"), "{err}");
            assert_eq!(writer.close().await.unwrap().content_length(), 8);
        }));
    }
}
