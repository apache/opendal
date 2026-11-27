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

use std::future::{pending, poll_fn};
use std::pin::pin;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::task::Waker;

use oio::{Read, ReadStream};

use super::*;

const DEADLINE: Duration = Duration::from_millis(8);

#[derive(Clone, Default)]
struct TestExecutor {
    elapsed: Arc<AtomicBool>,
    sleeps: Arc<AtomicUsize>,
}

impl TestExecutor {
    fn time_out<T>(&self, future: impl Future<Output = Result<T>>) -> Error {
        let mut future = pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        self.elapsed.store(false, Ordering::Relaxed);
        assert!(future.as_mut().poll(&mut cx).is_pending());
        self.elapsed.store(true, Ordering::Relaxed);
        let Poll::Ready(Err(error)) = future.as_mut().poll(&mut cx) else {
            panic!("the originating executor timer must cancel the operation");
        };
        error
    }
}

impl Execute for TestExecutor {
    fn execute(&self, _: BoxedStaticFuture<()>) {
        panic!("tail cut should not spawn background tasks");
    }

    fn sleep(&self, duration: Duration) -> Option<BoxedStaticFuture<()>> {
        assert_eq!(duration, DEADLINE);
        self.sleeps.fetch_add(1, Ordering::Relaxed);
        let elapsed = self.elapsed.clone();
        Some(Box::pin(poll_fn(move |_| {
            if elapsed.load(Ordering::Relaxed) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })))
    }
}

#[derive(Clone, Copy, Debug, Default)]
enum Work {
    #[default]
    Ready,
    YieldOnce,
    Pending,
}

#[derive(Clone, Debug, Default)]
struct TestService {
    work: Arc<Mutex<Work>>,
    cancelled: Arc<AtomicUsize>,
}

struct CountCancellation<'a>(&'a AtomicUsize);

impl Drop for CountCancellation<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl TestService {
    async fn run(&self) {
        let work = *self.work.lock().unwrap();
        match work {
            Work::Ready => (),
            Work::YieldOnce => {
                let mut yielded = false;
                poll_fn(|cx| {
                    if yielded {
                        return Poll::Ready(());
                    }
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                })
                .await;
            }
            Work::Pending => {
                let _guard = CountCancellation(&self.cancelled);
                pending::<()>().await;
            }
        }
    }
}

impl Service for TestService {
    type Reader = Self;
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("test")
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            ..Default::default()
        }
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        self.run().await;
        Ok(RpStat::new(MetadataBuilder::file(3).build()))
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(self.clone())
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unreachable!()
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unreachable!()
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        unreachable!()
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unreachable!()
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        unreachable!()
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unreachable!()
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unreachable!()
    }
}

impl Read for TestService {
    async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
        unreachable!()
    }

    async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.run().await;
        Ok((RpRead::default(), Box::new(self.clone())))
    }
}

impl ReadStream for TestService {
    async fn read(&mut self) -> Result<Buffer> {
        self.run().await;
        Ok(Buffer::from("x"))
    }
}

fn complete<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..2 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
    panic!("work did not complete within two polls");
}

fn assert_tail_cut(error: Error, operation: Operation) {
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(error.is_temporary());
    let message = match operation {
        Operation::Stat => "cancelled by tail cut",
        Operation::Read => "io cancelled by tail cut",
        _ => unreachable!(),
    };
    assert_eq!(error.message(), message);
    let error = error.to_string();
    assert!(error.contains(&format!("at {operation}")), "{error}");
    assert!(error.contains("percentile: P97"), "{error}");
    assert!(error.contains("deadline: 8ms"), "{error}");
}

#[test]
fn adaptive_deadlines_use_the_originating_executor_without_tokio() {
    let layer = TailCutLayer::builder()
        .percentile(97)
        .min_samples(1)
        .min_deadline(DEADLINE)
        .max_deadline(DEADLINE)
        .build();
    let service = TestService::default();
    let executor_a = TestExecutor::default();
    let executor_b = TestExecutor::default();
    let ctx = OperationContext::default().with_executor(Executor::with(executor_a.clone()));
    let op = Operator::from_parts(ctx, Arc::new(service.clone())).layer(layer.clone());
    // Dispatch through the composed service so the context and reader can be dropped independently.
    let reader = op
        .service()
        .read(op.context(), "file", OpRead::default())
        .unwrap();
    let range = BytesRange::new(0, Some(3));
    let samples = |key| {
        layer
            .stats
            .stats_for(key)
            .total_samples(None, layer.config.window)
    };
    let sample_counts = || {
        [
            Operation::Stat.into(),
            StatKey::ReadOpen,
            StatKey::ReadStream,
        ]
        .map(samples)
    };

    complete(op.stat("file")).unwrap();
    let (_, mut stream) = complete(reader.open(range)).unwrap();
    complete(stream.read_dyn()).unwrap();
    assert_eq!(samples(StatKey::ReadStream), 0);
    *service.work.lock().unwrap() = Work::YieldOnce;
    complete(stream.read_dyn()).unwrap();
    drop(stream);
    assert_eq!(executor_a.sleeps.load(Ordering::Relaxed), 0);
    assert_eq!(sample_counts(), [1, 1, 1]);

    let op = op.with_context(
        OperationContext::default().with_executor(Executor::with(executor_b.clone())),
    );
    let new_reader = op
        .service()
        .read(op.context(), "file", OpRead::default())
        .unwrap();
    *service.work.lock().unwrap() = Work::Pending;
    assert_tail_cut(executor_b.time_out(op.stat("file")), Operation::Stat);
    assert_tail_cut(executor_a.time_out(reader.open(range)), Operation::Read);
    assert_tail_cut(executor_b.time_out(new_reader.open(range)), Operation::Read);
    assert_eq!(sample_counts(), [1, 1, 1]);

    // Open an A stream after replacing the context. Ready work wins with A's timer already elapsed.
    *service.work.lock().unwrap() = Work::Ready;
    let (_, mut stream) = complete(reader.open(range)).unwrap();
    drop(reader);
    complete(stream.read_dyn()).unwrap();
    assert_eq!(sample_counts(), [1, 2, 1]);

    *service.work.lock().unwrap() = Work::Pending;
    assert_tail_cut(executor_a.time_out(stream.read_dyn()), Operation::Read);
    assert_eq!(samples(StatKey::ReadStream), 1);
    assert_eq!(service.cancelled.load(Ordering::Relaxed), 4);
}
