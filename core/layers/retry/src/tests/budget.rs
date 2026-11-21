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

use std::panic::UnwindSafe;

use futures::FutureExt as _;
use oio::Copy as _;
use oio::Delete as _;
use oio::List as _;
use oio::Read as _;
use oio::ReadStream as _;
use oio::Write as _;

use super::*;

#[derive(Debug, PartialEq, Eq)]
enum BudgetEvent {
    Deposit(Operation),
    Reserve(Operation),
    Release(Operation),
    Retry(Operation, u32, Duration),
}

#[derive(Debug)]
struct BudgetState {
    available: usize,
    events: Vec<BudgetEvent>,
}

#[derive(Debug, Clone)]
struct TestBudget(Arc<Mutex<BudgetState>>);

impl TestBudget {
    fn new(available: usize) -> Self {
        Self(Arc::new(Mutex::new(BudgetState {
            available,
            events: Vec::new(),
        })))
    }

    fn available(&self) -> usize {
        self.0.lock().unwrap().available
    }

    fn take_events(&self) -> Vec<BudgetEvent> {
        std::mem::take(&mut self.0.lock().unwrap().events)
    }
}

impl RetryBudget for TestBudget {
    fn deposit(&self, op: Operation) {
        let mut state = self.0.lock().unwrap();
        state.available += 1;
        state.events.push(BudgetEvent::Deposit(op));
    }

    fn try_reserve(&self, op: Operation) -> bool {
        let mut state = self.0.lock().unwrap();
        state.events.push(BudgetEvent::Reserve(op));
        if state.available == 0 {
            return false;
        }
        state.available -= 1;
        true
    }

    fn release(&self, op: Operation) {
        let mut state = self.0.lock().unwrap();
        state.available += 1;
        state.events.push(BudgetEvent::Release(op));
    }
}

impl RetryInterceptor for TestBudget {
    fn intercept(&self, event: RetryEvent<'_>) {
        let mut state = self.0.lock().unwrap();
        state.events.push(BudgetEvent::Retry(
            event.op,
            event.attempt,
            event.retry_after,
        ));
    }
}

#[test]
fn retry_layer_auto_traits() {
    fn assert_unwind_safe<T: UnwindSafe + RefUnwindSafe>() {}
    fn assert_layer_traits<T: Send + Sync + Debug>(_: &T) {}

    assert_unwind_safe::<RetryLayer>();
    assert_unwind_safe::<RetryLayer<TestBudget>>();
    assert_layer_traits(&RetryLayer::new().with_budget(TestBudget::new(1)));
    assert_layer_traits(&RetryLayer::new().with_notify(TestBudget::new(1)));
}

#[tokio::test]
async fn retry_limits_for_stat_and_reader_construction() -> Result<()> {
    use BudgetEvent::*;

    for (max_times, tokens, expected_attempts) in [(0, 2, 1), (1, 2, 2), (2, 0, 1)] {
        for operation in [Operation::Stat, Operation::Read] {
            let budget = TestBudget::new(tokens);
            let service = MockBuilder::with_operation_failures(2);
            let op = Operator::new(service.clone())?.layer(
                RetryLayer::new()
                    .with_max_times(max_times)
                    .with_min_delay(Duration::ZERO)
                    .with_budget(budget.clone())
                    .with_notify(budget.clone()),
            );
            let err = match operation {
                Operation::Stat if expected_attempts == 1 => op
                    .stat("file")
                    .now_or_never()
                    .expect("a stopped retry must not sleep")
                    .expect_err("stat must fail"),
                Operation::Stat => op.stat("file").await.expect_err("stat must fail"),
                Operation::Read => op
                    .reader("file")
                    .await
                    .expect_err("reader creation must fail"),
                _ => unreachable!(),
            };
            assert_eq!(err.kind(), ErrorKind::RateLimited);
            assert!(err.is_persistent());
            assert_eq!(*service.attempt.lock().unwrap(), expected_attempts);
            let events = match (max_times, tokens) {
                (0, _) => vec![],
                (_, 0) => vec![Reserve(operation)],
                _ => vec![Reserve(operation), Retry(operation, 1, Duration::ZERO)],
            };
            assert_eq!(budget.take_events(), events);
            assert_eq!(budget.available(), tokens - (expected_attempts - 1));
        }
    }
    Ok(())
}

#[tokio::test]
async fn retry_reservation_is_shared_and_refunded_when_sleep_is_cancelled() -> Result<()> {
    use BudgetEvent::*;

    let delay = Duration::from_secs(60);
    let budget = TestBudget::new(0);
    let layer = RetryLayer::new()
        .with_max_times(1)
        .with_min_delay(delay)
        .with_max_delay(delay)
        .with_budget(budget.clone());

    let healthy = Operator::new(MockBuilder::with_operation_failures(0))?.layer(layer.clone());
    healthy.stat("file").await?;
    assert_eq!(budget.take_events(), [Deposit(Operation::Stat)]);

    let service = MockBuilder::with_operation_failures(1);
    let first = Operator::new(service.clone())?.layer(layer.clone().with_notify(budget.clone()));
    let second = Operator::new(MockBuilder::with_operation_failures(1))?.layer(
        layer
            .with_min_delay(Duration::ZERO)
            .with_notify(budget.clone()),
    );
    let mut pending = Box::pin(first.stat("file"));
    assert!(futures::poll!(&mut pending).is_pending());
    assert_eq!(*service.attempt.lock().unwrap(), 1);
    assert_eq!(budget.available(), 0);
    assert_eq!(
        budget.take_events(),
        [Reserve(Operation::Stat), Retry(Operation::Stat, 1, delay)]
    );

    let err = second
        .stat("file")
        .now_or_never()
        .expect("an exhausted budget must not sleep")
        .expect_err("budget is reserved by the first operator");
    assert!(err.is_persistent());
    assert_eq!(budget.take_events(), [Reserve(Operation::Stat)]);

    drop(pending);
    assert_eq!(budget.available(), 1);
    assert_eq!(budget.take_events(), [Release(Operation::Stat)]);

    let retrying = Operator::new(MockBuilder::with_operation_failures(1))?.layer(
        RetryLayer::new()
            .with_min_delay(Duration::ZERO)
            .with_budget(budget.clone())
            .with_notify(budget.clone()),
    );
    retrying.stat("file").await?;
    assert_eq!(budget.available(), 1);
    assert_eq!(
        budget.take_events(),
        [
            Reserve(Operation::Stat),
            Retry(Operation::Stat, 1, Duration::ZERO),
            Deposit(Operation::Stat)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn stream_recovery_reserves_only_for_reopens() -> Result<()> {
    use BudgetEvent::*;

    let budget = TestBudget::new(2);
    let builder = MockBuilder::default();
    let op = Operator::new(builder.clone())?.layer(
        RetryLayer::new()
            .with_max_times(2)
            .with_min_delay(Duration::ZERO)
            .with_budget(budget.clone())
            .with_notify(budget.clone()),
    );
    let reader = op.service().read(op.context(), "file", OpRead::default())?;
    let (_, mut stream) = reader.open(BytesRange::from(0..13)).await?;
    assert!(budget.take_events().is_empty());
    assert_eq!(
        stream.read().await?.to_bytes(),
        Bytes::from_static(b"Hello, World!")
    );
    assert_eq!(
        budget.take_events(),
        [
            Reserve(Operation::Read),
            Retry(Operation::Read, 1, Duration::ZERO),
            Reserve(Operation::Read),
            Retry(Operation::Read, 2, Duration::ZERO),
            Deposit(Operation::Read),
        ]
    );
    assert_eq!(budget.available(), 1);

    assert!(stream.read().await?.is_empty());
    assert_eq!(*builder.attempt.lock().unwrap(), 4);
    assert!(budget.take_events().is_empty());
    Ok(())
}

#[tokio::test]
async fn exhausted_stream_cannot_reopen_without_a_reservation() -> Result<()> {
    let budget = TestBudget::new(0);
    let builder = MockBuilder::default();
    let op = Operator::new(builder.clone())?.layer(
        RetryLayer::new()
            .with_budget(budget.clone())
            .with_notify(budget.clone()),
    );
    let reader = op.service().read(op.context(), "file", OpRead::default())?;
    let (_, mut stream) = reader.open(BytesRange::from(0..13)).await?;
    let err = stream
        .read()
        .now_or_never()
        .expect("an exhausted budget must not sleep")
        .expect_err("recovery requires a reservation");
    assert!(err.is_persistent());
    assert_eq!(*builder.attempt.lock().unwrap(), 1);
    assert_eq!(
        budget.take_events(),
        [BudgetEvent::Reserve(Operation::Read)]
    );

    let err = stream
        .read()
        .await
        .expect_err("a stopped stream must not reopen");
    assert!(err.is_persistent());
    assert_eq!(*builder.attempt.lock().unwrap(), 1);
    assert!(budget.take_events().is_empty());
    Ok(())
}

#[tokio::test]
async fn reader_deposit_is_independent_of_chunk_size() -> Result<()> {
    let budget = TestBudget::new(0);
    let op = Operator::new(services::Memory::default())?;
    let content = Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz");
    op.write("file", content.clone()).await?;
    let op = op.layer(RetryLayer::new().with_budget(budget.clone()));

    for chunk in [None, Some(4), Some(16)] {
        let reader = match chunk {
            Some(chunk) => op.reader_with("file").chunk(chunk).concurrent(4).await?,
            None => op.reader("file").await?,
        };
        assert!(budget.take_events().is_empty());
        for _ in 0..2 {
            assert_eq!(
                reader.read(0..content.len() as u64).await?.to_bytes(),
                content
            );
        }
        assert_eq!(
            budget.take_events(),
            [BudgetEvent::Deposit(Operation::Read)]
        );
    }
    assert_eq!(budget.available(), 3);
    Ok(())
}

#[tokio::test]
async fn streaming_eof_and_direct_reads_share_one_deposit() -> Result<()> {
    let budget = TestBudget::new(0);
    let op = Operator::new(services::Memory::default())?;
    op.write("file", "content").await?;
    let op = op.layer(RetryLayer::new().with_budget(budget.clone()));

    let reader = op.service().read(op.context(), "file", OpRead::default())?;
    let (_, mut stream) = reader.open(BytesRange::default()).await?;
    assert!(!stream.read().await?.is_empty());
    assert!(budget.take_events().is_empty());

    let (eof, direct) = futures::join!(stream.read(), reader.read(BytesRange::from(0..7)));
    assert!(eof?.is_empty());
    assert_eq!(direct?.1.to_bytes(), Bytes::from_static(b"content"));
    assert_eq!(
        budget.take_events(),
        [BudgetEvent::Deposit(Operation::Read)]
    );

    let reader = op.service().read(op.context(), "file", OpRead::default())?;
    let (_, mut stream) = reader.open(BytesRange::default()).await?;
    assert_eq!(
        stream.read_all().await?.to_bytes(),
        Bytes::from_static(b"content")
    );
    assert_eq!(
        budget.take_events(),
        [BudgetEvent::Deposit(Operation::Read)]
    );
    Ok(())
}

#[tokio::test]
async fn stateful_handles_deposit_only_once_at_completion() -> Result<()> {
    use BudgetEvent::*;

    let budget = TestBudget::new(3);
    let layer = RetryLayer::new()
        .with_min_delay(Duration::ZERO)
        .with_budget(budget.clone());
    let service = layer.apply_service(Arc::new(services::Memory::default().build()?));
    let ctx = OperationContext::default();

    let mut writer = service.write(&ctx, "file", OpWrite::default())?;
    writer.write(Buffer::from("first")).await?;
    writer.write(Buffer::from("second")).await?;
    assert!(budget.take_events().is_empty());
    writer.close().await?;
    writer.close().await?;
    assert_eq!(budget.take_events(), [Deposit(Operation::Write)]);

    let mut writer = service.write(&ctx, "other", OpWrite::default())?;
    writer.write(Buffer::from("other")).await?;
    writer.abort().await?;
    assert!(budget.take_events().is_empty());
    service
        .write(&ctx, "second", OpWrite::default())?
        .close()
        .await?;
    assert_eq!(budget.take_events(), [Deposit(Operation::Write)]);

    drop(service.list(&ctx, "", OpList::default())?);
    let mut lister = service.list(&ctx, "", OpList::default())?;
    for _ in 0..2 {
        assert!(lister.next().await?.is_some());
    }
    assert!(budget.take_events().is_empty());
    assert!(lister.next().await?.is_none());
    assert!(lister.next().await?.is_none());
    assert_eq!(budget.take_events(), [Deposit(Operation::List)]);

    let mut deleter = service.delete(&ctx)?;
    deleter.delete("file", OpDelete::default()).await?;
    deleter.delete("second", OpDelete::default()).await?;
    assert!(budget.take_events().is_empty());
    deleter.close().await?;
    deleter.close().await?;
    assert_eq!(budget.take_events(), [Deposit(Operation::Delete)]);

    let service = layer.apply_service(Arc::new(MockService::default()));
    let mut copier = service.copy(&ctx, "file", "target", OpCopy::default())?;
    assert_eq!(copier.next().await?, Some(8));
    assert_eq!(copier.next().await?, Some(5));
    assert!(copier.next().await?.is_none());
    assert_eq!(
        budget.take_events(),
        [
            Reserve(Operation::Copy),
            Reserve(Operation::Copy),
            Reserve(Operation::Copy)
        ]
    );
    copier.close().await?;
    copier.close().await?;
    assert_eq!(budget.take_events(), [Deposit(Operation::Copy)]);
    Ok(())
}
