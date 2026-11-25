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

use std::collections::VecDeque;
use std::future::pending;
use std::pin::pin;
use std::sync::atomic::AtomicUsize;

use bytes::Bytes;
use oio::List as _;
use oio::Read as _;
use oio::ReadStream as _;
use opendal_layer_timeout::TimeoutLayer;

use super::*;

#[derive(Debug, PartialEq, Eq)]
enum BudgetEvent {
    Deposit(Operation),
    Reserve(Operation),
    Release(Operation),
}

#[derive(Debug, Default)]
struct TrackingBudgetState {
    available: usize,
    events: Vec<BudgetEvent>,
}

#[derive(Debug, Clone)]
struct TrackingBudget(Arc<Mutex<TrackingBudgetState>>);

impl TrackingBudget {
    fn new(available: usize) -> Self {
        Self(Arc::new(Mutex::new(TrackingBudgetState {
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

impl RetryBudget for TrackingBudget {
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

#[derive(Debug, Clone, Copy)]
enum Action {
    Pending,
    TemporaryErr,
    Ok,
}

#[derive(Debug, Clone, Default)]
struct CancelState {
    delete_close_actions: Arc<Mutex<VecDeque<Action>>>,
    deleted_paths: Arc<Mutex<Vec<String>>>,
    copy_abort_actions: Arc<Mutex<VecDeque<Action>>>,
    copy_aborted: Arc<AtomicBool>,
    copy_abort_attempts: Arc<AtomicUsize>,
    write_close_actions: Arc<Mutex<VecDeque<Action>>>,
    write_abort_actions: Arc<Mutex<VecDeque<Action>>>,
    write_aborted: Arc<AtomicBool>,
    write_abort_attempts: Arc<AtomicUsize>,
    copy_from_actions: Arc<Mutex<VecDeque<Action>>>,
    copy_from_ranges: Arc<Mutex<Vec<(String, BytesRange)>>>,
    list_next_actions: Arc<Mutex<VecDeque<Action>>>,
}

#[derive(Debug, Clone, Default)]
struct CancelBuilder {
    state: CancelState,
}

impl Builder for CancelBuilder {
    type Config = ();

    fn build(self) -> Result<impl Service> {
        Ok(CancelService { state: self.state })
    }
}

#[derive(Debug, Clone)]
struct CancelService {
    state: CancelState,
}

impl Service for CancelService {
    type Reader = ();
    type Writer = CancelWriter;
    type Lister = CancelLister;
    type Deleter = CancelDeleter;
    type Copier = CancelCopier;
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("cancel-mock")
    }

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            stat: true,
            write: true,
            write_can_multi: true,
            write_can_copy_from: true,
            delete: true,
            delete_max_size: Some(10),
            list: true,
            copy: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        Ok(RpStat::new(MetadataBuilder::file(20).build()))
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(())
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        Ok(CancelWriter {
            state: self.state.clone(),
            written_bytes: 0,
        })
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        Ok(CancelDeleter {
            state: self.state.clone(),
            queued: Vec::new(),
        })
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Ok(CancelCopier {
            state: self.state.clone(),
        })
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        Ok(CancelLister {
            state: self.state.clone(),
            entries: VecDeque::from(["a"]),
        })
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

struct CancelDeleter {
    state: CancelState,
    queued: Vec<String>,
}

impl oio::Delete for CancelDeleter {
    async fn delete(&mut self, path: &str, _: OpDelete) -> Result<()> {
        self.queued.push(path.to_owned());
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let action = self
            .state
            .delete_close_actions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Action::Ok);
        match action {
            Action::Pending => pending::<Result<()>>().await,
            Action::TemporaryErr => {
                Err(Error::new(ErrorKind::RateLimited, "temporary delete failure").set_temporary())
            }
            Action::Ok => {
                self.state
                    .deleted_paths
                    .lock()
                    .unwrap()
                    .extend(self.queued.drain(..));
                Ok(())
            }
        }
    }
}

struct CancelCopier {
    state: CancelState,
}

impl oio::Copy for CancelCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<Metadata> {
        Ok(MetadataBuilder::unknown().build())
    }

    async fn abort(&mut self) -> Result<()> {
        self.state
            .copy_abort_attempts
            .fetch_add(1, Ordering::SeqCst);
        let action = self
            .state
            .copy_abort_actions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Action::Ok);
        match action {
            Action::Pending => pending::<Result<()>>().await,
            Action::TemporaryErr => {
                Err(Error::new(ErrorKind::RateLimited, "temporary abort failure").set_temporary())
            }
            Action::Ok => {
                self.state.copy_aborted.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    }
}

struct CancelWriter {
    state: CancelState,
    written_bytes: u64,
}

impl oio::Write for CancelWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.written_bytes += bs.len() as u64;
        Ok(())
    }

    async fn copy_from(&mut self, path: &str, _: OpRead, range: BytesRange) -> Result<()> {
        self.state
            .copy_from_ranges
            .lock()
            .unwrap()
            .push((path.to_owned(), range));
        let action = self
            .state
            .copy_from_actions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Action::Ok);
        match action {
            Action::Pending => pending::<Result<()>>().await,
            Action::TemporaryErr => Err(Error::new(
                ErrorKind::RateLimited,
                "temporary copy_from failure",
            )
            .set_temporary()),
            Action::Ok => {
                self.written_bytes += range.size().unwrap_or(0);
                Ok(())
            }
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        let action = self
            .state
            .write_close_actions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Action::Ok);
        match action {
            Action::Pending => pending::<Result<Metadata>>().await,
            Action::TemporaryErr => {
                Err(Error::new(ErrorKind::RateLimited, "temporary close failure").set_temporary())
            }
            Action::Ok => Ok(MetadataBuilder::file(self.written_bytes).build()),
        }
    }

    async fn abort(&mut self) -> Result<()> {
        self.state
            .write_abort_attempts
            .fetch_add(1, Ordering::SeqCst);
        let action = self
            .state
            .write_abort_actions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Action::Ok);
        match action {
            Action::Pending => pending::<Result<()>>().await,
            Action::TemporaryErr => {
                Err(Error::new(ErrorKind::RateLimited, "temporary abort failure").set_temporary())
            }
            Action::Ok => {
                self.state.write_aborted.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    }
}

struct CancelLister {
    state: CancelState,
    entries: VecDeque<&'static str>,
}

impl oio::List for CancelLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let action = self
            .state
            .list_next_actions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Action::Ok);
        match action {
            Action::Pending => pending::<Result<Option<oio::Entry>>>().await,
            Action::TemporaryErr => {
                Err(Error::new(ErrorKind::RateLimited, "temporary list failure").set_temporary())
            }
            Action::Ok => Ok(self
                .entries
                .pop_front()
                .map(|path| oio::Entry::new(path, MetadataBuilder::file(0).build()))),
        }
    }
}

#[derive(Debug)]
enum StreamStep {
    Chunk(&'static [u8]),
    Pending,
}

#[derive(Debug, Clone, Default)]
struct CancelReadState {
    opened_ranges: Arc<Mutex<Vec<BytesRange>>>,
    steps: Arc<Mutex<VecDeque<StreamStep>>>,
}

struct CancelReader {
    state: CancelReadState,
    content_length: u64,
}

impl oio::Read for CancelReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.state.opened_ranges.lock().unwrap().push(range);
        let rp = RpRead::new(MetadataBuilder::file(self.content_length).build());
        Ok((
            rp,
            Box::new(CancelReadStream {
                state: self.state.clone(),
            }),
        ))
    }

    async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
        unreachable!("only streaming reads are exercised")
    }
}

struct CancelReadStream {
    state: CancelReadState,
}

impl oio::ReadStream for CancelReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        let step = self.state.steps.lock().unwrap().pop_front();
        match step {
            Some(StreamStep::Chunk(bytes)) => Ok(Buffer::from(Bytes::from_static(bytes))),
            Some(StreamStep::Pending) => pending::<Result<Buffer>>().await,
            None => Ok(Buffer::new()),
        }
    }
}

#[tokio::test]
async fn deleter_cancel_mid_flight_preserves_queued_paths_and_releases_budget() -> Result<()> {
    use BudgetEvent::*;

    let state = CancelState::default();
    *state.delete_close_actions.lock().unwrap() =
        VecDeque::from([Action::Pending, Action::TemporaryErr, Action::Ok]);
    let budget = TrackingBudget::new(1);

    let op = Operator::new(CancelBuilder {
        state: state.clone(),
    })?
    .layer(
        RetryLayer::new()
            .with_min_delay(Duration::from_secs(60))
            .with_max_delay(Duration::from_secs(60))
            .with_max_times(2)
            .with_budget(budget.clone()),
    );

    let mut deleter = op.deleter().await?;
    deleter.delete("a").await?;
    deleter.delete("b").await?;

    // Cancel while inner `close()` is pending.
    {
        let mut fut = pin!(deleter.close());
        assert!(futures::poll!(&mut fut).is_pending());
    }
    assert!(state.deleted_paths.lock().unwrap().is_empty());
    assert!(budget.take_events().is_empty());

    // Cancel while `RetryLayer` is sleeping in backoff; the permit must be released.
    {
        let mut fut = pin!(deleter.close());
        assert!(futures::poll!(&mut fut).is_pending());
        assert_eq!(budget.available(), 0);
        assert_eq!(budget.take_events(), [Reserve(Operation::Delete)]);
    }
    assert_eq!(budget.available(), 1);
    assert_eq!(budget.take_events(), [Release(Operation::Delete)]);
    assert!(state.deleted_paths.lock().unwrap().is_empty());

    // Subsequent flush/close completes all queued paths cleanly and deposits once.
    deleter.close().await?;
    assert_eq!(*state.deleted_paths.lock().unwrap(), vec!["a", "b"]);
    assert_eq!(budget.available(), 2);
    assert_eq!(budget.take_events(), [Deposit(Operation::Delete)]);
    Ok(())
}

#[tokio::test]
async fn copier_abort_cancel_mid_flight_remains_abortable() -> Result<()> {
    let state = CancelState::default();
    *state.copy_abort_actions.lock().unwrap() = VecDeque::from([Action::Pending, Action::Ok]);

    let op = Operator::new(CancelBuilder {
        state: state.clone(),
    })?
    .layer(RetryLayer::new().with_min_delay(Duration::ZERO));

    let mut copier = op.copier("from", "to").await?;
    {
        let mut fut = pin!(copier.abort());
        assert!(futures::poll!(&mut fut).is_pending());
    }
    assert_eq!(state.copy_abort_attempts.load(Ordering::SeqCst), 1);
    assert!(!state.copy_aborted.load(Ordering::SeqCst));

    copier.abort().await?;
    assert_eq!(state.copy_abort_attempts.load(Ordering::SeqCst), 2);
    assert!(state.copy_aborted.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn writer_close_timeout_outside_retry_allows_abort() -> Result<()> {
    use BudgetEvent::*;

    let state = CancelState::default();
    *state.write_close_actions.lock().unwrap() = VecDeque::from([Action::TemporaryErr]);
    let budget = TrackingBudget::new(1);

    let op = Operator::new(CancelBuilder {
        state: state.clone(),
    })?
    .layer(
        RetryLayer::new()
            .with_min_delay(Duration::from_secs(60))
            .with_max_delay(Duration::from_secs(60))
            .with_max_times(3)
            .with_budget(budget.clone()),
    )
    .layer(TimeoutLayer::default().with_io_timeout(Duration::from_millis(20)));

    let mut writer = op.writer("test_write").await?;
    writer.write("aaa").await?;
    writer.write("bbb").await?;

    let err = writer
        .close()
        .await
        .expect_err("outer timeout must interrupt backoff sleep");
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert_eq!(err.message(), "io operation timeout reached");
    assert_eq!(budget.available(), 1);
    assert_eq!(
        budget.take_events(),
        [Reserve(Operation::Write), Release(Operation::Write)]
    );

    writer.abort().await?;
    assert!(state.write_aborted.load(Ordering::SeqCst));
    assert_eq!(state.write_abort_attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn writer_close_and_abort_cancel_mid_flight_recover() -> Result<()> {
    use BudgetEvent::*;

    let state = CancelState::default();
    *state.write_close_actions.lock().unwrap() = VecDeque::from([Action::Pending, Action::Ok]);
    *state.write_abort_actions.lock().unwrap() = VecDeque::from([Action::Pending, Action::Ok]);
    let budget = TrackingBudget::new(0);

    let op = Operator::new(CancelBuilder {
        state: state.clone(),
    })?
    .layer(
        RetryLayer::new()
            .with_min_delay(Duration::ZERO)
            .with_budget(budget.clone()),
    );

    // Cancel `Writer::close` mid-flight and retry `close`.
    let mut writer = op.writer("close_recovery").await?;
    writer.write("hello!").await?;
    {
        let mut fut = pin!(writer.close());
        assert!(futures::poll!(&mut fut).is_pending());
    }
    let meta = writer.close().await?;
    assert_eq!(meta.content_length(), 6);
    assert_eq!(budget.take_events(), [Deposit(Operation::Write)]);

    // Cancel `Writer::abort` mid-flight and retry `abort`.
    let mut writer = op.writer("abort_recovery").await?;
    writer.write("hello!").await?;
    {
        let mut fut = pin!(writer.abort());
        assert!(futures::poll!(&mut fut).is_pending());
    }
    assert_eq!(state.write_abort_attempts.load(Ordering::SeqCst), 1);
    assert!(!state.write_aborted.load(Ordering::SeqCst));

    writer.abort().await?;
    assert_eq!(state.write_abort_attempts.load(Ordering::SeqCst), 2);
    assert!(state.write_aborted.load(Ordering::SeqCst));
    assert!(budget.take_events().is_empty());
    Ok(())
}

#[tokio::test]
async fn writer_copy_from_cancel_mid_flight_resumes_under_retry_layer() -> Result<()> {
    let state = CancelState::default();
    *state.copy_from_actions.lock().unwrap() =
        VecDeque::from([Action::Ok, Action::Pending, Action::Ok]);

    let op = Operator::new(CancelBuilder {
        state: state.clone(),
    })?
    .layer(RetryLayer::new().with_min_delay(Duration::ZERO));

    let mut writer = op.writer_with("target").chunk(10).await?;
    {
        let mut fut = pin!(writer.copy_from("source", 0..20));
        assert!(futures::poll!(&mut fut).is_pending());
    }

    writer.copy_from("source", 0..20).await?;
    writer.close().await?;

    assert_eq!(
        *state.copy_from_ranges.lock().unwrap(),
        vec![
            ("source".to_owned(), BytesRange::new(0, Some(10))),
            ("source".to_owned(), BytesRange::new(10, Some(10))),
            ("source".to_owned(), BytesRange::new(10, Some(10))),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn lister_cancel_mid_flight_does_not_poison() -> Result<()> {
    use BudgetEvent::*;

    let state = CancelState::default();
    *state.list_next_actions.lock().unwrap() =
        VecDeque::from([Action::Pending, Action::Ok, Action::Ok]);
    let budget = TrackingBudget::new(0);

    let service = RetryLayer::new()
        .with_min_delay(Duration::ZERO)
        .with_budget(budget.clone())
        .apply_service(Arc::new(CancelService { state }));
    let ctx = OperationContext::default();
    let mut lister = service.list(&ctx, "", OpList::default())?;

    {
        let mut fut = pin!(lister.next());
        assert!(futures::poll!(&mut fut).is_pending());
    }
    assert!(budget.take_events().is_empty());

    let entry = lister.next().await?.expect("first entry must be returned");
    assert_eq!(entry.path(), "a");
    assert!(lister.next().await?.is_none());
    assert_eq!(budget.take_events(), [Deposit(Operation::List)]);
    Ok(())
}

#[tokio::test]
async fn read_stream_cancel_mid_flight_preserves_reopen_guard_with_budget() -> Result<()> {
    // With RetryBudget, dropping an in-flight read must require reopening the stream
    // so callers cannot bypass the budget on subsequent calls.
    let budgeted_state = CancelReadState::default();
    *budgeted_state.steps.lock().unwrap() = VecDeque::from([StreamStep::Pending]);
    let budget = TrackingBudget::new(1);
    let reader = RetryReader::new(
        CancelReader {
            state: budgeted_state.clone(),
            content_length: 13,
        },
        RetryLayer::new()
            .with_min_delay(Duration::ZERO)
            .with_budget(budget.clone())
            .policy,
    );

    let (_, mut stream) = reader.open(BytesRange::from(0..13)).await?;
    {
        let mut fut = pin!(stream.read());
        assert!(futures::poll!(&mut fut).is_pending());
    }

    let err = stream
        .read()
        .await
        .expect_err("cancelled stream under retry budget must require reopen");
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert_eq!(err.message(), "read stream must be reopened");
    assert!(err.is_persistent());
    assert_eq!(
        *budgeted_state.opened_ranges.lock().unwrap(),
        vec![BytesRange::from(0..13)]
    );
    assert!(budget.take_events().is_empty());

    // Without RetryBudget, dropping an in-flight read after progress reopens at
    // the exact remaining range.
    let unbudgeted_state = CancelReadState::default();
    *unbudgeted_state.steps.lock().unwrap() = VecDeque::from([
        StreamStep::Chunk(b"Hello, "),
        StreamStep::Pending,
        StreamStep::Chunk(b"World!"),
    ]);
    let reader = RetryReader::new(
        CancelReader {
            state: unbudgeted_state.clone(),
            content_length: 13,
        },
        RetryLayer::new().with_min_delay(Duration::ZERO).policy,
    );

    let (_, mut stream) = reader.open(BytesRange::from(0..13)).await?;
    assert_eq!(
        stream.read().await?.to_bytes(),
        Bytes::from_static(b"Hello, ")
    );
    {
        let mut fut = pin!(stream.read());
        assert!(futures::poll!(&mut fut).is_pending());
    }
    assert_eq!(
        stream.read().await?.to_bytes(),
        Bytes::from_static(b"World!")
    );
    assert_eq!(
        *unbudgeted_state.opened_ranges.lock().unwrap(),
        vec![BytesRange::from(0..13), BytesRange::from(7..13)]
    );
    Ok(())
}
