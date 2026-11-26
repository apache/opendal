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

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use futures::poll;
use opendal_core::options::ReaderOptions;
use opendal_core::raw::oio::{Read, ReadStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::advance;

use super::*;

const DEFAULT_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
enum Request {
    Stat,
    Open(BytesRange),
    Read(BytesRange),
}

struct Attempt {
    request: Request,
    read_args: Option<OpRead>,
    reply: oneshot::Sender<Result<&'static str>>,
}

#[derive(Debug, Clone)]
struct TestService {
    attempts: mpsc::UnboundedSender<Attempt>,
    readers: Arc<AtomicUsize>,
}

impl TestService {
    async fn attempt(&self, request: Request, read_args: Option<OpRead>) -> Result<&'static str> {
        let (reply, response) = oneshot::channel();
        assert!(
            self.attempts
                .send(Attempt {
                    request,
                    read_args,
                    reply
                })
                .is_ok()
        );
        response.await.expect("test must supply a response")
    }
}

impl Service for TestService {
    type Reader = TestReader;
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
            read_with_version: true,
            read_with_if_match: true,
            ..Default::default()
        }
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        let body = self.attempt(Request::Stat, None).await?;
        Ok(RpStat::new(
            MetadataBuilder::file(body.len() as u64).build(),
        ))
    }

    fn read(&self, _: &OperationContext, _: &str, args: OpRead) -> Result<Self::Reader> {
        self.readers.fetch_add(1, Ordering::Relaxed);
        Ok(TestReader {
            service: self.clone(),
            args,
        })
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unreachable!("unexpected create_dir")
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unreachable!("unexpected write")
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unreachable!("unexpected delete")
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        unreachable!("unexpected list")
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        unreachable!("unexpected copy")
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unreachable!("unexpected rename")
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unreachable!("unexpected presign")
    }
}

struct TestReader {
    service: TestService,
    args: OpRead,
}

impl Read for TestReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let body = self
            .service
            .attempt(Request::Open(range), Some(self.args.clone()))
            .await?;
        Ok((
            RpRead::new(MetadataBuilder::file(100).build()),
            Box::new(Buffer::from(body)),
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let body = self
            .service
            .attempt(Request::Read(range), Some(self.args.clone()))
            .await?;
        Ok((RpRead::default(), Buffer::from(body)))
    }
}

fn setup(layer: HedgeLayer) -> (Operator, mpsc::UnboundedReceiver<Attempt>, Arc<AtomicUsize>) {
    let (attempts, received) = mpsc::unbounded_channel();
    let readers = Arc::new(AtomicUsize::new(0));
    let service = Arc::new(TestService {
        attempts,
        readers: readers.clone(),
    });
    let op = Operator::from_parts(OperationContext::default(), service).layer(layer);
    (op, received, readers)
}

#[derive(Clone, Default)]
struct RecordingExecutor(Arc<Mutex<Vec<Duration>>>);

impl Execute for RecordingExecutor {
    fn execute(&self, _: BoxedStaticFuture<()>) {
        panic!("hedging should not spawn background tasks");
    }

    fn sleep(&self, duration: Duration) -> Option<BoxedStaticFuture<()>> {
        self.0.lock().unwrap().push(duration);
        Some(Box::pin(tokio::time::sleep(duration)))
    }
}

fn temporary(message: &'static str) -> Error {
    Error::new(ErrorKind::Unexpected, message)
        .with_context("status", 503)
        .set_temporary()
}

#[tokio::test(start_paused = true)]
async fn primary_completion_before_delay_returns_directly() {
    let (op, mut attempts, _) = setup(HedgeLayer::new());
    for result in [Ok("primary"), Err(temporary("primary unavailable"))] {
        let mut stat = pin!(op.stat("object"));
        assert!(poll!(&mut stat).is_pending());
        let attempt = attempts.try_recv().unwrap();
        assert_eq!(attempt.request, Request::Stat);
        let success = result.is_ok();
        attempt.reply.send(result).unwrap();

        let Poll::Ready(result) = poll!(&mut stat) else {
            panic!("the primary result must be returned immediately");
        };
        if success {
            assert_eq!(result.unwrap().content_length(), 7);
        } else {
            let err = result.unwrap_err();
            assert!(err.is_temporary());
            assert_eq!(err.message(), "primary unavailable");
        }
        assert!(attempts.try_recv().is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn stat_hedge_can_win_with_success_or_permanent_error() {
    let (op, mut attempts, _) = setup(HedgeLayer::new());
    for result in [Ok("hedge"), Err(Error::new(ErrorKind::NotFound, "missing"))] {
        let mut stat = pin!(op.stat("object"));
        assert!(poll!(&mut stat).is_pending());
        let primary = attempts.try_recv().unwrap();
        advance(DEFAULT_DELAY - Duration::from_secs(1)).await;
        assert!(poll!(&mut stat).is_pending());
        assert!(attempts.try_recv().is_err());
        advance(Duration::from_secs(1)).await;
        assert!(poll!(&mut stat).is_pending());
        let hedge = attempts.try_recv().unwrap();
        advance(DEFAULT_DELAY * 2).await;
        assert!(poll!(&mut stat).is_pending());
        assert!(attempts.try_recv().is_err());

        let success = result.is_ok();
        hedge.reply.send(result).unwrap();
        let Poll::Ready(result) = poll!(&mut stat) else {
            panic!("the hedge result must be returned immediately");
        };
        if success {
            assert_eq!(result.unwrap().content_length(), 5);
        } else {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);
        }
        assert!(primary.reply.is_closed());
        assert!(attempts.try_recv().is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn temporary_error_waits_for_in_flight_attempts_and_stops_new_attempts() {
    let (op, mut attempts, _) = setup(HedgeLayer::new().with_max_hedges(3));
    for result in [Ok("primary"), Err(temporary("primary unavailable"))] {
        let mut stat = pin!(op.stat("object"));
        assert!(poll!(&mut stat).is_pending());
        let primary = attempts.try_recv().unwrap();
        advance(DEFAULT_DELAY).await;
        assert!(poll!(&mut stat).is_pending());
        let hedge = attempts.try_recv().unwrap();
        hedge
            .reply
            .send(Err(temporary("hedge unavailable")))
            .unwrap();
        assert!(poll!(&mut stat).is_pending());

        advance(DEFAULT_DELAY * 3).await;
        assert!(poll!(&mut stat).is_pending());
        assert!(attempts.try_recv().is_err());
        let success = result.is_ok();
        primary.reply.send(result).unwrap();
        let Poll::Ready(result) = poll!(&mut stat) else {
            panic!("the remaining attempt has completed");
        };
        if success {
            assert_eq!(result.unwrap().content_length(), 7);
        } else {
            let err = result.unwrap_err();
            assert!(err.is_temporary());
            assert_eq!(err.message(), "primary unavailable");
            assert!(err.to_string().contains("status: 503"));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn hedges_are_capped_and_spaced_after_a_stall_and_cancel_with_the_caller() {
    let delay = Duration::from_secs(2);
    let (op, mut attempts, _) = setup(HedgeLayer::new().with_max_hedges(2).with_delay(delay));
    let (primary, first_hedge, second_hedge) = {
        let mut stat = pin!(op.stat("object"));
        assert!(poll!(&mut stat).is_pending());
        let primary = attempts.try_recv().unwrap();
        advance(delay * 10).await;
        assert!(poll!(&mut stat).is_pending());
        let first_hedge = attempts.try_recv().unwrap();
        assert!(attempts.try_recv().is_err());
        advance(delay / 2).await;
        assert!(poll!(&mut stat).is_pending());
        assert!(attempts.try_recv().is_err());
        advance(delay / 2).await;
        assert!(poll!(&mut stat).is_pending());
        let second_hedge = attempts.try_recv().unwrap();
        advance(delay * 10).await;
        assert!(poll!(&mut stat).is_pending());
        assert!(attempts.try_recv().is_err());
        (primary, first_hedge, second_hedge)
    };

    assert!(primary.reply.is_closed());
    assert!(first_hedge.reply.is_closed());
    assert!(second_hedge.reply.is_closed());
}

#[tokio::test(start_paused = true)]
async fn stat_hedge_uses_its_delay_override_on_the_context_executor() {
    let delay = Duration::from_secs(1);
    let (op, mut attempts, _) = setup(HedgeLayer::new().with_stat_delay(delay));
    let executor = RecordingExecutor::default();
    let context = op.context().with_executor(Executor::with(executor.clone()));
    let op = op.with_context(context);
    let mut stat = pin!(op.stat("object"));
    assert!(poll!(&mut stat).is_pending());
    assert_eq!(*executor.0.lock().unwrap(), [delay]);
    let primary = attempts.try_recv().unwrap();
    advance(delay).await;
    assert!(poll!(&mut stat).is_pending());
    attempts
        .try_recv()
        .unwrap()
        .reply
        .send(Ok("hedge"))
        .unwrap();
    assert_eq!(stat.await.unwrap().content_length(), 5);
    assert!(primary.reply.is_closed());
}

#[tokio::test(start_paused = true)]
async fn hedging_can_be_disabled_globally_or_per_operation() {
    for layer in [
        HedgeLayer::new().with_max_hedges(0),
        HedgeLayer::new().without_stat_hedge(),
    ] {
        let (op, mut attempts, _) = setup(layer);
        let mut stat = pin!(op.stat("object"));
        assert!(poll!(&mut stat).is_pending());
        let primary = attempts.try_recv().unwrap();
        advance(DEFAULT_DELAY * 2).await;
        assert!(poll!(&mut stat).is_pending());
        assert!(attempts.try_recv().is_err());
        primary.reply.send(Ok("primary")).unwrap();
        assert_eq!(stat.await.unwrap().content_length(), 7);
    }

    let (op, mut attempts, _) = setup(HedgeLayer::new().without_read_hedge());
    let reader = op
        .service()
        .read(op.context(), "object", OpRead::new())
        .unwrap();
    let range = BytesRange::from(7..10);
    let mut open = pin!(reader.open(range));
    let mut read = pin!(reader.read(range));
    assert!(poll!(&mut open).is_pending());
    assert!(poll!(&mut read).is_pending());
    let primary_open = attempts.try_recv().unwrap();
    let primary_read = attempts.try_recv().unwrap();
    advance(DEFAULT_DELAY * 2).await;
    assert!(poll!(&mut open).is_pending());
    assert!(poll!(&mut read).is_pending());
    assert!(attempts.try_recv().is_err());
    primary_open.reply.send(Ok("abc")).unwrap();
    primary_read.reply.send(Ok("abc")).unwrap();
    assert_eq!(open.await.unwrap().1.read().await.unwrap().to_vec(), b"abc");
    assert_eq!(read.await.unwrap().1.to_vec(), b"abc");
}

#[tokio::test(start_paused = true)]
async fn read_hedges_preserve_options_and_return_the_winning_stream_intact() {
    let delay = Duration::from_secs(1);
    let (op, mut attempts, readers) = setup(HedgeLayer::new().with_read_delay(delay));
    let executor = RecordingExecutor::default();
    let context = op.context().with_executor(Executor::with(executor.clone()));
    let op = op.with_context(context);
    let (args, _) = ReaderOptions {
        version: Some("version-1".to_string()),
        if_match: Some("etag-1".to_string()),
        ..Default::default()
    }
    .into();
    let reader = op.service().read(op.context(), "object", args).unwrap();
    drop(op);
    let range = BytesRange::from(7..10);
    let mut open = pin!(reader.open(range));
    let mut read = pin!(reader.read(range));
    assert!(poll!(&mut open).is_pending());
    assert!(poll!(&mut read).is_pending());
    let primary_open = attempts.try_recv().unwrap();
    let primary_read = attempts.try_recv().unwrap();
    assert_eq!(*executor.0.lock().unwrap(), [delay, delay]);
    advance(delay).await;
    assert!(poll!(&mut open).is_pending());
    assert!(poll!(&mut read).is_pending());
    let hedge_open = attempts.try_recv().unwrap();
    let hedge_read = attempts.try_recv().unwrap();
    for attempt in [&primary_open, &hedge_open] {
        assert_eq!(attempt.request, Request::Open(range));
    }
    for attempt in [&primary_read, &hedge_read] {
        assert_eq!(attempt.request, Request::Read(range));
    }
    for attempt in [&primary_open, &primary_read, &hedge_open, &hedge_read] {
        let args = attempt.read_args.as_ref().unwrap();
        assert_eq!(args.version(), Some("version-1"));
        assert_eq!(args.if_match(), Some("etag-1"));
    }

    hedge_open.reply.send(Ok("abc")).unwrap();
    hedge_read.reply.send(Ok("xyz")).unwrap();
    let (rp, mut stream) = open.await.unwrap();
    assert!(primary_open.reply.is_closed());
    assert_eq!(rp.metadata().unwrap().content_length(), 100);
    assert_eq!(stream.read().await.unwrap().to_vec(), b"abc");
    assert!(stream.read().await.unwrap().is_empty());
    assert_eq!(read.await.unwrap().1.to_vec(), b"xyz");
    assert!(primary_read.reply.is_closed());
    assert_eq!(readers.load(Ordering::Relaxed), 1);
    assert!(attempts.try_recv().is_err());
}
