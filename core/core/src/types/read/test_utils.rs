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

//! Deterministic read fixtures shared by read path tests.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::channel::oneshot;

use crate::raw::oio;
use crate::raw::*;
use crate::types::context::ReadContext;
use crate::*;

/// The operation that reached the mock reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadRequestKind {
    Open,
    Read,
    StreamRead,
}

/// The operation a configured failure applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailedRequestKind {
    Open,
    Read,
}

#[derive(Debug, Clone, Copy)]
struct FailedRequest {
    kind: FailedRequestKind,
    range: BytesRange,
}

/// One request observed by [`ReadFault`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadRequest {
    pub(crate) kind: ReadRequestKind,
    pub(crate) range: BytesRange,
}

/// Records read requests and injects deterministic failures or pending polls.
#[derive(Debug, Default)]
pub(crate) struct ReadFault {
    failed_requests: Mutex<VecDeque<FailedRequest>>,
    pending_opens: AtomicUsize,
    pending_read: Mutex<Option<oneshot::Receiver<()>>>,
    pending_read_tx: Mutex<Option<oneshot::Sender<()>>>,
    requests: Mutex<Vec<ReadRequest>>,
}

impl ReadFault {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Fail the next `open` for `range` with a temporary error.
    pub(crate) fn with_failed_open_range(mut self, range: BytesRange) -> Self {
        self.push_failed_request(FailedRequestKind::Open, range);
        self
    }

    /// Fail the next read for `range` with a temporary error.
    pub(crate) fn with_failed_read_range(mut self, range: BytesRange) -> Self {
        self.push_failed_request(FailedRequestKind::Read, range);
        self
    }

    fn push_failed_request(&mut self, kind: FailedRequestKind, range: BytesRange) {
        self.failed_requests
            .get_mut()
            .expect("failed_requests mutex must be available")
            .push_back(FailedRequest { kind, range });
    }

    /// Return `Pending` from the first poll of the first `open` call.
    pub(crate) fn with_pending_open(mut self) -> Self {
        self.pending_opens = AtomicUsize::new(1);
        self
    }

    /// Suspend the first read until [`Self::release_pending_read`] is called.
    pub(crate) fn with_pending_read(mut self) -> Self {
        let (tx, rx) = oneshot::channel();
        self.pending_read = Mutex::new(Some(rx));
        self.pending_read_tx = Mutex::new(Some(tx));
        self
    }

    /// Resume a read suspended by [`Self::with_pending_read`].
    pub(crate) fn release_pending_read(&self) {
        let sender = self
            .pending_read_tx
            .lock()
            .expect("pending_read_tx mutex must be available")
            .take();
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
    }

    /// Ranges passed to `open`, in call order.
    pub(crate) fn opened_ranges(&self) -> Vec<BytesRange> {
        self.requests
            .lock()
            .expect("requests mutex must be available")
            .iter()
            .filter(|request| request.kind == ReadRequestKind::Open)
            .map(|request| request.range)
            .collect()
    }

    /// Ranges passed to `read`, in call order.
    pub(crate) fn read_ranges(&self) -> Vec<BytesRange> {
        self.requests
            .lock()
            .expect("requests mutex must be available")
            .iter()
            .filter(|request| request.kind == ReadRequestKind::Read)
            .map(|request| request.range)
            .collect()
    }

    fn record(&self, kind: ReadRequestKind, range: BytesRange) {
        self.requests
            .lock()
            .expect("requests mutex must be available")
            .push(ReadRequest { kind, range });
    }

    fn take_failure(&self, kind: ReadRequestKind, range: BytesRange) -> bool {
        let failed_kind = match kind {
            ReadRequestKind::Open => FailedRequestKind::Open,
            ReadRequestKind::Read | ReadRequestKind::StreamRead => FailedRequestKind::Read,
        };

        let mut failed_requests = self
            .failed_requests
            .lock()
            .expect("failed_requests mutex must be available");

        let Some(index) = failed_requests
            .iter()
            .position(|failed| failed.kind == failed_kind && failed.range == range)
        else {
            return false;
        };

        failed_requests.remove(index);
        true
    }

    fn consume_pending_open(&self) -> bool {
        self.pending_opens.swap(0, Ordering::SeqCst) > 0
    }

    async fn wait_pending_read(&self) {
        let receiver = self
            .pending_read
            .lock()
            .expect("pending_read mutex must be available")
            .take();
        if let Some(receiver) = receiver {
            let _ = receiver.await;
        }
    }
}

/// A service that serves fixed bytes and delegates faults to [`ReadFault`].
#[derive(Debug)]
pub(crate) struct MockReadService {
    data: Bytes,
    fault: Arc<ReadFault>,
}

impl MockReadService {
    pub(crate) fn new(data: &'static [u8], fault: Arc<ReadFault>) -> Self {
        Self {
            data: Bytes::from_static(data),
            fault,
        }
    }
}

impl Service for MockReadService {
    type Reader = oio::Reader;
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("mock", "", "")
    }

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            stat: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unreachable!()
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        Ok(RpStat::new(
            MetadataBuilder::file(self.data.len() as u64).build(),
        ))
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(Box::new(MockReader {
            data: self.data.clone(),
            fault: self.fault.clone(),
        }))
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unreachable!()
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unreachable!()
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
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

struct MockReader {
    data: Bytes,
    fault: Arc<ReadFault>,
}

impl oio::Read for MockReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.fault.record(ReadRequestKind::Open, range);
        if self.fault.take_failure(ReadRequestKind::Open, range) {
            return Err(
                Error::new(ErrorKind::Unexpected, "injected read open failure").set_temporary(),
            );
        }
        if self.fault.consume_pending_open() {
            YieldOnce::default().await;
        }

        let metadata = MetadataBuilder::file(self.data.len() as u64).build();
        Ok((
            RpRead::new(metadata),
            Box::new(MockReadStream {
                data: slice(&self.data, range),
                range,
                fault: self.fault.clone(),
            }),
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        self.fault.record(ReadRequestKind::Read, range);
        if self.fault.take_failure(ReadRequestKind::Read, range) {
            return Err(Error::new(ErrorKind::Unexpected, "injected read failure").set_temporary());
        }
        self.fault.wait_pending_read().await;

        Ok((RpRead::default(), Buffer::from(slice(&self.data, range))))
    }
}

struct MockReadStream {
    data: Bytes,
    range: BytesRange,
    fault: Arc<ReadFault>,
}

impl oio::ReadStream for MockReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        self.fault.record(ReadRequestKind::StreamRead, self.range);
        if self
            .fault
            .take_failure(ReadRequestKind::StreamRead, self.range)
        {
            return Err(
                Error::new(ErrorKind::Unexpected, "injected stream read failure").set_temporary(),
            );
        }
        self.fault.wait_pending_read().await;

        Ok(Buffer::from(std::mem::take(&mut self.data)))
    }
}

/// Create a `ReadContext` backed by fixed bytes and a fault injector.
pub(crate) fn new_mock_read_context(
    data: &'static [u8],
    fault: Arc<ReadFault>,
    options: OpReader,
) -> ReadContext {
    let ctx = OperationContext::new().with_executor(crate::types::execute::test_executor());
    let args = OpRead::new();
    let srv = Arc::new(MockReadService::new(data, fault));
    let reader = srv
        .read(&ctx, "test", args.clone())
        .expect("mock read must succeed");

    ReadContext::new(ctx, srv, "test".to_string(), args, options, reader)
}

fn slice(data: &Bytes, range: BytesRange) -> Bytes {
    let len = data.len() as u64;
    let (offset, size) = match range {
        BytesRange::Range { offset, size } => (offset, size),
        BytesRange::Suffix { size } => (len.saturating_sub(size), Some(size)),
    };
    let end = size
        .map(|size| offset.saturating_add(size))
        .unwrap_or(len)
        .min(len);
    let start = offset.min(end);

    data.slice(start as usize..end as usize)
}

/// Yields once and then completes, so a caller can drop the future while pending.
#[derive(Default)]
struct YieldOnce {
    yielded: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
