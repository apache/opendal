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

use crate::raw::oio;
use crate::raw::*;
use crate::types::context::ReadContext;
use crate::*;

/// The operation that reached the mock reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadRequestKind {
    Open,
    Read,
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
    failed_ranges: Mutex<VecDeque<BytesRange>>,
    pending_opens: AtomicUsize,
    requests: Mutex<Vec<ReadRequest>>,
}

impl ReadFault {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Fail the next request for `range` with a temporary error.
    pub(crate) fn with_failed_range(mut self, range: BytesRange) -> Self {
        self.failed_ranges
            .get_mut()
            .expect("failed_ranges mutex must be available")
            .push_back(range);
        self
    }

    /// Return `Pending` from the first poll of the first `open` call.
    pub(crate) fn with_pending_open(mut self) -> Self {
        self.pending_opens = AtomicUsize::new(1);
        self
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

    fn take_failure(&self, range: BytesRange) -> bool {
        let mut failed_ranges = self
            .failed_ranges
            .lock()
            .expect("failed_ranges mutex must be available");

        let Some(index) = failed_ranges
            .iter()
            .position(|failed_range| *failed_range == range)
        else {
            return false;
        };

        failed_ranges.remove(index);
        true
    }

    fn consume_pending_open(&self) -> bool {
        self.pending_opens.swap(0, Ordering::SeqCst) > 0
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
        if self.fault.take_failure(range) {
            return Err(
                Error::new(ErrorKind::Unexpected, "injected read open failure").set_temporary(),
            );
        }
        if self.fault.consume_pending_open() {
            YieldOnce::default().await;
        }

        Ok((RpRead::default(), Box::new(slice(&self.data, range))))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        self.fault.record(ReadRequestKind::Read, range);
        if self.fault.take_failure(range) {
            return Err(Error::new(ErrorKind::Unexpected, "injected read failure").set_temporary());
        }

        Ok((RpRead::default(), Buffer::from(slice(&self.data, range))))
    }
}

/// Create a `ReadContext` backed by fixed bytes and a fault injector.
pub(crate) fn new_mock_read_context(
    data: &'static [u8],
    fault: Arc<ReadFault>,
    options: OpReader,
) -> ReadContext {
    let ctx = OperationContext::new();
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
