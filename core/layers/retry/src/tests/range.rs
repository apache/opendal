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
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::*;

#[derive(Debug, Clone)]
struct MockRangeReader {
    content: Bytes,
    content_length: Option<u64>,
    ranges: Arc<Mutex<Vec<BytesRange>>>,
    /// For each failed open, return this many bytes before a temporary error.
    /// Opens after this list return the full requested slice.
    failed_reads: Vec<usize>,
}

impl MockRangeReader {
    fn new(content: &'static [u8], content_length: Option<u64>, failed_reads: Vec<usize>) -> Self {
        Self {
            content: Bytes::from_static(content),
            content_length,
            ranges: Arc::new(Mutex::new(Vec::new())),
            failed_reads,
        }
    }

    fn ranges(&self) -> Vec<BytesRange> {
        self.ranges.lock().unwrap().clone()
    }
}

impl oio::StreamRead for MockRangeReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let failed_read = {
            let mut ranges = self.ranges.lock().unwrap();
            ranges.push(range);
            self.failed_reads.get(ranges.len() - 1).copied()
        };
        let content_range = range.to_content_range(self.content.len())?;
        let rp = match self.content_length {
            Some(content_length) => RpRead::new(MetadataBuilder::file(content_length).build()),
            None => RpRead::default(),
        };
        let mut buf = Buffer::from(self.content.slice(content_range));
        if let Some(size) = failed_read {
            buf = buf.slice(..size.min(buf.len()));
        }
        Ok((
            rp,
            Box::new(MockRangeStream {
                buf,
                fail_after: failed_read.is_some(),
            }),
        ))
    }
}

struct MockRangeStream {
    buf: Buffer,
    fail_after: bool,
}

impl oio::ReadStream for MockRangeStream {
    async fn read(&mut self) -> Result<Buffer> {
        if !self.buf.is_empty() {
            return Ok(std::mem::take(&mut self.buf));
        }
        if self.fail_after {
            return Err(
                Error::new(ErrorKind::RateLimited, "temporary failure while reading")
                    .set_temporary(),
            );
        }
        Ok(Buffer::new())
    }
}

#[derive(Default, Clone)]
struct RetryRecorder {
    events: Arc<Mutex<Vec<(Operation, u32)>>>,
}

impl RetryRecorder {
    fn events(&self) -> Vec<(Operation, u32)> {
        self.events.lock().unwrap().clone()
    }
}

impl RetryInterceptor for RetryRecorder {
    fn intercept(&self, event: RetryEvent<'_>) {
        self.events.lock().unwrap().push((event.op, event.attempt));
    }
}

#[derive(Debug, Clone, Default)]
struct RecordingBudget {
    reservations: Arc<AtomicUsize>,
    deposits: Arc<AtomicUsize>,
}

impl RecordingBudget {
    fn reservations(&self) -> usize {
        self.reservations.load(Ordering::Relaxed)
    }

    fn deposits(&self) -> usize {
        self.deposits.load(Ordering::Relaxed)
    }
}

impl RetryBudget for RecordingBudget {
    fn deposit(&self, _: Operation) {
        self.deposits.fetch_add(1, Ordering::Relaxed);
    }

    fn try_reserve(&self, _: Operation) -> bool {
        self.reservations.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn release(&self, _: Operation) {}
}

fn retry_reader_with_notify<I: RetryInterceptor>(
    inner: MockRangeReader,
    notify: I,
) -> RetryReader<oio::StreamReader<MockRangeReader>, I> {
    RetryReader::new(
        oio::StreamReader::new(inner),
        RetryLayer::new()
            .with_min_delay(Duration::ZERO)
            .with_max_delay(Duration::ZERO)
            .with_notify(notify)
            .policy,
    )
}

fn retry_reader(
    inner: MockRangeReader,
) -> RetryReader<oio::StreamReader<MockRangeReader>, DefaultRetryInterceptor> {
    retry_reader_with_notify(inner, DefaultRetryInterceptor)
}

#[test]
fn resolve_range_clamps_to_object_size() {
    let file = RpRead::new(MetadataBuilder::file(5).build());
    assert_eq!(
        resolve_range(BytesRange::default(), &file),
        BytesRange::new(0, Some(5))
    );
    assert_eq!(
        resolve_range(BytesRange::from(0..10), &file),
        BytesRange::new(0, Some(5))
    );
    assert_eq!(
        resolve_range(BytesRange::new(5, None), &file),
        BytesRange::new(5, Some(0))
    );
    assert_eq!(
        resolve_range(BytesRange::from(8..12), &file),
        BytesRange::new(8, Some(0))
    );
    assert_eq!(
        resolve_range(BytesRange::new(3, Some(0)), &file),
        BytesRange::new(3, Some(0))
    );
    assert_eq!(
        resolve_range(BytesRange::suffix(10), &file),
        BytesRange::new(0, Some(5))
    );
    assert_eq!(
        resolve_range(BytesRange::suffix(2), &file),
        BytesRange::new(3, Some(2))
    );

    let empty = RpRead::new(MetadataBuilder::file(0).build());
    assert_eq!(
        resolve_range(BytesRange::default(), &empty),
        BytesRange::new(0, Some(0))
    );

    let dir = RpRead::new(MetadataBuilder::dir().build());
    assert_eq!(
        resolve_range(BytesRange::default(), &dir),
        BytesRange::default()
    );

    let missing = RpRead::default();
    assert_eq!(
        resolve_range(BytesRange::default(), &missing),
        BytesRange::default()
    );
}

#[tokio::test]
async fn over_eof_range_does_not_reopen_at_eof() -> Result<()> {
    let inner = MockRangeReader::new(b"01234", Some(5), vec![5]);
    let recorder = RetryRecorder::default();
    let reader = retry_reader_with_notify(inner.clone(), recorder.clone());

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::from(0..10)).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"01234"));
    assert_eq!(inner.ranges(), vec![BytesRange::from(0..10)]);
    assert!(recorder.events().is_empty());
    Ok(())
}

#[tokio::test]
async fn unbounded_range_reaches_eof_without_retry() -> Result<()> {
    let inner = MockRangeReader::new(b"01234", Some(5), vec![5]);
    let recorder = RetryRecorder::default();
    let budget = RecordingBudget::default();
    let reader = RetryReader::new(
        oio::StreamReader::new(inner.clone()),
        RetryLayer::new()
            .with_min_delay(Duration::ZERO)
            .with_max_delay(Duration::ZERO)
            .with_notify(recorder.clone())
            .with_budget(budget.clone())
            .policy,
    );

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"01234"));
    assert_eq!(inner.ranges(), vec![BytesRange::default()]);
    assert!(recorder.events().is_empty());
    assert_eq!(budget.reservations(), 0);
    assert_eq!(budget.deposits(), 1);
    Ok(())
}

#[tokio::test]
async fn midstream_error_resumes_without_duplication() -> Result<()> {
    let inner = MockRangeReader::new(b"01234", Some(5), vec![2]);
    let recorder = RetryRecorder::default();
    let reader = retry_reader_with_notify(inner.clone(), recorder.clone());

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::from(0..10)).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"01234"));
    assert_eq!(
        inner.ranges(),
        vec![BytesRange::from(0..10), BytesRange::new(2, Some(3))]
    );
    assert_eq!(recorder.events(), vec![(Operation::Read, 1)]);
    Ok(())
}

#[tokio::test]
async fn suffix_larger_than_object_resumes_without_duplication() -> Result<()> {
    let inner = MockRangeReader::new(b"abcd", Some(4), vec![2]);
    let reader = retry_reader(inner);

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(6)).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"abcd"));
    Ok(())
}

#[tokio::test]
async fn suffix_within_object_resumes_without_duplication() -> Result<()> {
    let inner = MockRangeReader::new(b"0123456789", Some(10), vec![2]);
    let reader = retry_reader(inner);

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(4)).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"6789"));
    Ok(())
}

#[tokio::test]
async fn known_object_size_resolves_retry_to_absolute_range() -> Result<()> {
    let inner = MockRangeReader::new(b"abcd", Some(4), vec![2]);
    let reader = retry_reader(inner.clone());

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(6)).await?;
    let _ = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(
        inner.ranges(),
        vec![BytesRange::suffix(6), BytesRange::new(2, Some(2))]
    );
    Ok(())
}

#[tokio::test]
async fn unknown_object_size_fails_without_retry_after_progress() -> Result<()> {
    let inner = MockRangeReader::new(b"abcd", None, vec![2]);
    let recorder = RetryRecorder::default();
    let reader = retry_reader_with_notify(inner.clone(), recorder.clone());

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(6)).await?;
    let err = oio::ReadStream::read_all(&mut stream)
        .await
        .expect_err("resuming an unresolved suffix range after progress is unsafe");

    assert_eq!(err.kind(), ErrorKind::RateLimited);
    assert!(err.is_persistent());
    assert!(recorder.events().is_empty());
    assert_eq!(inner.ranges(), vec![BytesRange::suffix(6)]);
    Ok(())
}

#[tokio::test]
async fn unknown_object_size_reopens_suffix_before_progress() -> Result<()> {
    let inner = MockRangeReader::new(b"abcd", None, vec![0]);
    let reader = retry_reader(inner.clone());

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(6)).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"abcd"));
    assert_eq!(
        inner.ranges(),
        vec![BytesRange::suffix(6), BytesRange::suffix(6)]
    );
    Ok(())
}

#[tokio::test]
async fn unknown_object_size_suffix_reaches_eof_idempotently() -> Result<()> {
    let inner = MockRangeReader::new(b"abcd", None, vec![4]);
    let reader = retry_reader(inner.clone());

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(4)).await?;
    assert_eq!(
        oio::ReadStream::read(&mut stream).await?.to_bytes(),
        Bytes::from_static(b"abcd")
    );
    assert!(oio::ReadStream::read(&mut stream).await?.is_empty());
    assert!(oio::ReadStream::read(&mut stream).await?.is_empty());
    assert_eq!(inner.ranges(), vec![BytesRange::suffix(4)]);
    Ok(())
}

#[tokio::test]
async fn resolved_suffix_range_resumes_across_multiple_failures() -> Result<()> {
    let inner = MockRangeReader::new(b"0123456789", Some(10), vec![1, 2]);
    let reader = retry_reader(inner.clone());

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(4)).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"6789"));
    assert_eq!(
        inner.ranges(),
        vec![
            BytesRange::suffix(4),
            BytesRange::new(7, Some(3)),
            BytesRange::new(9, Some(1)),
        ]
    );
    Ok(())
}
