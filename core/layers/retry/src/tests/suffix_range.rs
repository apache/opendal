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

use bytes::Bytes;

use super::*;

#[derive(Debug, Clone, Copy)]
enum FirstRead {
    /// Fail before returning any bytes.
    Error,
    /// Return this many bytes, then fail.
    Bytes(usize),
}

#[derive(Debug, Clone)]
struct MockSuffixReader {
    content: Bytes,
    content_length: Option<u64>,
    ranges: Arc<Mutex<Vec<BytesRange>>>,
    first_read: FirstRead,
}

impl MockSuffixReader {
    fn new(content: &'static [u8], content_length: Option<u64>, first_read: FirstRead) -> Self {
        Self {
            content: Bytes::from_static(content),
            content_length,
            ranges: Arc::new(Mutex::new(Vec::new())),
            first_read,
        }
    }

    fn ranges(&self) -> Vec<BytesRange> {
        self.ranges.lock().unwrap().clone()
    }
}

impl oio::StreamRead for MockSuffixReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let first = {
            let mut ranges = self.ranges.lock().unwrap();
            ranges.push(range);
            ranges.len() == 1
        };
        let content_range = range.to_content_range(self.content.len())?;
        let rp = match self.content_length {
            Some(content_length) => RpRead::new(MetadataBuilder::file(content_length).build()),
            None => RpRead::default(),
        };
        let mut buf = Buffer::from(self.content.slice(content_range));
        if first {
            match self.first_read {
                FirstRead::Error => buf = Buffer::new(),
                FirstRead::Bytes(size) => buf = buf.slice(..size.min(buf.len())),
            }
        }
        Ok((
            rp,
            Box::new(MockSuffixStream {
                buf,
                fail_after: first,
            }),
        ))
    }
}

struct MockSuffixStream {
    buf: Buffer,
    fail_after: bool,
}

impl oio::ReadStream for MockSuffixStream {
    async fn read(&mut self) -> Result<Buffer> {
        if !self.buf.is_empty() {
            return Ok(std::mem::take(&mut self.buf));
        }
        if self.fail_after {
            return Err(
                Error::new(ErrorKind::Unexpected, "temporary failure while reading")
                    .set_temporary(),
            );
        }
        Ok(Buffer::new())
    }
}

fn retry_reader(
    inner: MockSuffixReader,
) -> RetryReader<oio::StreamReader<MockSuffixReader>, DefaultRetryInterceptor> {
    RetryReader::new(
        oio::StreamReader::new(inner),
        RetryLayer::new()
            .with_min_delay(Duration::ZERO)
            .with_max_delay(Duration::ZERO)
            .policy,
    )
}

#[tokio::test]
async fn suffix_larger_than_object_resumes_without_duplication() -> Result<()> {
    let inner = MockSuffixReader::new(b"abcd", Some(4), FirstRead::Bytes(2));
    let reader = retry_reader(inner);

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(6)).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"abcd"));
    Ok(())
}

#[tokio::test]
async fn suffix_within_object_resumes_without_duplication() -> Result<()> {
    let inner = MockSuffixReader::new(b"0123456789", Some(10), FirstRead::Bytes(2));
    let reader = retry_reader(inner);

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(4)).await?;
    let buf = oio::ReadStream::read_all(&mut stream).await?;

    assert_eq!(buf.to_bytes(), Bytes::from_static(b"6789"));
    Ok(())
}

#[tokio::test]
async fn known_object_size_resolves_retry_to_absolute_range() -> Result<()> {
    let inner = MockSuffixReader::new(b"abcd", Some(4), FirstRead::Bytes(2));
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
async fn unknown_object_size_does_not_resume_suffix_after_progress() -> Result<()> {
    let inner = MockSuffixReader::new(b"abcd", None, FirstRead::Bytes(2));
    let reader = retry_reader(inner.clone());

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::suffix(6)).await?;
    let err = oio::ReadStream::read_all(&mut stream)
        .await
        .expect_err("resuming an unresolved suffix range after progress is unsafe");

    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(err.is_persistent());
    assert_eq!(inner.ranges(), vec![BytesRange::suffix(6)]);
    Ok(())
}

#[tokio::test]
async fn unknown_object_size_reopens_suffix_before_progress() -> Result<()> {
    let inner = MockSuffixReader::new(b"abcd", None, FirstRead::Error);
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
