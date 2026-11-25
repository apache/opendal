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

use std::future::Future;

use crate::raw::*;
use crate::*;

/// Defines the storage operations that [`AppendWriter`] needs to append to an object.
///
/// Services using HTTP or any other protocol can implement this trait by
/// retrieving the current end of the object and appending a [`Buffer`] there.
pub trait AppendWrite: Send + Sync + Unpin + 'static {
    /// Return the current end of the object as a byte offset.
    ///
    /// Return `0` if the object does not exist.
    fn offset(&self) -> impl Future<Output = Result<u64>> + MaybeSend;

    /// Append `size` bytes from `body` to the end of the object and return its metadata.
    ///
    /// `offset` is the expected byte offset at the end of the object.
    ///
    /// [`AppendWriter`] also calls this method with `size` `0` and an empty
    /// `body` when a write closes without data, so implementations must create
    /// the object and return its metadata for an empty append.
    fn append(
        &self,
        offset: u64,
        size: u64,
        body: Buffer,
    ) -> impl Future<Output = Result<Metadata>> + MaybeSend;
}

/// Implements [`oio::Write`] for a service that implements [`AppendWrite`].
///
/// On the first write, the writer calls [`AppendWrite::offset`] to find the end
/// of the object. It calls [`AppendWrite::append`] with the current offset, the
/// buffer's byte length, and the buffer. The writer advances the offset after
/// each successful append.
///
/// [`oio::Write::close`] appends an empty buffer when no append has succeeded
/// yet, so closing a writer without data still creates the object and reports
/// the metadata the service actually returns.
///
/// A service can use this writer as its [`Service::Writer`] or wrap it with
/// other writers to support multiple write modes.
pub struct AppendWriter<W: AppendWrite> {
    inner: W,

    offset: Option<u64>,

    meta: Metadata,

    /// Whether an append has succeeded, so `meta` describes the object.
    written: bool,
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this state.
impl<W: AppendWrite> AppendWriter<W> {
    /// Create a new AppendWriter.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            offset: None,
            meta: MetadataBuilder::unknown().build(),
            written: false,
        }
    }

    /// Return the current end of the object, caching it for later calls.
    async fn offset(&mut self) -> Result<u64> {
        match self.offset {
            Some(offset) => Ok(offset),
            None => {
                let offset = self.inner.offset().await?;
                self.offset = Some(offset);
                Ok(offset)
            }
        }
    }

    /// Append `bs` to the end of the object and record the returned metadata.
    ///
    /// The offset only advances after the service accepted the append, so a
    /// failed attempt is retried at the same offset.
    async fn append(&mut self, bs: Buffer) -> Result<()> {
        let offset = self.offset().await?;
        let size = bs.len() as u64;

        self.meta = self.inner.append(offset, size, bs).await?;
        self.offset = Some(offset + size);
        self.written = true;
        Ok(())
    }
}

impl<W> oio::Write for AppendWriter<W>
where
    W: AppendWrite,
{
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.append(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        // No append reached the service, so the object may not exist yet and
        // `meta` describes nothing. Append zero bytes to create the object or
        // to read the metadata of the existing one.
        if !self.written {
            self.append(Buffer::new()).await?;
        }

        let mut builder = self.meta.clone().into_builder();
        builder.set_file(self.offset.unwrap_or_default());
        Ok(builder.build())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::MutexGuard;

    use pretty_assertions::assert_eq;

    use super::*;
    use crate::raw::oio::Write;

    /// Simulates an appendable object and records every call into it.
    #[derive(Clone)]
    struct MockAppendWrite {
        state: Arc<Mutex<MockState>>,
    }

    struct MockState {
        /// Current object length in bytes.
        length: u64,
        offset_calls: usize,
        appends: Vec<(u64, u64)>,
        /// How many upcoming `append` calls fail with a temporary error.
        fail_appends: usize,
    }

    impl MockAppendWrite {
        fn new(length: u64) -> Self {
            Self {
                state: Arc::new(Mutex::new(MockState {
                    length,
                    offset_calls: 0,
                    appends: Vec::new(),
                    fail_appends: 0,
                })),
            }
        }

        fn state(&self) -> MutexGuard<'_, MockState> {
            self.state.lock().expect("mock state must not be poisoned")
        }

        fn offset_calls(&self) -> usize {
            self.state().offset_calls
        }

        fn appends(&self) -> Vec<(u64, u64)> {
            self.state().appends.clone()
        }
    }

    impl AppendWrite for MockAppendWrite {
        async fn offset(&self) -> Result<u64> {
            let mut state = self.state();
            state.offset_calls += 1;
            Ok(state.length)
        }

        async fn append(&self, offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
            let mut state = self.state();
            state.appends.push((offset, size));

            if state.fail_appends > 0 {
                state.fail_appends -= 1;
                return Err(
                    Error::new(ErrorKind::Unexpected, "mock append failure").set_temporary()
                );
            }

            assert_eq!(offset, state.length, "append must target the current end");
            assert_eq!(size, body.len() as u64, "append size must match the buffer");

            state.length += size;

            let mut meta = MetadataBuilder::unknown();
            meta.set_file(state.length);
            meta.etag(format!("etag-{}", state.length));
            meta.last_modified(Timestamp::new(1_700_000_000, 0)?);
            Ok(meta.build())
        }
    }

    #[tokio::test]
    async fn close_creates_missing_object_with_empty_append() {
        let mock = MockAppendWrite::new(0);
        let mut writer = AppendWriter::new(mock.clone());

        let meta = writer.close().await.expect("close must create the object");

        assert_eq!(mock.offset_calls(), 1);
        assert_eq!(mock.appends(), vec![(0, 0)]);
        assert_eq!(meta.mode(), EntryMode::FILE);
        assert_eq!(meta.content_length(), 0);
        assert_eq!(meta.etag(), Some("etag-0"));

        // Closing an already closed writer must not append again.
        let meta = writer.close().await.expect("close must stay idempotent");

        assert_eq!(mock.offset_calls(), 1);
        assert_eq!(mock.appends(), vec![(0, 0)]);
        assert_eq!(meta.content_length(), 0);
    }

    #[tokio::test]
    async fn close_keeps_metadata_of_existing_object() {
        let mock = MockAppendWrite::new(10);
        let mut writer = AppendWriter::new(mock.clone());

        let meta = writer
            .close()
            .await
            .expect("close must append to the existing object");

        assert_eq!(mock.offset_calls(), 1);
        assert_eq!(mock.appends(), vec![(10, 0)]);
        assert_eq!(meta.mode(), EntryMode::FILE);
        assert_eq!(meta.content_length(), 10);
        assert_eq!(meta.etag(), Some("etag-10"));
        assert_eq!(
            meta.last_modified(),
            Some(Timestamp::new(1_700_000_000, 0).expect("timestamp must be valid"))
        );
    }

    #[tokio::test]
    async fn close_retries_empty_append_after_temporary_failure() {
        let mock = MockAppendWrite::new(0);
        mock.state().fail_appends = 1;
        let mut writer = AppendWriter::new(mock.clone());

        let err = writer
            .close()
            .await
            .expect_err("the first close must report the append failure");
        assert!(err.is_temporary(), "the failure must stay retryable");

        assert_eq!(mock.offset_calls(), 1);
        assert_eq!(mock.appends(), vec![(0, 0)]);

        let meta = writer
            .close()
            .await
            .expect("the retried close must succeed");

        // The failed append did not move the object end, so the cached offset is reused.
        assert_eq!(mock.offset_calls(), 1);
        assert_eq!(mock.appends(), vec![(0, 0), (0, 0)]);
        assert_eq!(meta.content_length(), 0);
    }

    #[tokio::test]
    async fn close_after_write_does_not_append_again() {
        let mock = MockAppendWrite::new(0);
        let mut writer = AppendWriter::new(mock.clone());

        writer
            .write(Buffer::from("hello"))
            .await
            .expect("write must append the buffer");
        let meta = writer.close().await.expect("close must succeed");

        assert_eq!(mock.offset_calls(), 1);
        assert_eq!(mock.appends(), vec![(0, 5)]);
        assert_eq!(meta.content_length(), 5);
        assert_eq!(meta.etag(), Some("etag-5"));
    }

    #[tokio::test]
    async fn empty_write_creates_object_once() {
        let mock = MockAppendWrite::new(0);
        let mut writer = AppendWriter::new(mock.clone());

        writer
            .write(Buffer::new())
            .await
            .expect("writing an empty buffer must create the object");
        let meta = writer.close().await.expect("close must succeed");

        assert_eq!(mock.offset_calls(), 1);
        assert_eq!(mock.appends(), vec![(0, 0)]);
        assert_eq!(meta.content_length(), 0);
    }
}
