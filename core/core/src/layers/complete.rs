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

use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::raw::oio;
use crate::raw::*;
use crate::*;

/// CompleteLayer keeps validation wrappers for read/write operations.
pub struct CompleteLayer;

impl std::fmt::Debug for CompleteLayer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteLayer").finish()
    }
}

impl Layer for CompleteLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(self.layer(inner))
    }
}

impl CompleteLayer {
    fn layer(&self, inner: Servicer) -> CompleteService {
        CompleteService { inner }
    }
}

/// Provide complete wrapper for backend.
pub struct CompleteService {
    inner: Servicer,
}

impl std::fmt::Debug for CompleteService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl Service for CompleteService {
    type Reader = CompleteReader<oio::Reader>;
    type Writer = CompleteWriter;
    type Lister = CompleteLister;
    type Deleter = oio::Deleter;
    type Copier = CompleteCopier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let reader = self.inner.read(ctx, path, args)?;
        Ok(CompleteReader::new(reader))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let append = args.append();
        let w = self.inner.write(ctx, path, args)?;
        Ok(CompleteWriter::new(w, append))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let source_content_length_hint = args.source_content_length_hint();
        let copier = self.inner.copy(ctx, from, to, args)?;
        Ok(CompleteCopier::new(copier, source_content_length_hint))
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let metadata = self.inner.stat(ctx, path, args).await?.into_metadata();
        Ok(RpStat::new(metadata))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let lister = self.inner.list(ctx, path, args)?;
        Ok(CompleteLister::new(ctx.clone(), self.inner.clone(), lister))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

pub struct CompleteCopier {
    inner: oio::Copier,
    source_content_length_hint: Option<u64>,
    copied: Option<u64>,
}

impl CompleteCopier {
    fn new(inner: oio::Copier, source_content_length_hint: Option<u64>) -> Self {
        Self {
            inner,
            source_content_length_hint,
            copied: None,
        }
    }

    fn complete_metadata(&self, metadata: Metadata) -> Result<Metadata> {
        if metadata.is_file() {
            if let Some(copied) = self.copied
                && metadata.content_length() != copied
            {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "copy result content length does not match copied bytes",
                )
                .with_operation("CompleteCopier::close")
                .with_context("expected", copied)
                .with_context("actual", metadata.content_length()));
            }

            return Ok(metadata);
        }

        let Some(copied) = self.copied.or(self.source_content_length_hint) else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "copy result does not contain enough information to determine content length",
            )
            .with_operation("CompleteCopier::close"));
        };
        let mut builder = metadata.into_builder();
        builder.set_file(copied);
        Ok(builder.build())
    }
}

impl oio::Copy for CompleteCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        let progress = self.inner.next().await?;
        if let Some(progress) = progress {
            let copied = self.copied.unwrap_or_default();
            self.copied = Some(copied.checked_add(progress as u64).ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "copied byte count overflowed u64")
                    .with_operation("CompleteCopier::next")
            })?);
        }
        Ok(progress)
    }

    async fn close(&mut self) -> Result<Metadata> {
        let metadata = self.inner.close().await?;
        self.complete_metadata(metadata)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

pub struct CompleteLister {
    inner: oio::Lister,
    ctx: OperationContext,
    srv: Servicer,
}

impl CompleteLister {
    fn new(ctx: OperationContext, srv: Servicer, inner: oio::Lister) -> Self {
        Self { inner, ctx, srv }
    }

    async fn complete_unknown_entry_mode(&self, entry: oio::Entry) -> Result<oio::Entry> {
        let path = entry.path().to_string();
        let version = entry.metadata().version().map(str::to_owned);
        let op = options::StatOptions {
            version,
            ..Default::default()
        }
        .into();

        let stat_metadata = match self.srv.stat(&self.ctx, &path, op).await {
            Ok(rp) => rp.into_metadata(),
            // The entry comes from the service's own listing, so a stat that
            // cannot see its target — a dangling symlink in fs, whose stat
            // follows the link — cannot refine the mode, but it does not mean
            // the entry is gone. Keep the entry as listed.
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(entry),
            Err(err) => return Err(err),
        };
        if stat_metadata.mode() == EntryMode::Unknown {
            return Ok(entry);
        }

        let (mut path, metadata) = entry.into_parts();
        let mut builder = metadata.into_builder();
        if stat_metadata.is_file() {
            builder.set_file(stat_metadata.content_length());
        } else {
            builder.set_dir();
            if !path.ends_with('/') {
                path.push('/');
            }
        }
        Ok(oio::Entry::with(path, builder.build()))
    }
}

impl oio::List for CompleteLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let Some(entry) = self.inner.next().await? else {
            return Ok(None);
        };

        if entry.mode() != EntryMode::Unknown || entry.metadata().is_deleted() {
            return Ok(Some(entry));
        }

        Ok(Some(self.complete_unknown_entry_mode(entry).await?))
    }
}

pub struct CompleteReader<R> {
    inner: R,
}

impl<R> CompleteReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: oio::Read> oio::Read for CompleteReader<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (rp, stream) = self.inner.open(range).await?;
        let require_start = !range.is_suffix() && range.size().is_some_and(|size| size > 0);
        let available = rp
            .metadata()
            .filter(|metadata| metadata.is_file())
            .map(|metadata| {
                let length = metadata.content_length();
                match range {
                    BytesRange::Range { offset, size } => {
                        let remaining = length.saturating_sub(offset);
                        size.map_or(remaining, |size| size.min(remaining))
                    }
                    BytesRange::Suffix { size } => size.min(length),
                }
            });
        if require_start && available == Some(0) {
            return Err(Error::new(
                ErrorKind::RangeNotSatisfied,
                "range starts at or beyond the end of the object",
            )
            .with_context("offset", range.offset()));
        }
        let stream = CompleteReadStream {
            inner: stream,
            max_size: available.or(range.size()),
            expected_size: available,
            require_start: require_start && available.is_none(),
            read: 0,
        };
        Ok((rp, Box::new(stream)))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let size = if range.is_suffix() {
            None
        } else {
            range.size()
        };
        let (rp, buffer) = self.inner.read(range).await?;
        check_complete(size, buffer.len() as u64)?;
        Ok((rp, buffer))
    }
}

struct CompleteReadStream<R> {
    inner: R,
    max_size: Option<u64>,
    expected_size: Option<u64>,
    require_start: bool,
    read: u64,
}

impl<R: oio::ReadStream> oio::ReadStream for CompleteReadStream<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let buf = self.inner.read().await?;
        if buf.is_empty() {
            if self.require_start && self.read == 0 {
                return Err(Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "range starts at or beyond the end of the object",
                ));
            }
            check_complete(self.expected_size, self.read)?;
        } else {
            self.read += buf.len() as u64;
            if self.max_size.is_some_and(|size| self.read > size) {
                check_complete(self.max_size, self.read)?;
            }
        }
        Ok(buf)
    }
}

pub(crate) fn check_complete(size: Option<u64>, actual: u64) -> Result<()> {
    let Some(size) = size else {
        return Ok(());
    };

    match actual.cmp(&size) {
        Ordering::Equal => Ok(()),
        Ordering::Less => Err(
            Error::new(ErrorKind::Unexpected, "reader got too little data")
                .with_context("expect", size)
                .with_context("actual", actual),
        ),
        Ordering::Greater => Err(
            Error::new(ErrorKind::Unexpected, "reader got too much data")
                .with_context("expect", size)
                .with_context("actual", actual),
        ),
    }
}

/// State of a [`CompleteWriter`].
///
/// The writer lives in the state because each phase decides what to do with it:
/// `Open` and `Written` keep it for the next write, `Failed` keeps it so the
/// caller can still abort, and `Closed` holds none.
///
/// A failure stays recorded until the writer is closed or aborted, so a later
/// successful operation does not clear it.
enum State {
    /// The writer has not accepted data yet.
    Open(oio::Writer),
    /// The writer accepted data and must be closed or aborted.
    Written(oio::Writer),
    /// The writer is closed or aborted and accepts no more data.
    Closed,
    /// A write, copy, or close failed. The writer is kept so the caller can
    /// still abort it.
    Failed(oio::Writer),
}

impl State {
    /// Record that the writer accepted data.
    fn written(self) -> State {
        match self {
            State::Open(writer) | State::Written(writer) => State::Written(writer),
            State::Failed(writer) => State::Failed(writer),
            State::Closed => State::Closed,
        }
    }

    /// Record that a write, copy, or close failed.
    fn failed(self) -> State {
        match self {
            State::Open(writer) | State::Written(writer) | State::Failed(writer) => {
                State::Failed(writer)
            }
            State::Closed => State::Closed,
        }
    }
}

pub struct CompleteWriter {
    append: bool,
    size: u64,
    state: State,
}

impl CompleteWriter {
    pub fn new(inner: oio::Writer, append: bool) -> CompleteWriter {
        CompleteWriter {
            append,
            size: 0,
            state: State::Open(inner),
        }
    }

    fn check(&self, content_length: u64) -> Result<()> {
        if self.append {
            return Ok(());
        }

        match self.size.cmp(&content_length) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(
                Error::new(ErrorKind::Unexpected, "writer got too little data")
                    .with_context("expect", content_length)
                    .with_context("actual", self.size),
            ),
            Ordering::Greater => Err(
                Error::new(ErrorKind::Unexpected, "writer got too much data")
                    .with_context("expect", content_length)
                    .with_context("actual", self.size),
            ),
        }
    }

    /// Validate the metadata returned by the inner writer on close.
    ///
    /// An append cannot verify the written size, so it requires the service to
    /// report the final content length. A service that reports unknown metadata
    /// gets the size written through this writer promoted onto it.
    fn check_close(&self, metadata: Metadata) -> Result<Metadata> {
        if metadata.is_file() {
            self.check(metadata.content_length())?;
            return Ok(metadata);
        }
        if self.append {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "append result does not contain the final content length",
            )
            .with_operation("CompleteWriter::close"));
        }

        let mut builder = metadata.into_builder();
        builder.set_file(self.size);
        Ok(builder.build())
    }

    /// Move the state out so an operation can record its outcome on it.
    ///
    /// The writer moves to the next state through this call. `State::Closed`
    /// fills the slot during the move, and no await point is crossed here.
    fn take_state(&mut self) -> State {
        std::mem::replace(&mut self.state, State::Closed)
    }
}

/// Check if the writer has been closed or aborted while debug_assertions
/// enabled. This code will never be executed in release mode.
#[cfg(debug_assertions)]
impl Drop for CompleteWriter {
    fn drop(&mut self) {
        if matches!(self.state, State::Written(_)) {
            log::warn!(
                "writer has not been closed or aborted after successful write operation, must be a bug"
            )
        }
    }
}

impl oio::Write for CompleteWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let len = bs.len() as u64;

        // The writer stays in the state while the write runs, so a canceled
        // write never loses it.
        let result = match &mut self.state {
            State::Open(writer) | State::Written(writer) | State::Failed(writer) => {
                writer.write(bs).await
            }
            State::Closed => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "writer has been closed or aborted",
                ));
            }
        };

        let state = self.take_state();
        self.state = if result.is_ok() {
            state.written()
        } else {
            state.failed()
        };
        result?;
        self.size += len;

        Ok(())
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        // The writer stays in the state while the copy runs, so a canceled copy
        // never loses it.
        let writer = match &mut self.state {
            State::Open(writer) | State::Written(writer) | State::Failed(writer) => writer,
            State::Closed => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "writer has been closed or aborted",
                ));
            }
        };
        let size = range.size().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "native writer copy requires a bounded range",
            )
        })?;
        let result = writer.copy_from(path, args, range).await;

        let state = self.take_state();
        self.state = if result.is_ok() {
            state.written()
        } else {
            state.failed()
        };
        result?;
        self.size += size;

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        // The writer stays in the state while closing, so a failed close can be
        // retried.
        let result = match &mut self.state {
            State::Open(writer) | State::Written(writer) | State::Failed(writer) => {
                writer.close().await
            }
            State::Closed => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "writer has been closed or aborted",
                ));
            }
        };
        let result = result.and_then(|metadata| self.check_close(metadata));

        // Keep the writer when closing failed: `RetryLayer` closes it again.
        let state = self.take_state();
        self.state = if result.is_ok() {
            State::Closed
        } else {
            state.failed()
        };
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let result = match &mut self.state {
            State::Open(writer) | State::Written(writer) | State::Failed(writer) => {
                writer.abort().await
            }
            State::Closed => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "writer has been closed or aborted",
                ));
            }
        };

        // Keep the writer when aborting failed, so `abort` can be retried.
        let state = self.take_state();
        self.state = if result.is_ok() {
            State::Closed
        } else {
            state.failed()
        };
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::raw::oio::Copy as _;
    use crate::raw::oio::Write as _;

    struct MockCopier {
        progress: VecDeque<usize>,
        metadata: Metadata,
    }

    impl MockCopier {
        fn new(progress: impl IntoIterator<Item = usize>, metadata: Metadata) -> Self {
            Self {
                progress: progress.into_iter().collect(),
                metadata,
            }
        }
    }

    impl oio::Copy for MockCopier {
        async fn next(&mut self) -> Result<Option<usize>> {
            Ok(self.progress.pop_front())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(self.metadata.clone())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn incomplete_copy_metadata() -> Metadata {
        MetadataBuilder::unknown().build()
    }

    #[tokio::test]
    async fn test_copy_uses_source_content_length_hint() -> Result<()> {
        let inner = Box::new(MockCopier::new([], incomplete_copy_metadata()));
        let mut copier = CompleteCopier::new(inner, Some(8));

        let metadata = copier.close().await?;

        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 8);
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_uses_reported_progress() -> Result<()> {
        let inner = Box::new(MockCopier::new([3, 5], incomplete_copy_metadata()));
        let mut copier = CompleteCopier::new(inner, Some(7));

        while copier.next().await?.is_some() {}
        let metadata = copier.close().await?;

        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 8);
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_uses_result_metadata_over_source_hint() -> Result<()> {
        let metadata = MetadataBuilder::file(8);
        let inner = Box::new(MockCopier::new([], metadata.build()));
        let mut copier = CompleteCopier::new(inner, Some(7));

        let metadata = copier.close().await?;

        assert_eq!(metadata.content_length(), 8);
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_requires_authoritative_content_length() {
        let inner = Box::new(MockCopier::new([], incomplete_copy_metadata()));
        let mut copier = CompleteCopier::new(inner, None);

        let err = copier
            .close()
            .await
            .expect_err("copy without an authoritative content length must fail");

        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    struct UnsupportedCopyWriter;

    impl oio::Write for UnsupportedCopyWriter {
        async fn write(&mut self, _: Buffer) -> Result<()> {
            Ok(())
        }

        async fn copy_from(&mut self, _: &str, _: OpRead, _: BytesRange) -> Result<()> {
            Err(Error::new(ErrorKind::Unsupported, "copy is unsupported"))
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct MockWriter {
        metadata: Metadata,
        /// Number of `write` calls that fail before one succeeds.
        write_failures: usize,
        /// Number of `close` calls that fail before one succeeds.
        close_failures: usize,
    }

    impl MockWriter {
        fn new(metadata: Metadata) -> Self {
            Self {
                metadata,
                write_failures: 0,
                close_failures: 0,
            }
        }
    }

    impl oio::Write for MockWriter {
        async fn write(&mut self, _: Buffer) -> Result<()> {
            if self.write_failures > 0 {
                self.write_failures -= 1;
                return Err(Error::new(ErrorKind::Unexpected, "mock write failed"));
            }
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            if self.close_failures > 0 {
                self.close_failures -= 1;
                return Err(Error::new(ErrorKind::Unexpected, "mock close failed"));
            }
            Ok(self.metadata.clone())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct TestReadStream {
        buffer: Buffer,
        object_length: Option<u64>,
    }

    impl oio::StreamRead for TestReadStream {
        async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            let rp = self.object_length.map_or_else(RpRead::default, |length| {
                RpRead::new(MetadataBuilder::file(length).build())
            });
            Ok((rp, Box::new(self.buffer.clone())))
        }
    }

    #[tokio::test]
    async fn test_open_validates_available_bytes() -> Result<()> {
        let range = BytesRange::from(2..6);
        for length in [None, Some(4)] {
            let reader = CompleteReader::new(oio::StreamReader::new(TestReadStream {
                buffer: Buffer::from("ab"),
                object_length: length,
            }));
            let (_, mut stream) = oio::Read::open(&reader, range).await?;
            assert_eq!(stream.read_all_dyn().await?.to_bytes(), "ab");
        }

        for (length, content, message) in [
            (Some(5), "ab", "reader got too little data"),
            (Some(3), "ab", "reader got too much data"),
            (None, "abcde", "reader got too much data"),
        ] {
            let reader = CompleteReader::new(oio::StreamReader::new(TestReadStream {
                buffer: Buffer::from(content),
                object_length: length,
            }));
            let (_, mut stream) = oio::Read::open(&reader, range).await?;
            let err = stream.read_all_dyn().await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), message);
        }
        Ok(())
    }

    struct MockReadReader {
        buffer: Buffer,
    }

    impl oio::Read for MockReadReader {
        async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            Err(Error::new(ErrorKind::Unsupported, "open is not supported"))
        }

        async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
            Ok((
                RpRead::new(MetadataBuilder::file(self.buffer.len() as u64).build()),
                self.buffer.clone(),
            ))
        }
    }

    fn new_test_reader(buffer: impl Into<Buffer>) -> CompleteReader<MockReadReader> {
        CompleteReader::new(MockReadReader {
            buffer: buffer.into(),
        })
    }

    #[tokio::test]
    async fn test_read_rejects_short_buffer() {
        let reader = new_test_reader("a");

        let err = oio::Read::read(&reader, BytesRange::from(0_u64..2))
            .await
            .expect_err("read should reject short buffer");

        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_read_rejects_extra_buffer() {
        let reader = new_test_reader("ab");

        let err = oio::Read::read(&reader, BytesRange::from(0_u64..1))
            .await
            .expect_err("read should reject extra buffer");

        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_writer_copy_from_error_is_terminal() {
        let mut writer = CompleteWriter::new(Box::new(UnsupportedCopyWriter), false);

        let err = writer
            .copy_from("source", OpRead::new(), BytesRange::new(0, Some(8)))
            .await
            .expect_err("copy_from should fail");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert!(matches!(writer.state, State::Failed(_)));

        writer.abort().await.unwrap();
        assert!(matches!(writer.state, State::Closed));
    }

    #[tokio::test]
    async fn test_writer_close_is_terminal() -> Result<()> {
        let mut writer = CompleteWriter::new(
            Box::new(MockWriter::new(MetadataBuilder::unknown().build())),
            false,
        );
        writer.write(Buffer::from("abc")).await?;
        writer.close().await?;
        assert!(matches!(writer.state, State::Closed));

        // A closed writer accepts no more writes and can't be closed or
        // aborted again.
        assert!(writer.write(Buffer::from("a")).await.is_err());
        assert!(writer.close().await.is_err());
        assert!(writer.abort().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_failed_close_keeps_writer_for_retry() -> Result<()> {
        let mut inner = MockWriter::new(MetadataBuilder::unknown().build());
        inner.close_failures = 1;
        let mut writer = CompleteWriter::new(Box::new(inner), false);
        writer.write(Buffer::from("abc")).await?;

        let err = writer.close().await.expect_err("first close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(matches!(writer.state, State::Failed(_)));

        // The failed close keeps the writer, so a retry can still close it.
        let metadata = writer.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 3);
        assert!(matches!(writer.state, State::Closed));
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_records_failure_until_closed_or_aborted() -> Result<()> {
        let mut inner = MockWriter::new(MetadataBuilder::unknown().build());
        inner.write_failures = 1;
        let mut writer = CompleteWriter::new(Box::new(inner), false);

        assert!(writer.write(Buffer::from("abc")).await.is_err());
        assert!(matches!(writer.state, State::Failed(_)));

        // A later successful write does not clear the recorded failure.
        writer.write(Buffer::from("abc")).await?;
        assert!(matches!(writer.state, State::Failed(_)));

        writer.abort().await?;
        assert!(matches!(writer.state, State::Closed));
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_promotes_unknown_metadata_with_written_size() -> Result<()> {
        let inner = MockWriter::new(MetadataBuilder::unknown().build());
        let mut writer = CompleteWriter::new(Box::new(inner), false);
        writer.write(Buffer::from("abc")).await?;

        let metadata = writer.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_preserves_explicit_empty_file_length() -> Result<()> {
        let inner = MockWriter::new(MetadataBuilder::file(0).build());
        let mut writer = CompleteWriter::new(Box::new(inner), false);

        let metadata = writer.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_rejects_explicit_zero_after_writing() {
        let inner = MockWriter::new(MetadataBuilder::file(0).build());
        let mut writer = CompleteWriter::new(Box::new(inner), false);
        writer.write(Buffer::from("a")).await.unwrap();

        let err = writer
            .close()
            .await
            .expect_err("an explicit empty-file result must not mean missing length");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_append_requires_final_content_length() {
        let inner = MockWriter::new(MetadataBuilder::unknown().build());
        let mut writer = CompleteWriter::new(Box::new(inner), true);
        writer.write(Buffer::from("a")).await.unwrap();

        let err = writer
            .close()
            .await
            .expect_err("append without the final content length must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }
}
