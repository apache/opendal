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
    type Writer = CompleteWriter<oio::Writer>;
    type Lister = CompleteLister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut cap = self.inner.capability();
        cap.read_with_suffix = cap.read;
        cap
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let native_read_with_suffix = self.inner.capability().read_with_suffix;
        let (rp, reader) = self.inner.read(ctx, path, args.clone()).await?;
        let reader = CompleteReader::new(
            ctx.clone(),
            self.inner.clone(),
            path.to_string(),
            args,
            native_read_with_suffix,
            reader,
        );
        Ok((rp, reader))
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let append = args.append();
        let (rp, w) = self.inner.write(ctx, path, args).await?;
        Ok((rp, CompleteWriter::new(w, append)))
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        self.inner.copy(ctx, from, to, args, opts).await
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

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete(ctx).await
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, lister) = self.inner.list(ctx, path, args).await?;
        let lister = CompleteLister::new(ctx.clone(), self.inner.clone(), lister);
        Ok((rp, lister))
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

pub struct CompleteLister {
    inner: oio::Lister,
    ctx: OperationContext,
    srv: Servicer,
}

impl CompleteLister {
    fn new(ctx: OperationContext, srv: Servicer, inner: oio::Lister) -> Self {
        Self { inner, ctx, srv }
    }

    async fn ensure_file_content_length(&self, entry: &mut oio::Entry) -> Result<()> {
        let path = entry.path().to_string();
        let version = entry.metadata().version().map(str::to_owned);
        let mut op = OpStat::new();
        if let Some(version) = version.as_deref() {
            op = op.with_version(version);
        }

        let stat_metadata = self.srv.stat(&self.ctx, &path, op).await?.into_metadata();
        if !stat_metadata.has_content_length() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "content length is required for list file entries",
            )
            .with_operation("CompleteLister::ensure_file_content_length")
            .with_context("service", self.srv.info().scheme().to_string())
            .with_context("path", path));
        }

        entry
            .metadata_mut()
            .set_content_length(stat_metadata.content_length());
        Ok(())
    }
}

impl oio::List for CompleteLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let Some(mut entry) = self.inner.next().await? else {
                return Ok(None);
            };

            if !entry.mode().is_file()
                || entry.metadata().is_deleted()
                || entry.metadata().has_content_length()
            {
                return Ok(Some(entry));
            }

            match self.ensure_file_content_length(&mut entry).await {
                Ok(()) => return Ok(Some(entry)),
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            }
        }
    }
}

pub struct CompleteReader<R> {
    inner: R,
    ctx: OperationContext,
    srv: Servicer,
    path: String,
    args: OpRead,
    native_read_with_suffix: bool,
}

impl<R> CompleteReader<R> {
    pub fn new(
        ctx: OperationContext,
        srv: Servicer,
        path: String,
        args: OpRead,
        native_read_with_suffix: bool,
        inner: R,
    ) -> Self {
        Self {
            inner,
            ctx,
            srv,
            path,
            args,
            native_read_with_suffix,
        }
    }

    async fn resolve_range(&self, range: BytesRange) -> Result<BytesRange> {
        if !range.is_suffix() || self.native_read_with_suffix {
            return Ok(range);
        }

        let BytesRange::Suffix { size } = range else {
            unreachable!("checked by BytesRange::is_suffix")
        };

        let mut op = OpStat::new();
        if let Some(version) = self.args.version() {
            op = op.with_version(version);
        }

        let content_length = self
            .srv
            .stat(&self.ctx, &self.path, op)
            .await?
            .into_metadata()
            .content_length();
        let start = content_length.saturating_sub(size);
        Ok(BytesRange::new(start, Some(content_length - start)))
    }
}

impl<R: oio::Read> oio::Read for CompleteReader<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let range = self.resolve_range(range).await?;
        let size = if range.is_suffix() {
            None
        } else {
            range.size()
        };
        self.inner.open(range).await.map(|(rp, stream)| {
            (
                rp,
                Box::new(CompleteReadStream::new(stream, size)) as Box<dyn oio::ReadStreamDyn>,
            )
        })
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let range = self.resolve_range(range).await?;
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

pub struct CompleteReadStream<R> {
    inner: R,
    size: Option<u64>,
    read: u64,
}

impl<R> CompleteReadStream<R> {
    pub fn new(inner: R, size: Option<u64>) -> Self {
        Self {
            inner,
            size,
            read: 0,
        }
    }

    pub fn check(&self) -> Result<()> {
        check_complete(self.size, self.read)
    }
}

impl<R: oio::ReadStream> oio::ReadStream for CompleteReadStream<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let buf = self.inner.read().await?;

        if buf.is_empty() {
            self.check()?;
        } else {
            self.read += buf.len() as u64;
        }

        Ok(buf)
    }
}

fn check_complete(size: Option<u64>, actual: u64) -> Result<()> {
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

/// Tracks the state of the Write operation.
/// A successful operation goes through states: Open -> Written -> Closed
/// A failed operation terminates in the Error state
#[derive(Debug, PartialEq, Eq)]
enum CompleteState {
    Open,
    Written,
    Closed,
    Error,
}

impl CompleteState {
    /// Attempt to transition to the destination state. Once CompleteState has
    /// errored all further transitions are ignored.
    fn transition(&mut self, destination: CompleteState) {
        if *self != CompleteState::Error {
            *self = destination
        }
    }
}

pub struct CompleteWriter<W> {
    inner: Option<W>,
    append: bool,
    size: u64,
    state: CompleteState,
}

impl<W> CompleteWriter<W> {
    pub fn new(inner: W, append: bool) -> CompleteWriter<W> {
        CompleteWriter {
            inner: Some(inner),
            append,
            size: 0,
            state: CompleteState::Open,
        }
    }

    fn check(&self, content_length: u64) -> Result<()> {
        if self.append || content_length == 0 {
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
}

/// Check if the writer has been closed or aborted while debug_assertions
/// enabled. This code will never be executed in release mode.
#[cfg(debug_assertions)]
impl<W> Drop for CompleteWriter<W> {
    fn drop(&mut self) {
        if self.state == CompleteState::Written {
            log::warn!(
                "writer has not been closed or aborted after successful write operation, must be a bug"
            )
        }
    }
}

impl<W> oio::Write for CompleteWriter<W>
where
    W: oio::Write,
{
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let w = self.inner.as_mut().ok_or_else(|| {
            debug_assert_ne!(
                self.state,
                CompleteState::Open,
                "bug: inner is empty, but state is Open"
            );
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        let len = bs.len();
        w.write(bs)
            .await
            .inspect_err(|_| self.state.transition(CompleteState::Error))?;
        self.size += len as u64;
        self.state.transition(CompleteState::Written);

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let w = self.inner.as_mut().ok_or_else(|| {
            debug_assert_ne!(
                self.state,
                CompleteState::Open,
                "bug: inner is empty, but state is Open"
            );
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        // we must return `Err` before setting inner to None; otherwise,
        // we won't be able to retry `close` in `RetryLayer`.
        let mut ret = w
            .close()
            .await
            .inspect_err(|_| self.state.transition(CompleteState::Error))?;
        self.check(ret.content_length())
            .inspect_err(|_| self.state.transition(CompleteState::Error))?;
        if ret.content_length() == 0 {
            ret = ret.with_content_length(self.size);
        }
        self.inner = None;
        self.state.transition(CompleteState::Closed);

        Ok(ret)
    }

    async fn abort(&mut self) -> Result<()> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        w.abort()
            .await
            .inspect_err(|_| self.state.transition(CompleteState::Error))?;
        self.inner = None;
        self.state.transition(CompleteState::Closed);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockReadReader {
        buffer: Buffer,
    }

    impl oio::Read for MockReadReader {
        async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            Err(Error::new(ErrorKind::Unsupported, "open is not supported"))
        }

        async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
            Ok((RpRead::default(), self.buffer.clone()))
        }
    }

    fn new_test_reader(buffer: impl Into<Buffer>) -> CompleteReader<MockReadReader> {
        let ctx = OperationContext::new(HttpClient::default(), Executor::default());
        let srv = Arc::new(()) as Servicer;
        CompleteReader::new(
            ctx,
            srv,
            "test".to_string(),
            OpRead::new(),
            true,
            MockReadReader {
                buffer: buffer.into(),
            },
        )
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
}
