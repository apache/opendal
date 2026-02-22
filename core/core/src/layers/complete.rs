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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::raw::oio;
use crate::raw::{
    Access, AccessorInfo, Layer, LayeredAccess, OpCreateDir, OpList, OpPresign, OpRead, OpStat,
    OpWrite, RpCreateDir, RpDelete, RpList, RpPresign, RpRead, RpStat, RpWrite,
};
use crate::*;

/// CompleteLayer keeps validation wrappers for read/write operations.
pub struct CompleteLayer;

impl<A: Access> Layer<A> for CompleteLayer {
    type LayeredAccess = CompleteAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CompleteAccessor {
            info: inner.info(),
            inner: Arc::new(inner),
        }
    }
}

/// Provide complete wrapper for backend.
pub struct CompleteAccessor<A: Access> {
    info: Arc<AccessorInfo>,
    inner: Arc<A>,
}

impl<A: Access> Debug for CompleteAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<A: Access> LayeredAccess for CompleteAccessor<A> {
    type Inner = A;
    type Reader = CompleteReader<A::Reader>;
    type Writer = CompleteWriter<A::Writer>;
    type Lister = CompleteLister<A>;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner().create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let size = args.range().size();
        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, CompleteReader::new(r, size)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let (rp, w) = self.inner.write(path, args.clone()).await?;
        let w = CompleteWriter::new(w, args.append());
        Ok((rp, w))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner().delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let (rp, lister) = self.inner.list(path, args).await?;
        let lister = CompleteLister::new(self.inner.clone(), self.info.clone(), lister);
        Ok((rp, lister))
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }
}

pub struct CompleteLister<A: Access> {
    inner: A::Lister,
    acc: Arc<A>,
    info: Arc<AccessorInfo>,
}

impl<A: Access> CompleteLister<A> {
    fn new(acc: Arc<A>, info: Arc<AccessorInfo>, inner: A::Lister) -> Self {
        Self { inner, acc, info }
    }

    fn should_complete_file_content_length(entry: &oio::Entry) -> bool {
        entry.mode().is_file()
            && !entry.metadata().is_deleted()
            && !entry.metadata().has_content_length()
    }

    async fn ensure_file_content_length(&self, entry: &mut oio::Entry) -> Result<()> {
        let path = entry.path().to_string();
        let version = entry.metadata().version().map(str::to_owned);
        let mut op = OpStat::new();
        if let Some(version) = version.as_deref() {
            op = op.with_version(version);
        }

        let stat_metadata = self.acc.stat(&path, op).await?.into_metadata();
        if !stat_metadata.has_content_length() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "content length is required for list file entries",
            )
            .with_operation("CompleteLister::ensure_file_content_length")
            .with_context("service", self.info.scheme().to_string())
            .with_context("path", path));
        }

        entry
            .metadata_mut()
            .set_content_length(stat_metadata.content_length());
        Ok(())
    }
}

impl<A: Access> oio::List for CompleteLister<A> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let Some(mut entry) = self.inner.next().await? else {
                return Ok(None);
            };

            if !Self::should_complete_file_content_length(&entry) {
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
    size: Option<u64>,
    read: u64,
}

impl<R> CompleteReader<R> {
    pub fn new(inner: R, size: Option<u64>) -> Self {
        Self {
            inner,
            size,
            read: 0,
        }
    }

    pub fn check(&self) -> Result<()> {
        let Some(size) = self.size else {
            return Ok(());
        };

        match self.read.cmp(&size) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(
                Error::new(ErrorKind::Unexpected, "reader got too little data")
                    .with_context("expect", size)
                    .with_context("actual", self.read),
            ),
            Ordering::Greater => Err(
                Error::new(ErrorKind::Unexpected, "reader got too much data")
                    .with_context("expect", size)
                    .with_context("actual", self.read),
            ),
        }
    }
}

impl<R: oio::Read> oio::Read for CompleteReader<R> {
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
