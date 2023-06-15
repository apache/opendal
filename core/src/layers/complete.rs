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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;

use crate::raw::oio::into_reader::RangeReader;
use crate::raw::oio::to_flat_pager;
use crate::raw::oio::to_hierarchy_pager;
use crate::raw::oio::Entry;
use crate::raw::oio::IntoStreamableReader;
use crate::raw::oio::ToFlatPager;
use crate::raw::oio::ToHierarchyPager;
use crate::raw::*;
use crate::*;

/// Complete underlying services features so that users can use them in
/// the same way.
///
/// # Notes
///
/// CompleteLayer is not a public accessible layer that can be used by
/// external users. OpenDAL will make sure every accessor will apply this
/// layer once and only once.
///
/// # Internal
///
/// So far `CompleteLayer` will do the following things:
///
/// ## Read Completion
///
/// OpenDAL requires all reader implements [`oio::Read`] and
/// [`oio::BlockingRead`]. However, not all services have the
/// capabilities. CompleteLayer will add those capabilities in
/// a zero cost way.
///
/// Underlying services will return [`AccessorHint`] to indicate the
/// features that returning readers support.
///
/// - If both `seekable` and `streamable`, return directly.
/// - If not `streamable`, with [`oio::into_streamable_reader`].
/// - If not `seekable`, with [`oio::into_reader::by_range`]
/// - If neither not supported, wrap both by_range and into_streamable.
///
/// All implementations of Reader should be `zero cost`. In our cases,
/// which means others must pay the same cost for the same feature provide
/// by us.
///
/// For examples, call `read` without `seek` should always act the same as
/// calling `read` on plain reader.
///
/// ### Read is Seekable
///
/// We use internal `AccessorHint::ReadSeekable` to decide the most
/// suitable implementations.
///
/// If there is a hint that `ReadSeekable`, we will open it with given args
/// directly. Otherwise, we will pick a seekable reader implementation based
/// on input range for it.
///
/// - `Some(offset), Some(size)` => `RangeReader`
/// - `Some(offset), None` and `None, None` => `OffsetReader`
/// - `None, Some(size)` => get the total size first to convert as `RangeReader`
///
/// No matter which reader we use, we will make sure the `read` operation
/// is zero cost.
///
/// ### Read is Streamable
///
/// We use internal `AccessorHint::ReadStreamable` to decide the most
/// suitable implementations.
///
/// If there is a hint that `ReadStreamable`, we will use existing reader
/// directly. Otherwise, we will use transform this reader as a stream.
///
/// ### Consume instead of Drop
///
/// Normally, if reader is seekable, we need to drop current reader and start
/// a new read call.
///
/// We can consume the data if the seek position is close enough. For
/// example, users try to seek to `Current(1)`, we can just read the data
/// can consume it.
///
/// In this way, we can reduce the extra cost of dropping reader.
///
/// ## List Completion
///
/// There are two styles of list, but not all services support both of
/// them. CompleteLayer will add those capabilities in a zero cost way.
///
/// Underlying services will return [`AccessorHint`] to indicate the
/// features that returning pagers support.
///
/// - If both `flat` and `hierarchy`, return directly.
/// - If only `flat`, with [`oio::to_flat_pager`].
/// - if only `hierarchy`, with [`oio::to_hierarchy_pager`].
/// - If neither not supported, something must be wrong.
///
/// ## Capability Check
///
/// Before performing any operations, `CompleteLayer` will first check
/// the operation against capability of the underlying service. If the
/// operation is not supported, an error will be returned directly.
///
/// [`AccessorHint`]: crate::raw::AccessorHint
pub struct CompleteLayer;

impl<A: Accessor> Layer<A> for CompleteLayer {
    type LayeredAccessor = CompleteReaderAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.info();
        CompleteReaderAccessor {
            meta,
            inner: Arc::new(inner),
        }
    }
}

/// Provide reader wrapper for backend.
pub struct CompleteReaderAccessor<A: Accessor> {
    meta: AccessorInfo,
    inner: Arc<A>,
}

impl<A: Accessor> Debug for CompleteReaderAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<A: Accessor> CompleteReaderAccessor<A> {
    async fn complete_reader(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, CompleteReader<A, A::Reader>)> {
        let capability = self.meta.capability();
        if !capability.read {
            return new_capability_unsupported_error(Operation::Read);
        }

        let seekable = capability.read_can_seek;
        let streamable = capability.read_can_next;

        let range = args.range();
        let (rp, r) = self.inner.read(path, args).await?;
        let content_length = rp.metadata().content_length();

        match (seekable, streamable) {
            (true, true) => Ok((rp, CompleteReader::AlreadyComplete(r))),
            (true, false) => {
                let r = oio::into_streamable_reader(r, 256 * 1024);
                Ok((rp, CompleteReader::NeedStreamable(r)))
            }
            _ => {
                let (offset, size) = match (range.offset(), range.size()) {
                    (Some(offset), _) => (offset, content_length),
                    (None, None) => (0, content_length),
                    (None, Some(size)) => {
                        // TODO: we can read content range to calculate
                        // the total content length.
                        let om = self.inner.stat(path, OpStat::new()).await?.into_metadata();
                        let total_size = om.content_length();
                        let (offset, size) = if size > total_size {
                            (0, total_size)
                        } else {
                            (total_size - size, size)
                        };

                        (offset, size)
                    }
                };
                let r = oio::into_reader::by_range(self.inner.clone(), path, r, offset, size);

                if streamable {
                    Ok((rp, CompleteReader::NeedSeekable(r)))
                } else {
                    let r = oio::into_streamable_reader(r, 256 * 1024);
                    Ok((rp, CompleteReader::NeedBoth(r)))
                }
            }
        }
    }

    fn complete_blocking_reader(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, CompleteReader<A, A::BlockingReader>)> {
        let capability = self.meta.capability();
        if !capability.read || !capability.blocking {
            return new_capability_unsupported_error(Operation::BlockingRead);
        }

        let seekable = capability.read_can_seek;
        let streamable = capability.read_can_next;

        let (rp, r) = self.inner.blocking_read(path, args)?;

        match (seekable, streamable) {
            (true, true) => Ok((rp, CompleteReader::AlreadyComplete(r))),
            (true, false) => {
                let r = oio::into_streamable_reader(r, 256 * 1024);
                Ok((rp, CompleteReader::NeedStreamable(r)))
            }
            (false, _) => Err(Error::new(
                ErrorKind::Unsupported,
                "non seekable blocking reader is not supported",
            )),
        }
    }

    async fn complete_list(
        &self,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, CompletePager<A, A::Pager>)> {
        let cap = self.meta.capability();
        if !cap.list {
            return Err(
                Error::new(ErrorKind::Unsupported, "operation is not supported")
                    .with_context("service", self.meta.scheme())
                    .with_operation("list"),
            );
        }

        let delimiter = args.delimiter();

        if delimiter.is_empty() {
            return if cap.list_without_delimiter {
                let (rp, p) = self.inner.list(path, args).await?;
                Ok((rp, CompletePager::AlreadyComplete(p)))
            } else {
                let p = to_flat_pager(
                    self.inner.clone(),
                    path,
                    args.with_delimiter("/").limit().unwrap_or(1000),
                );
                Ok((RpList::default(), CompletePager::NeedFlat(p)))
            };
        }

        if delimiter == "/" {
            return if cap.list_with_delimiter_slash {
                let (rp, p) = self.inner.list(path, args).await?;
                Ok((rp, CompletePager::AlreadyComplete(p)))
            } else {
                let (_, p) = self.inner.list(path, args.with_delimiter("")).await?;
                let p = to_hierarchy_pager(p, path);
                Ok((RpList::default(), CompletePager::NeedHierarchy(p)))
            };
        }

        Err(Error::new(
            ErrorKind::Unsupported,
            "list with other delimiter is not supported",
        )
        .with_context("service", self.meta.scheme())
        .with_context("delimiter", delimiter))
    }

    fn complete_blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, CompletePager<A, A::BlockingPager>)> {
        let cap = self.meta.capability();
        if !cap.list {
            return Err(
                Error::new(ErrorKind::Unsupported, "operation is not supported")
                    .with_context("service", self.meta.scheme())
                    .with_operation("list"),
            );
        }

        let delimiter = args.delimiter();

        if delimiter.is_empty() {
            return if cap.list_without_delimiter {
                let (rp, p) = self.inner.blocking_list(path, args)?;
                Ok((rp, CompletePager::AlreadyComplete(p)))
            } else {
                let p = to_flat_pager(
                    self.inner.clone(),
                    path,
                    args.with_delimiter("/").limit().unwrap_or(1000),
                );
                Ok((RpList::default(), CompletePager::NeedFlat(p)))
            };
        }

        if delimiter == "/" {
            return if cap.list_with_delimiter_slash {
                let (rp, p) = self.inner.blocking_list(path, args)?;
                Ok((rp, CompletePager::AlreadyComplete(p)))
            } else {
                let (_, p) = self.inner.blocking_list(path, args.with_delimiter(""))?;
                let p: ToHierarchyPager<<A as Accessor>::BlockingPager> =
                    to_hierarchy_pager(p, path);
                Ok((RpList::default(), CompletePager::NeedHierarchy(p)))
            };
        }

        Err(Error::new(
            ErrorKind::Unsupported,
            "list with other delimiter is not supported",
        )
        .with_context("service", self.meta.scheme())
        .with_context("delimiter", delimiter))
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for CompleteReaderAccessor<A> {
    type Inner = A;
    type Reader = CompleteReader<A, A::Reader>;
    type BlockingReader = CompleteReader<A, A::BlockingReader>;
    type Writer = CompleteWriter<A::Writer>;
    type BlockingWriter = CompleteWriter<A::BlockingWriter>;
    type Appender = CompleteAppender<A::Appender>;
    type Pager = CompletePager<A, A::Pager>;
    type BlockingPager = CompletePager<A, A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.complete_reader(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.complete_blocking_reader(path, args)
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let capability = self.meta.capability();
        if !capability.stat {
            return new_capability_unsupported_error(Operation::Stat);
        }

        self.inner.stat(path, args).await.map(|v| {
            v.map_metadata(|m| {
                let bit = m.bit();
                m.with_bit(bit | Metakey::Complete)
            })
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let capability = self.meta.capability();
        if !capability.stat || !capability.blocking {
            return new_capability_unsupported_error(Operation::BlockingStat);
        }

        self.inner.blocking_stat(path, args).map(|v| {
            v.map_metadata(|m| {
                let bit = m.bit();
                m.with_bit(bit | Metakey::Complete)
            })
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let capability = self.meta.capability();
        if !capability.write {
            return new_capability_unsupported_error(Operation::Write);
        }

        let size = args.content_length();
        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, CompleteWriter::new(w, size)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let capability = self.meta.capability();
        if !capability.write || !capability.blocking {
            return new_capability_unsupported_error(Operation::BlockingWrite);
        }

        let size = args.content_length();
        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| (rp, CompleteWriter::new(w, size)))
    }

    async fn append(&self, path: &str, args: OpAppend) -> Result<(RpAppend, Self::Appender)> {
        let capability = self.meta.capability();
        if !capability.append {
            return new_capability_unsupported_error(Operation::Append);
        }

        self.inner
            .append(path, args)
            .await
            .map(|(rp, a)| (rp, CompleteAppender::new(a)))
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let capability = self.meta.capability();
        if !capability.create_dir {
            return new_capability_unsupported_error(Operation::CreateDir);
        }

        self.inner().create_dir(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let capability = self.meta.capability();
        if !capability.create_dir || !capability.blocking {
            return new_capability_unsupported_error(Operation::BlockingCreateDir);
        }

        self.inner().blocking_create_dir(path, args)
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let capability = self.meta.capability();
        if !capability.delete {
            return new_capability_unsupported_error(Operation::Delete);
        }

        self.inner().delete(path, args).await
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let capability = self.meta.capability();
        if !capability.delete || !capability.blocking {
            return new_capability_unsupported_error(Operation::BlockingDelete);
        }

        self.inner().blocking_delete(path, args)
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let capability = self.meta.capability();
        if !capability.copy {
            return new_capability_unsupported_error(Operation::Copy);
        }

        self.inner().copy(from, to, args).await
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let capability = self.meta.capability();
        if !capability.copy || !capability.blocking {
            return new_capability_unsupported_error(Operation::BlockingCopy);
        }

        self.inner().blocking_copy(from, to, args)
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let capability = self.meta.capability();
        if !capability.rename {
            return new_capability_unsupported_error(Operation::Rename);
        }

        self.inner().rename(from, to, args).await
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let capability = self.meta.capability();
        if !capability.rename || !capability.blocking {
            return new_capability_unsupported_error(Operation::BlockingRename);
        }

        self.inner().blocking_rename(from, to, args)
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let capability = self.meta.capability();
        if !capability.list {
            return new_capability_unsupported_error(Operation::List);
        }

        self.complete_list(path, args).await
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let capability = self.meta.capability();
        if !capability.list || !capability.blocking {
            return new_capability_unsupported_error(Operation::BlockingList);
        }

        self.complete_blocking_list(path, args)
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let capability = self.meta.capability();
        if !capability.presign {
            return new_capability_unsupported_error(Operation::Presign);
        }

        self.inner.presign(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let capability = self.meta.capability();
        if !capability.batch {
            return new_capability_unsupported_error(Operation::Batch);
        }

        self.inner().batch(args).await
    }
}

pub enum CompleteReader<A: Accessor, R> {
    AlreadyComplete(R),
    NeedSeekable(RangeReader<A>),
    NeedStreamable(IntoStreamableReader<R>),
    NeedBoth(IntoStreamableReader<RangeReader<A>>),
}

impl<A, R> oio::Read for CompleteReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.poll_read(cx, buf),
            NeedSeekable(r) => r.poll_read(cx, buf),
            NeedStreamable(r) => r.poll_read(cx, buf),
            NeedBoth(r) => r.poll_read(cx, buf),
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.poll_seek(cx, pos),
            NeedSeekable(r) => r.poll_seek(cx, pos),
            NeedStreamable(r) => r.poll_seek(cx, pos),
            NeedBoth(r) => r.poll_seek(cx, pos),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.poll_next(cx),
            NeedSeekable(r) => r.poll_next(cx),
            NeedStreamable(r) => r.poll_next(cx),
            NeedBoth(r) => r.poll_next(cx),
        }
    }
}

impl<A, R> oio::BlockingRead for CompleteReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.read(buf),
            NeedStreamable(r) => r.read(buf),
            _ => unreachable!("not supported types of complete reader"),
        }
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.seek(pos),
            NeedStreamable(r) => r.seek(pos),
            _ => unreachable!("not supported types of complete reader"),
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.next(),
            NeedStreamable(r) => r.next(),
            _ => unreachable!("not supported types of complete reader"),
        }
    }
}

pub enum CompletePager<A: Accessor, P> {
    AlreadyComplete(P),
    NeedFlat(ToFlatPager<Arc<A>, P>),
    NeedHierarchy(ToHierarchyPager<P>),
}

#[async_trait]
impl<A, P> oio::Page for CompletePager<A, P>
where
    A: Accessor<Pager = P>,
    P: oio::Page,
{
    async fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        use CompletePager::*;

        match self {
            AlreadyComplete(p) => p.next().await,
            NeedFlat(p) => p.next().await,
            NeedHierarchy(p) => p.next().await,
        }
    }
}

impl<A, P> oio::BlockingPage for CompletePager<A, P>
where
    A: Accessor<BlockingPager = P>,
    P: oio::BlockingPage,
{
    fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        use CompletePager::*;

        match self {
            AlreadyComplete(p) => p.next(),
            NeedFlat(p) => p.next(),
            NeedHierarchy(p) => p.next(),
        }
    }
}

pub struct CompleteWriter<W> {
    inner: Option<W>,
    size: Option<u64>,
    written: u64,
}

impl<W> CompleteWriter<W> {
    pub fn new(inner: W, size: Option<u64>) -> CompleteWriter<W> {
        CompleteWriter {
            inner: Some(inner),
            size,
            written: 0,
        }
    }
}

/// Check if the writer has been closed or aborted while debug_assertions
/// enabled. This code will never be executed in release mode.
#[cfg(debug_assertions)]
impl<W> Drop for CompleteWriter<W> {
    fn drop(&mut self) {
        if self.inner.is_some() {
            // Do we need to panic here?
            log::warn!("writer has not been closed or aborted, must be a bug")
        }
    }
}

#[async_trait]
impl<W> oio::Write for CompleteWriter<W>
where
    W: oio::Write,
{
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let n = bs.len();

        if let Some(size) = self.size {
            if self.written + n as u64 > size {
                return Err(Error::new(
                    ErrorKind::ContentTruncated,
                    &format!(
                        "writer got too much data, expect: {size}, actual: {}",
                        self.written + n as u64
                    ),
                ));
            }
        }

        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;
        w.write(bs).await?;
        self.written += n as u64;
        Ok(())
    }

    async fn sink(&mut self, size: u64, s: oio::Streamer) -> Result<()> {
        if let Some(total_size) = self.size {
            if self.written + size > total_size {
                return Err(Error::new(
                    ErrorKind::ContentTruncated,
                    &format!(
                        "writer got too much data, expect: {size}, actual: {}",
                        self.written + size
                    ),
                ));
            }
        }

        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;
        w.sink(size, s).await?;
        self.written += size;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        w.abort().await?;
        self.inner = None;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(size) = self.size {
            if self.written < size {
                return Err(Error::new(
                    ErrorKind::ContentIncomplete,
                    &format!(
                        "writer got too less data, expect: {size}, actual: {}",
                        self.written
                    ),
                ));
            }
        }

        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        w.close().await?;
        self.inner = None;

        Ok(())
    }
}

impl<W> oio::BlockingWrite for CompleteWriter<W>
where
    W: oio::BlockingWrite,
{
    fn write(&mut self, bs: Bytes) -> Result<()> {
        let n = bs.len();

        if let Some(size) = self.size {
            if self.written + n as u64 > size {
                return Err(Error::new(
                    ErrorKind::ContentTruncated,
                    &format!(
                        "writer got too much data, expect: {size}, actual: {}",
                        self.written + n as u64
                    ),
                ));
            }
        }

        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        w.write(bs)?;
        self.written += n as u64;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if let Some(size) = self.size {
            if self.written < size {
                return Err(Error::new(
                    ErrorKind::ContentIncomplete,
                    &format!(
                        "writer got too less data, expect: {size}, actual: {}",
                        self.written
                    ),
                ));
            }
        }

        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        w.close()?;
        self.inner = None;
        Ok(())
    }
}

pub struct CompleteAppender<A> {
    inner: Option<A>,
}

impl<A> CompleteAppender<A> {
    pub fn new(inner: A) -> CompleteAppender<A> {
        CompleteAppender { inner: Some(inner) }
    }
}

/// Check if the appender has been closed while debug_assertions enabled.
/// This code will never be executed in release mode.
#[cfg(debug_assertions)]
impl<A> Drop for CompleteAppender<A> {
    fn drop(&mut self) {
        if self.inner.is_some() {
            // Do we need to panic here?
            log::warn!("appender has not been closed, must be a bug")
        }
    }
}

#[async_trait]
impl<A> oio::Append for CompleteAppender<A>
where
    A: oio::Append,
{
    async fn append(&mut self, bs: Bytes) -> Result<()> {
        let a = self
            .inner
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "appender has been closed"))?;

        a.append(bs).await
    }

    async fn close(&mut self) -> Result<()> {
        let a = self
            .inner
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "appender has been closed"))?;

        a.close().await?;
        self.inner = None;
        Ok(())
    }
}

fn new_capability_unsupported_error<R>(operation: Operation) -> Result<R> {
    Err(Error::new(ErrorKind::Unsupported, "operation is not supported").with_operation(operation))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use async_trait::async_trait;
    use http::HeaderMap;
    use http::Method as HttpMethod;

    use super::*;

    #[derive(Default)]
    struct MockBuilder {
        capability: Capability,
    }

    impl MockBuilder {
        fn with_capacity(mut self, capability: Capability) -> Self {
            self.capability = capability;
            self
        }
    }

    impl Builder for MockBuilder {
        const SCHEME: Scheme = Scheme::Custom("mock");
        type Accessor = MockService;

        fn from_map(_: HashMap<String, String>) -> Self {
            Self::default()
        }

        fn build(&mut self) -> Result<Self::Accessor> {
            Ok(MockService {
                capability: self.capability,
            })
        }
    }

    #[derive(Debug)]
    struct MockService {
        capability: Capability,
    }

    #[async_trait]
    impl Accessor for MockService {
        type Reader = ();
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Appender = ();
        type Pager = ();
        type BlockingPager = ();

        fn info(&self) -> AccessorInfo {
            let mut info = AccessorInfo::default();
            info.set_capability(self.capability);

            info
        }

        async fn stat(&self, _: &str, _: OpStat) -> Result<RpStat> {
            Ok(RpStat::new(Metadata::new(EntryMode::Unknown)))
        }

        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
            Ok((RpRead::new(0), ()))
        }

        async fn write(&self, _: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
            Ok((RpWrite::new(), ()))
        }

        async fn append(&self, _: &str, _: OpAppend) -> Result<(RpAppend, Self::Appender)> {
            Ok((RpAppend::new(), ()))
        }

        async fn copy(&self, _: &str, _: &str, _: OpCopy) -> Result<RpCopy> {
            Ok(RpCopy {})
        }

        async fn create_dir(&self, _: &str, _: OpCreateDir) -> Result<RpCreateDir> {
            Ok(RpCreateDir {})
        }

        async fn delete(&self, _: &str, _: OpDelete) -> Result<RpDelete> {
            Ok(RpDelete {})
        }

        async fn list(&self, _: &str, _: OpList) -> Result<(RpList, Self::Pager)> {
            Ok((RpList {}, ()))
        }

        async fn rename(&self, _: &str, _: &str, _: OpRename) -> Result<RpRename> {
            Ok(RpRename {})
        }

        async fn presign(&self, _: &str, _: OpPresign) -> Result<RpPresign> {
            Ok(RpPresign::new(PresignedRequest::new(
                HttpMethod::POST,
                "https://example.com/presign".parse().expect("should parse"),
                HeaderMap::new(),
            )))
        }
    }

    /// Perform the test against different capability preconditions.
    macro_rules! capability_test {
        ($cap:ident, |$arg:ident| { $($body:tt)* }) => {
            paste::item! {
                #[tokio::test]
                async fn [<test_capability_ $cap>]() {
                    let res_builder = |$arg: Operator| async move {
                        let res = { $($body)* };
                        res.await.err()
                    };

                    let builder = MockBuilder::default().with_capacity(Capability {
                        $cap: false,
                        ..Default::default()
                    });
                    let op = Operator::new(builder).expect("should build").finish();
                    let res = res_builder(op.clone()).await;
                    assert_eq!(res.expect("should not be None").kind(), ErrorKind::Unsupported);

                    let builder = MockBuilder::default().with_capacity(Capability {
                        $cap: true,
                        ..Default::default()
                    });
                    let op = Operator::new(builder).expect("should build").finish();
                    let res = res_builder(op.clone()).await;
                    assert!(res.is_none());
                }
            }
        };
    }

    capability_test!(stat, |op| { op.stat("/path/to/mock_file") });
    capability_test!(read, |op| { op.read("/path/to/mock_file") });
    capability_test!(write, |op| { op.writer("/path/to/mock_file") });
    capability_test!(append, |op| { op.appender("/path/to/mock_file") });
    capability_test!(create_dir, |op| { op.create_dir("/path/to/mock_dir/") });
    capability_test!(delete, |op| { op.delete("/path/to/mock_file") });
    capability_test!(copy, |op| {
        op.copy("/path/to/mock_file", "/path/to/mock_file_2")
    });
    capability_test!(rename, |op| {
        op.rename("/path/to/mock_file", "/path/to/mock_file_2")
    });
    capability_test!(list, |op| { op.list("/path/to/mock_dir/") });
    capability_test!(presign, |op| {
        op.presign_read("/path/to/mock_file", Duration::from_secs(1))
    });
}
