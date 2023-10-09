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

use std::cmp;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;

use crate::raw::oio::into_flat_page;
use crate::raw::oio::into_hierarchy_page;
use crate::raw::oio::ByRangeSeekableReader;
use crate::raw::oio::Entry;
use crate::raw::oio::FlatPager;
use crate::raw::oio::HierarchyPager;
use crate::raw::oio::StreamableReader;
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
/// Underlying services will return [`AccessorInfo`] to indicate the
/// features that returning readers support.
///
/// - If both `seekable` and `streamable`, return directly.
/// - If not `streamable`, with [`oio::into_read_from_stream`].
/// - If not `seekable`, with [`oio::into_seekable_read_by_range`]
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
/// We use [`Capability`] to decide the most suitable implementations.
///
/// If [`Capability`] `read_can_seek` is true, we will open it with given args
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
/// If [`Capability`] `read_can_next` is true, we will use existing reader
/// directly. Otherwise, we will use transform this reader as a stream.
///
/// ## List Completion
///
/// There are two styles of list, but not all services support both of
/// them. CompleteLayer will add those capabilities in a zero cost way.
///
/// Underlying services will return [`Capability`] to indicate the
/// features that returning pagers support.
///
/// - If both `list_with_delimiter_slash` and `list_without_delimiter`, return directly.
/// - If only `list_without_delimiter`, with [`oio::to_flat_pager`].
/// - if only `list_with_delimiter_slash`, with [`oio::to_hierarchy_pager`].
/// - If neither not supported, something must be wrong for `list` is true.
///
/// ## Capability Check
///
/// Before performing any operations, `CompleteLayer` will first check
/// the operation against capability of the underlying service. If the
/// operation is not supported, an error will be returned directly.
pub struct CompleteLayer;

impl<A: Accessor> Layer<A> for CompleteLayer {
    type LayeredAccessor = CompleteReaderAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        CompleteReaderAccessor {
            meta: inner.info(),
            inner: Arc::new(inner),
        }
    }
}

/// Provide complete wrapper for backend.
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
    fn new_unsupported_error(&self, op: impl Into<&'static str>) -> Error {
        let scheme = self.meta.scheme();
        let op = op.into();
        Error::new(
            ErrorKind::Unsupported,
            &format!("service {scheme} doesn't support operation {op}"),
        )
        .with_operation(op)
    }

    async fn complete_reader(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, CompleteReader<A, A::Reader>)> {
        let capability = self.meta.native_capability();
        if !capability.read {
            return Err(self.new_unsupported_error(Operation::Read));
        }

        let seekable = capability.read_can_seek;
        let streamable = capability.read_can_next;

        let range = args.range();
        let (rp, r) = self.inner.read(path, args).await?;
        let content_length = rp.metadata().content_length();

        match (seekable, streamable) {
            (true, true) => Ok((rp, CompleteReader::AlreadyComplete(r))),
            (true, false) => {
                let r = oio::into_streamable_read(r, 256 * 1024);
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
                let r = oio::into_seekable_read_by_range(self.inner.clone(), path, r, offset, size);

                if streamable {
                    Ok((rp, CompleteReader::NeedSeekable(r)))
                } else {
                    let r = oio::into_streamable_read(r, 256 * 1024);
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
        let capability = self.meta.full_capability();
        if !capability.read || !capability.blocking {
            return Err(self.new_unsupported_error(Operation::BlockingRead));
        }

        let seekable = capability.read_can_seek;
        let streamable = capability.read_can_next;

        let range = args.range();
        let (rp, r) = self.inner.blocking_read(path, args)?;
        let content_length = rp.metadata().content_length();

        match (seekable, streamable) {
            (true, true) => Ok((rp, CompleteReader::AlreadyComplete(r))),
            (true, false) => {
                let r = oio::into_streamable_read(r, 256 * 1024);
                Ok((rp, CompleteReader::NeedStreamable(r)))
            }
            _ => {
                let (offset, size) = match (range.offset(), range.size()) {
                    (Some(offset), _) => (offset, content_length),
                    (None, None) => (0, content_length),
                    (None, Some(size)) => {
                        // TODO: we can read content range to calculate
                        // the total content length.
                        let om = self
                            .inner
                            .blocking_stat(path, OpStat::new())?
                            .into_metadata();
                        let total_size = om.content_length();
                        let (offset, size) = if size > total_size {
                            (0, total_size)
                        } else {
                            (total_size - size, size)
                        };

                        (offset, size)
                    }
                };
                let r = oio::into_seekable_read_by_range(self.inner.clone(), path, r, offset, size);

                if streamable {
                    Ok((rp, CompleteReader::NeedSeekable(r)))
                } else {
                    let r = oio::into_streamable_read(r, 256 * 1024);
                    Ok((rp, CompleteReader::NeedBoth(r)))
                }
            }
        }
    }

    async fn complete_list(
        &self,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, CompletePager<A, A::Pager>)> {
        let cap = self.meta.full_capability();
        if !cap.list {
            return Err(self.new_unsupported_error(Operation::List));
        }

        let delimiter = args.delimiter();

        if delimiter.is_empty() {
            return if cap.list_without_delimiter {
                let (rp, p) = self.inner.list(path, args).await?;
                Ok((rp, CompletePager::AlreadyComplete(p)))
            } else {
                let p = into_flat_page(
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
                let p = into_hierarchy_page(p, path);
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
        let cap = self.meta.full_capability();
        if !cap.list {
            return Err(self.new_unsupported_error(Operation::BlockingList));
        }

        let delimiter = args.delimiter();

        if delimiter.is_empty() {
            return if cap.list_without_delimiter {
                let (rp, p) = self.inner.blocking_list(path, args)?;
                Ok((rp, CompletePager::AlreadyComplete(p)))
            } else {
                let p = into_flat_page(
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
                let p: HierarchyPager<<A as Accessor>::BlockingPager> =
                    into_hierarchy_page(p, path);
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
    type Writer = oio::TwoWaysWriter<
        CompleteWriter<A::Writer>,
        oio::ExactBufWriter<CompleteWriter<A::Writer>>,
    >;
    type BlockingWriter = CompleteWriter<A::BlockingWriter>;
    type Pager = CompletePager<A, A::Pager>;
    type BlockingPager = CompletePager<A, A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        let mut meta = self.meta.clone();
        let cap = meta.full_capability_mut();
        if cap.read {
            cap.read_can_next = true;
            cap.read_can_seek = true;
        }
        meta
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let capability = self.meta.full_capability();
        if !capability.create_dir {
            return Err(self.new_unsupported_error(Operation::CreateDir));
        }

        self.inner().create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.complete_reader(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let capability = self.meta.full_capability();
        if !capability.write {
            return Err(self.new_unsupported_error(Operation::Write));
        }
        if args.append() && !capability.write_can_append {
            return Err(Error::new(
                ErrorKind::Unsupported,
                &format!(
                    "service {} doesn't support operation write with append",
                    self.info().scheme()
                ),
            ));
        }

        // Calculate buffer size.
        let buffer_size = args.buffer().map(|mut size| {
            if let Some(v) = capability.write_multi_max_size {
                size = cmp::min(v, size);
            }
            if let Some(v) = capability.write_multi_min_size {
                size = cmp::max(v, size);
            }
            if let Some(v) = capability.write_multi_align_size {
                // Make sure size >= size first.
                size = cmp::max(v, size);
                size -= size % v;
            }

            size
        });

        let (rp, w) = self.inner.write(path, args.clone()).await?;
        let w = CompleteWriter::new(w);

        let w = match buffer_size {
            None => oio::TwoWaysWriter::One(w),
            Some(size) => oio::TwoWaysWriter::Two(oio::ExactBufWriter::new(w, size)),
        };

        Ok((rp, w))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let capability = self.meta.full_capability();
        if !capability.copy {
            return Err(self.new_unsupported_error(Operation::Copy));
        }

        self.inner().copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let capability = self.meta.full_capability();
        if !capability.rename {
            return Err(self.new_unsupported_error(Operation::Rename));
        }

        self.inner().rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let capability = self.meta.full_capability();
        if !capability.stat {
            return Err(self.new_unsupported_error(Operation::Stat));
        }

        self.inner.stat(path, args).await.map(|v| {
            v.map_metadata(|m| {
                let bit = m.bit();
                m.with_bit(bit | Metakey::Complete)
            })
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let capability = self.meta.full_capability();
        if !capability.delete {
            return Err(self.new_unsupported_error(Operation::Delete));
        }

        self.inner().delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let capability = self.meta.full_capability();
        if !capability.list {
            return Err(self.new_unsupported_error(Operation::List));
        }

        self.complete_list(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let capability = self.meta.full_capability();
        if !capability.batch {
            return Err(self.new_unsupported_error(Operation::Batch));
        }

        self.inner().batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let capability = self.meta.full_capability();
        if !capability.presign {
            return Err(self.new_unsupported_error(Operation::Presign));
        }

        self.inner.presign(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let capability = self.meta.full_capability();
        if !capability.create_dir || !capability.blocking {
            return Err(self.new_unsupported_error(Operation::BlockingCreateDir));
        }

        self.inner().blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.complete_blocking_reader(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let capability = self.meta.full_capability();
        if !capability.write || !capability.blocking {
            return Err(self.new_unsupported_error(Operation::BlockingWrite));
        }

        if args.append() && !capability.write_can_append {
            return Err(Error::new(
                ErrorKind::Unsupported,
                &format!(
                    "service {} doesn't support operation write with append",
                    self.info().scheme()
                ),
            ));
        }

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| (rp, CompleteWriter::new(w)))
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let capability = self.meta.full_capability();
        if !capability.copy || !capability.blocking {
            return Err(self.new_unsupported_error(Operation::BlockingCopy));
        }

        self.inner().blocking_copy(from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let capability = self.meta.full_capability();
        if !capability.rename || !capability.blocking {
            return Err(self.new_unsupported_error(Operation::BlockingRename));
        }

        self.inner().blocking_rename(from, to, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let capability = self.meta.full_capability();
        if !capability.stat || !capability.blocking {
            return Err(self.new_unsupported_error(Operation::BlockingStat));
        }

        self.inner.blocking_stat(path, args).map(|v| {
            v.map_metadata(|m| {
                let bit = m.bit();
                m.with_bit(bit | Metakey::Complete)
            })
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let capability = self.meta.full_capability();
        if !capability.delete || !capability.blocking {
            return Err(self.new_unsupported_error(Operation::BlockingDelete));
        }

        self.inner().blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let capability = self.meta.full_capability();
        if !capability.list || !capability.blocking {
            return Err(self.new_unsupported_error(Operation::BlockingList));
        }

        self.complete_blocking_list(path, args)
    }
}

pub enum CompleteReader<A: Accessor, R> {
    AlreadyComplete(R),
    NeedSeekable(ByRangeSeekableReader<A, R>),
    NeedStreamable(StreamableReader<R>),
    NeedBoth(StreamableReader<ByRangeSeekableReader<A, R>>),
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
            NeedSeekable(r) => r.read(buf),
            NeedStreamable(r) => r.read(buf),
            NeedBoth(r) => r.read(buf),
        }
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.seek(pos),
            NeedSeekable(r) => r.seek(pos),
            NeedStreamable(r) => r.seek(pos),
            NeedBoth(r) => r.seek(pos),
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.next(),
            NeedSeekable(r) => r.next(),
            NeedStreamable(r) => r.next(),
            NeedBoth(r) => r.next(),
        }
    }
}

pub enum CompletePager<A: Accessor, P> {
    AlreadyComplete(P),
    NeedFlat(FlatPager<Arc<A>, P>),
    NeedHierarchy(HierarchyPager<P>),
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
}

impl<W> CompleteWriter<W> {
    pub fn new(inner: W) -> CompleteWriter<W> {
        CompleteWriter { inner: Some(inner) }
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
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;
        let n = ready!(w.poll_write(cx, bs))?;

        Poll::Ready(Ok(n))
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        ready!(w.poll_close(cx))?;
        self.inner = None;

        Poll::Ready(Ok(()))
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        ready!(w.poll_abort(cx))?;
        self.inner = None;

        Poll::Ready(Ok(()))
    }
}

impl<W> oio::BlockingWrite for CompleteWriter<W>
where
    W: oio::BlockingWrite,
{
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;
        let n = w.write(bs)?;

        Ok(n)
    }

    fn close(&mut self) -> Result<()> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        w.close()?;
        self.inner = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use http::HeaderMap;
    use http::Method as HttpMethod;

    use super::*;

    #[derive(Debug)]
    struct MockService {
        capability: Capability,
    }

    #[async_trait]
    impl Accessor for MockService {
        type Reader = oio::Reader;
        type BlockingReader = oio::BlockingReader;
        type Writer = oio::Writer;
        type BlockingWriter = oio::BlockingWriter;
        type Pager = oio::Pager;
        type BlockingPager = oio::BlockingPager;

        fn info(&self) -> AccessorInfo {
            let mut info = AccessorInfo::default();
            info.set_native_capability(self.capability);

            info
        }

        async fn create_dir(&self, _: &str, _: OpCreateDir) -> Result<RpCreateDir> {
            Ok(RpCreateDir {})
        }

        async fn read(&self, _: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
            Ok((RpRead::new(0), Box::new(())))
        }

        async fn write(&self, _: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
            Ok((RpWrite::new(), Box::new(())))
        }

        async fn copy(&self, _: &str, _: &str, _: OpCopy) -> Result<RpCopy> {
            Ok(RpCopy {})
        }

        async fn rename(&self, _: &str, _: &str, _: OpRename) -> Result<RpRename> {
            Ok(RpRename {})
        }

        async fn stat(&self, _: &str, _: OpStat) -> Result<RpStat> {
            Ok(RpStat::new(Metadata::new(EntryMode::Unknown)))
        }

        async fn delete(&self, _: &str, _: OpDelete) -> Result<RpDelete> {
            Ok(RpDelete {})
        }

        async fn list(&self, _: &str, _: OpList) -> Result<(RpList, Self::Pager)> {
            Ok((RpList {}, Box::new(())))
        }

        async fn presign(&self, _: &str, _: OpPresign) -> Result<RpPresign> {
            Ok(RpPresign::new(PresignedRequest::new(
                HttpMethod::POST,
                "https://example.com/presign".parse().expect("should parse"),
                HeaderMap::new(),
            )))
        }
    }

    fn new_test_operator(capability: Capability) -> Operator {
        let srv = MockService { capability };

        Operator::from_inner(Arc::new(srv)).layer(CompleteLayer)
    }

    #[tokio::test]
    async fn test_read() {
        let op = new_test_operator(Capability::default());
        let res = op.read("path").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            read: true,
            ..Default::default()
        });
        let res = op.read("path").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_stat() {
        let op = new_test_operator(Capability::default());
        let res = op.stat("path").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            stat: true,
            ..Default::default()
        });
        let res = op.stat("path").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_writer() {
        let op = new_test_operator(Capability::default());
        let res = op.write("path", vec![]).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            write: true,
            ..Default::default()
        });
        let res = op.writer("path").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_create_dir() {
        let op = new_test_operator(Capability::default());
        let res = op.create_dir("path/").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            create_dir: true,
            ..Default::default()
        });
        let res = op.create_dir("path/").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_delete() {
        let op = new_test_operator(Capability::default());
        let res = op.delete("path").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            delete: true,
            ..Default::default()
        });
        let res = op.delete("path").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_copy() {
        let op = new_test_operator(Capability::default());
        let res = op.copy("path_a", "path_b").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            copy: true,
            ..Default::default()
        });
        let res = op.copy("path_a", "path_b").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_rename() {
        let op = new_test_operator(Capability::default());
        let res = op.rename("path_a", "path_b").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            rename: true,
            ..Default::default()
        });
        let res = op.rename("path_a", "path_b").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_list() {
        let op = new_test_operator(Capability::default());
        let res = op.list("path/").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            list: true,
            ..Default::default()
        });
        let res = op.list("path/").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_presign() {
        let op = new_test_operator(Capability::default());
        let res = op.presign_read("path", Duration::from_secs(1)).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            presign: true,
            ..Default::default()
        });
        let res = op.presign_read("path", Duration::from_secs(1)).await;
        assert!(res.is_ok())
    }
}
