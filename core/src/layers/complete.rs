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

use crate::raw::oio::FlatLister;
use crate::raw::oio::{PrefixLister, RecursiveDeleter};
use crate::raw::*;
use crate::*;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

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
/// ## Stat Completion
///
/// Not all services support stat dir natively, but we can simulate it via list.
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
/// features that returning listers support.
///
/// - If support `list_with_recursive`, return directly.
/// - if not, wrap with [`FlatLister`].
///
pub struct CompleteLayer;

impl<A: Access> Layer<A> for CompleteLayer {
    type LayeredAccess = CompleteAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        info.update_full_capability(|mut cap| {
            if cap.list && cap.write_can_empty {
                cap.create_dir = true;
            }
            cap
        });

        CompleteAccessor {
            info,
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

impl<A: Access> CompleteAccessor<A> {
    async fn complete_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let capability = self.info.native_capability();
        if capability.create_dir {
            return self.inner().create_dir(path, args).await;
        }

        if capability.write_can_empty && capability.list {
            let (_, mut w) = self.inner.write(path, OpWrite::default()).await?;
            oio::Write::close(&mut w).await?;
            return Ok(RpCreateDir::default());
        }

        self.inner.create_dir(path, args).await
    }

    fn complete_blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let capability = self.info.native_capability();
        if capability.create_dir && capability.blocking {
            return self.inner().blocking_create_dir(path, args);
        }

        if capability.write_can_empty && capability.list && capability.blocking {
            let (_, mut w) = self.inner.blocking_write(path, OpWrite::default())?;
            oio::BlockingWrite::close(&mut w)?;
            return Ok(RpCreateDir::default());
        }

        self.inner.blocking_create_dir(path, args)
    }

    async fn complete_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let capability = self.info.native_capability();

        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        // Forward to inner if create_dir is supported.
        if path.ends_with('/') && capability.create_dir {
            let meta = self.inner.stat(path, args).await?.into_metadata();

            if meta.is_file() {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "stat expected a directory, but found a file",
                ));
            }

            return Ok(RpStat::new(meta));
        }

        // Otherwise, we can simulate stat dir via `list`.
        if path.ends_with('/') && capability.list_with_recursive {
            let (_, mut l) = self
                .inner
                .list(path, OpList::default().with_recursive(true).with_limit(1))
                .await?;

            return if oio::List::next(&mut l).await?.is_some() {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            } else {
                Err(Error::new(
                    ErrorKind::NotFound,
                    "the directory is not found",
                ))
            };
        }

        // Forward to underlying storage directly since we don't know how to handle stat dir.
        self.inner.stat(path, args).await
    }

    fn complete_blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let capability = self.info.native_capability();

        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        // Forward to inner if create dir is supported.
        if path.ends_with('/') && capability.create_dir {
            let meta = self.inner.blocking_stat(path, args)?.into_metadata();

            if meta.is_file() {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "stat expected a directory, but found a file",
                ));
            }

            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        // Otherwise, we can simulate stat a dir path via `list`.
        if path.ends_with('/') && capability.list_with_recursive {
            let (_, mut l) = self
                .inner
                .blocking_list(path, OpList::default().with_recursive(true).with_limit(1))?;

            return if oio::BlockingList::next(&mut l)?.is_some() {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            } else {
                Err(Error::new(
                    ErrorKind::NotFound,
                    "the directory is not found",
                ))
            };
        }

        // Forward to underlying storage directly since we don't know how to handle stat dir.
        self.inner.blocking_stat(path, args)
    }

    async fn complete_list(
        &self,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, CompleteLister<A, A::Lister>)> {
        let cap = self.info.native_capability();

        let recursive = args.recursive();

        match (recursive, cap.list_with_recursive) {
            // - If service can list_with_recursive, we can forward list to it directly.
            (_, true) => {
                let (rp, p) = self.inner.list(path, args).await?;
                Ok((rp, CompleteLister::One(p)))
            }
            // If recursive is true but service can't list_with_recursive
            (true, false) => {
                // Forward path that ends with /
                if path.ends_with('/') {
                    let p = FlatLister::new(self.inner.clone(), path);
                    Ok((RpList::default(), CompleteLister::Two(p)))
                } else {
                    let parent = get_parent(path);
                    let p = FlatLister::new(self.inner.clone(), parent);
                    let p = PrefixLister::new(p, path);
                    Ok((RpList::default(), CompleteLister::Four(p)))
                }
            }
            // If recursive and service doesn't support list_with_recursive, we need to handle
            // list prefix by ourselves.
            (false, false) => {
                // Forward path that ends with /
                if path.ends_with('/') {
                    let (rp, p) = self.inner.list(path, args).await?;
                    Ok((rp, CompleteLister::One(p)))
                } else {
                    let parent = get_parent(path);
                    let (rp, p) = self.inner.list(parent, args).await?;
                    let p = PrefixLister::new(p, path);
                    Ok((rp, CompleteLister::Three(p)))
                }
            }
        }
    }

    fn complete_blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, CompleteLister<A, A::BlockingLister>)> {
        let cap = self.info.native_capability();

        let recursive = args.recursive();

        match (recursive, cap.list_with_recursive) {
            // - If service can list_with_recursive, we can forward list to it directly.
            (_, true) => {
                let (rp, p) = self.inner.blocking_list(path, args)?;
                Ok((rp, CompleteLister::One(p)))
            }
            // If recursive is true but service can't list_with_recursive
            (true, false) => {
                // Forward path that ends with /
                if path.ends_with('/') {
                    let p = FlatLister::new(self.inner.clone(), path);
                    Ok((RpList::default(), CompleteLister::Two(p)))
                } else {
                    let parent = get_parent(path);
                    let p = FlatLister::new(self.inner.clone(), parent);
                    let p = PrefixLister::new(p, path);
                    Ok((RpList::default(), CompleteLister::Four(p)))
                }
            }
            // If recursive and service doesn't support list_with_recursive, we need to handle
            // list prefix by ourselves.
            (false, false) => {
                // Forward path that ends with /
                if path.ends_with('/') {
                    let (rp, p) = self.inner.blocking_list(path, args)?;
                    Ok((rp, CompleteLister::One(p)))
                } else {
                    let parent = get_parent(path);
                    let (rp, p) = self.inner.blocking_list(parent, args)?;
                    let p = PrefixLister::new(p, path);
                    Ok((rp, CompleteLister::Three(p)))
                }
            }
        }
    }
}

impl<A: Access> LayeredAccess for CompleteAccessor<A> {
    type Inner = A;
    type Reader = CompleteReader<A::Reader>;
    type BlockingReader = CompleteReader<A::BlockingReader>;
    type Writer = CompleteWriter<A::Writer>;
    type BlockingWriter = CompleteWriter<A::BlockingWriter>;
    type Lister = CompleteLister<A, A::Lister>;
    type BlockingLister = CompleteLister<A, A::BlockingLister>;
    type Deleter = CompleteDeleter<A, A::Deleter>;
    type BlockingDeleter = A::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.complete_create_dir(path, args).await
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
        self.complete_stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, d) = self.inner().delete().await?;
        let d = CompleteDeleter::new(self.inner.clone(), d);
        Ok((rp, d))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.complete_list(path, args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.complete_blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let size = args.range().size();
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, CompleteReader::new(r, size)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let append = args.append();
        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| (rp, CompleteWriter::new(w, append)))
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.complete_blocking_stat(path, args)
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.inner().blocking_delete()
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.complete_blocking_list(path, args)
    }
}

pub type CompleteLister<A, P> =
    FourWays<P, FlatLister<Arc<A>, P>, PrefixLister<P>, PrefixLister<FlatLister<Arc<A>, P>>>;

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

impl<R: oio::BlockingRead> oio::BlockingRead for CompleteReader<R> {
    fn read(&mut self) -> Result<Buffer> {
        let buf = self.inner.read()?;

        if buf.is_empty() {
            self.check()?;
        } else {
            self.read += buf.len() as u64;
        }

        Ok(buf)
    }
}

pub struct CompleteWriter<W> {
    inner: Option<W>,
    append: bool,
    size: u64,
}

impl<W> CompleteWriter<W> {
    pub fn new(inner: W, append: bool) -> CompleteWriter<W> {
        CompleteWriter {
            inner: Some(inner),
            append,
            size: 0,
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
        if self.inner.is_some() {
            log::warn!("writer has not been closed or aborted, must be a bug")
        }
    }
}

impl<W> oio::Write for CompleteWriter<W>
where
    W: oio::Write,
{
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        let len = bs.len();
        w.write(bs).await?;
        self.size += len as u64;

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        // we must return `Err` before setting inner to None; otherwise,
        // we won't be able to retry `close` in `RetryLayer`.
        let mut ret = w.close().await?;
        self.check(ret.content_length())?;
        if ret.content_length() == 0 {
            ret = ret.with_content_length(self.size);
        }
        self.inner = None;

        Ok(ret)
    }

    async fn abort(&mut self) -> Result<()> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        w.abort().await?;
        self.inner = None;

        Ok(())
    }
}

impl<W> oio::BlockingWrite for CompleteWriter<W>
where
    W: oio::BlockingWrite,
{
    fn write(&mut self, bs: Buffer) -> Result<()> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        let len = bs.len();
        w.write(bs)?;
        self.size += len as u64;

        Ok(())
    }

    fn close(&mut self) -> Result<Metadata> {
        let w = self.inner.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        let mut ret = w.close()?;
        self.check(ret.content_length())?;
        if ret.content_length() == 0 {
            ret = ret.with_content_length(self.size);
        }
        self.inner = None;

        Ok(ret)
    }
}

pub struct CompleteDeleter<A, D> {
    inner: RecursiveDeleter<A, D>,
    buffer: HashSet<(String, OpDelete)>,
}

impl<A: Access, D: oio::Delete> CompleteDeleter<A, D> {
    pub fn new(acc: Arc<A>, deleter: D) -> Self {
        Self {
            inner: RecursiveDeleter::new(acc, deleter),
            buffer: HashSet::default(),
        }
    }
}

impl<A: Access, D: oio::Delete> oio::Delete for CompleteDeleter<A, D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.buffer.insert((path.to_string(), args));
        Ok(())
    }

    async fn flush(&mut self) -> Result<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let size = self.buffer.len();
        for (path, args) in self.buffer.clone() {
            if args.recursive() {
                self.inner.recursive_delete(path.as_str()).await?
            } else {
                self.inner.delete(path.as_str(), args.clone()).await?;
            }

            self.buffer.remove(&(path, args));
        }

        self.inner.flush_all().await?;
        Ok(size)
    }
}
