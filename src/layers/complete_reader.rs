// Copyright 2023 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Complete underlying services reader support.
///
/// OpenDAL requires all reader implements [`output::Read`] and
/// [`output::BlockingRead`]. However, not all services have the
/// capabilities. CompleteReaderLayer is uesd to add those capabilities in
/// a zero cost way.
///
/// Underlying services will return [`AccessorHint`] to indicate the
/// features that returning readers support.
///
/// - If both `seekable` and `streamable`, we will return directly.
/// - If not `streamable`, we will wrap with [`output::into_streamable_reader`].
/// - If not `seekable`, we will wrap with [`output::into_reader::by_range`]
/// - If neither not supported, we will wrap both by_range and into_streamable.
///
/// [`AccessorHint`]: crate::raw::AccessorHint
pub struct CompleteReaderLayer;

impl<A: Accessor> Layer<A> for CompleteReaderLayer {
    type LayeredAccessor = CompleteReaderAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.metadata();
        CompleteReaderAccessor {
            meta,
            inner: Arc::new(inner),
        }
    }
}

/// Provide reader wrapper for backend.
pub struct CompleteReaderAccessor<A: Accessor> {
    meta: AccessorMetadata,
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
    ) -> Result<(RpRead, CompleteReader<A>)> {
        let (seekable, streamable) = (
            self.meta.hints().contains(AccessorHint::ReadIsSeekable),
            self.meta.hints().contains(AccessorHint::ReadIsStreamable),
        );

        let range = args.range();
        let (rp, r) = self.inner.read(path, args).await?;
        let content_length = rp.metadata().content_length();

        match (seekable, streamable) {
            (true, true) => Ok((rp, CompleteReader::AlreadyComplete(r))),
            (true, false) => {
                let r = output::into_streamable_reader(r, 256 * 1024);
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
                let r = output::into_reader::by_range(self.inner.clone(), path, r, offset, size);

                if streamable {
                    Ok((rp, CompleteReader::NeedSeekable(r)))
                } else {
                    let r = output::into_streamable_reader(r, 256 * 1024);
                    Ok((rp, CompleteReader::NeedBoth(r)))
                }
            }
        }
    }

    fn complete_blokcing_reader(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, CompleteBlockingReader<A>)> {
        let (seekable, streamable) = (
            self.meta.hints().contains(AccessorHint::ReadIsSeekable),
            self.meta.hints().contains(AccessorHint::ReadIsStreamable),
        );

        let (rp, r) = self.inner.blocking_read(path, args)?;

        match (seekable, streamable) {
            (true, true) => Ok((rp, CompleteBlockingReader::AlreadyComplete(r))),
            (true, false) => {
                let r = output::into_streamable_reader(r, 256 * 1024);
                Ok((rp, CompleteBlockingReader::NeedStreamable(r)))
            }
            (false, _) => Err(Error::new(
                ErrorKind::Unsupported,
                "non seekable blocking reader is not supported",
            )),
        }
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for CompleteReaderAccessor<A> {
    type Inner = A;
    type Reader = CompleteReader<A>;
    type BlockingReader = CompleteBlockingReader<A>;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.complete_reader(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.complete_blokcing_reader(path, args)
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner.list(path, args).await
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }
}

pub enum CompleteReader<A: Accessor> {
    AlreadyComplete(A::Reader),
    NeedSeekable(output::into_reader::RangeReader<A>),
    NeedStreamable(output::IntoStreamableReader<A::Reader>),
    NeedBoth(output::IntoStreamableReader<output::into_reader::RangeReader<A>>),
}

impl<A: Accessor> output::Read for CompleteReader<A> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.poll_read(cx, buf),
            NeedSeekable(r) => r.poll_read(cx, buf),
            NeedStreamable(r) => r.poll_read(cx, buf),
            NeedBoth(r) => r.poll_read(cx, buf),
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<io::Result<u64>> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.poll_seek(cx, pos),
            NeedSeekable(r) => r.poll_seek(cx, pos),
            NeedStreamable(r) => r.poll_seek(cx, pos),
            NeedBoth(r) => r.poll_seek(cx, pos),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<bytes::Bytes>>> {
        use CompleteReader::*;

        match self {
            AlreadyComplete(r) => r.poll_next(cx),
            NeedSeekable(r) => r.poll_next(cx),
            NeedStreamable(r) => r.poll_next(cx),
            NeedBoth(r) => r.poll_next(cx),
        }
    }
}

pub enum CompleteBlockingReader<A: Accessor> {
    AlreadyComplete(A::BlockingReader),
    NeedStreamable(output::IntoStreamableReader<A::BlockingReader>),
}

impl<A: Accessor> output::BlockingRead for CompleteBlockingReader<A> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        use CompleteBlockingReader::*;

        match self {
            AlreadyComplete(r) => r.read(buf),
            NeedStreamable(r) => r.read(buf),
        }
    }

    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        use CompleteBlockingReader::*;

        match self {
            AlreadyComplete(r) => r.seek(pos),
            NeedStreamable(r) => r.seek(pos),
        }
    }

    fn next(&mut self) -> Option<io::Result<bytes::Bytes>> {
        use CompleteBlockingReader::*;

        match self {
            AlreadyComplete(r) => r.next(),
            NeedStreamable(r) => r.next(),
        }
    }
}
