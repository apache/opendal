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
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::future::BoxFuture;

use crate::raw::*;
use crate::*;

/// Convert given reader into [`oio::Reader`] by range.
///
/// # Input
///
/// The input is an Accessor will may return a non-seekable reader.
///
/// # Output
///
/// The output is a reader that can be seek by range.
///
/// # Notes
///
/// This operation is not zero cost. If the accessor already returns a
/// seekable reader, please don't use this.
pub fn into_seekable_read_by_range<A: Accessor, R>(
    acc: Arc<A>,
    path: &str,
    reader: R,
    offset: u64,
    size: u64,
) -> ByRangeSeekableReader<A, R> {
    ByRangeSeekableReader {
        acc,
        path: path.to_string(),
        offset,
        size,
        cur: 0,
        state: State::Reading(reader),
        last_seek_pos: None,
    }
}

/// ByRangeReader that can do seek on non-seekable reader.
pub struct ByRangeSeekableReader<A: Accessor, R> {
    acc: Arc<A>,
    path: String,

    offset: u64,
    size: u64,
    cur: u64,
    state: State<R>,

    /// Seek operation could return Pending which may lead
    /// `SeekFrom::Current(off)` been input multiple times.
    ///
    /// So we need to store the last seek pos to make sure
    /// we always seek to the right position.
    last_seek_pos: Option<u64>,
}

enum State<R> {
    Idle,
    Sending(BoxFuture<'static, Result<(RpRead, R)>>),
    Reading(R),
}

/// Safety: State will only be accessed under &mut.
unsafe impl<R> Sync for State<R> {}

impl<A, R> ByRangeSeekableReader<A, R>
where
    A: Accessor,
{
    /// calculate the seek position.
    ///
    /// This operation will not update the `self.cur`.
    fn seek_pos(&self, pos: SeekFrom) -> Result<u64> {
        if let Some(last_pos) = self.last_seek_pos {
            return Ok(last_pos);
        }

        let (base, amt) = match pos {
            SeekFrom::Start(n) => (0, n as i64),
            SeekFrom::End(n) => (self.size as i64, n),
            SeekFrom::Current(n) => (self.cur as i64, n),
        };

        let n = match base.checked_add(amt) {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ))
            }
        };
        Ok(n)
    }
}

impl<A, R> ByRangeSeekableReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    fn read_future(&self) -> BoxFuture<'static, Result<(RpRead, R)>> {
        let acc = self.acc.clone();
        let path = self.path.clone();
        let op = OpRead::default().with_range(BytesRange::new(
            Some(self.offset + self.cur),
            Some(self.size - self.cur),
        ));

        Box::pin(async move { acc.read(&path, op).await })
    }
}

impl<A, R> ByRangeSeekableReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read_action(&self) -> Result<(RpRead, R)> {
        let acc = self.acc.clone();
        let path = self.path.clone();
        let op = OpRead::default().with_range(BytesRange::new(
            Some(self.offset + self.cur),
            Some(self.size - self.cur),
        ));

        acc.blocking_read(&path, op)
    }
}

impl<A, R> oio::Read for ByRangeSeekableReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match &mut self.state {
            State::Idle => {
                if self.cur >= self.size {
                    return Poll::Ready(Ok(0));
                }

                self.state = State::Sending(self.read_future());
                self.poll_read(cx, buf)
            }
            State::Sending(fut) => {
                // TODO
                //
                // we can use RpRead returned here to correct size.
                let (_, r) = ready!(Pin::new(fut).poll(cx)).map_err(|err| {
                    // If read future returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.state = State::Idle;
                    err
                })?;

                self.state = State::Reading(r);
                self.poll_read(cx, buf)
            }
            State::Reading(r) => match ready!(Pin::new(r).poll_read(cx, buf)) {
                Ok(0) => {
                    // Reset state to Idle after all data has been consumed.
                    self.state = State::Idle;
                    Poll::Ready(Ok(0))
                }
                Ok(n) => {
                    self.cur += n as u64;
                    Poll::Ready(Ok(n))
                }
                Err(e) => {
                    self.state = State::Idle;
                    Poll::Ready(Err(e))
                }
            },
        }
    }

    fn poll_seek(&mut self, _: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let seek_pos = self.seek_pos(pos)?;
        self.last_seek_pos = Some(seek_pos);

        match &mut self.state {
            State::Idle => {
                self.cur = seek_pos;
                self.last_seek_pos = None;
                Poll::Ready(Ok(self.cur))
            }
            State::Sending(_) => {
                // It's impossible for us to go into this state while
                // poll_seek. We can just drop this future and check state.
                self.state = State::Idle;

                self.cur = seek_pos;
                self.last_seek_pos = None;
                Poll::Ready(Ok(self.cur))
            }
            State::Reading(_) => {
                if seek_pos == self.cur {
                    self.last_seek_pos = None;
                    return Poll::Ready(Ok(self.cur));
                }

                self.state = State::Idle;
                self.cur = seek_pos;
                self.last_seek_pos = None;
                Poll::Ready(Ok(self.cur))
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match &mut self.state {
            State::Idle => {
                if self.cur >= self.size {
                    return Poll::Ready(None);
                }

                self.state = State::Sending(self.read_future());
                self.poll_next(cx)
            }
            State::Sending(fut) => {
                // TODO
                //
                // we can use RpRead returned here to correct size.
                let (_, r) = ready!(Pin::new(fut).poll(cx)).map_err(|err| {
                    // If read future returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.state = State::Idle;
                    err
                })?;

                self.state = State::Reading(r);
                self.poll_next(cx)
            }
            State::Reading(r) => match ready!(Pin::new(r).poll_next(cx)) {
                Some(Ok(bs)) => {
                    self.cur += bs.len() as u64;
                    Poll::Ready(Some(Ok(bs)))
                }
                Some(Err(err)) => {
                    self.state = State::Idle;
                    Poll::Ready(Some(Err(err)))
                }
                None => {
                    self.state = State::Idle;
                    Poll::Ready(None)
                }
            },
        }
    }
}

impl<A, R> oio::BlockingRead for ByRangeSeekableReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match &mut self.state {
            State::Idle => {
                if self.cur >= self.size {
                    return Ok(0);
                }

                let (_, r) = self.read_action()?;
                self.state = State::Reading(r);
                self.read(buf)
            }
            State::Reading(r) => {
                match r.read(buf) {
                    Ok(0) => {
                        // Reset state to Idle after all data has been consumed.
                        self.state = State::Idle;
                        Ok(0)
                    }
                    Ok(n) => {
                        self.cur += n as u64;
                        Ok(n)
                    }
                    Err(e) => {
                        self.state = State::Idle;
                        Err(e)
                    }
                }
            }
            State::Sending(_) => {
                unreachable!("It's invalid to go into State::Sending for BlockingRead, please report this bug")
            }
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let seek_pos = self.seek_pos(pos)?;

        match &mut self.state {
            State::Idle => {
                self.cur = seek_pos;
                Ok(self.cur)
            }
            State::Reading(_) => {
                if seek_pos == self.cur {
                    return Ok(self.cur);
                }

                self.state = State::Idle;
                self.cur = seek_pos;
                Ok(self.cur)
            }
            State::Sending(_) => {
                unreachable!("It's invalid to go into State::Sending for BlockingRead, please report this bug")
            }
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        match &mut self.state {
            State::Idle => {
                if self.cur >= self.size {
                    return None;
                }

                let r = match self.read_action() {
                    Ok((_, r)) => r,
                    Err(err) => return Some(Err(err)),
                };
                self.state = State::Reading(r);
                self.next()
            }
            State::Reading(r) => match r.next() {
                Some(Ok(bs)) => {
                    self.cur += bs.len() as u64;
                    Some(Ok(bs))
                }
                Some(Err(err)) => {
                    self.state = State::Idle;
                    Some(Err(err))
                }
                None => {
                    self.state = State::Idle;
                    None
                }
            },
            State::Sending(_) => {
                unreachable!("It's invalid to go into State::Sending for BlockingRead, please report this bug")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::SeekFrom;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::AsyncRead;
    use futures::AsyncReadExt;
    use futures::AsyncSeekExt;
    use rand::prelude::*;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;

    // Generate bytes between [4MiB, 16MiB)
    fn gen_bytes() -> (Bytes, usize) {
        let mut rng = thread_rng();

        let size = rng.gen_range(4 * 1024 * 1024..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        (Bytes::from(content), size)
    }

    #[derive(Debug, Clone, Default)]
    struct MockReadService {
        data: Bytes,
    }

    impl MockReadService {
        fn new(data: Bytes) -> Self {
            Self { data }
        }
    }

    #[async_trait]
    impl Accessor for MockReadService {
        type Reader = MockReader;
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Pager = ();
        type BlockingPager = ();

        fn info(&self) -> AccessorInfo {
            let mut am = AccessorInfo::default();
            am.set_native_capability(Capability {
                read: true,
                ..Default::default()
            });

            am
        }

        async fn read(&self, _: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
            let bs = args.range().apply_on_bytes(self.data.clone());

            Ok((
                RpRead::new(bs.len() as u64),
                MockReader {
                    inner: futures::io::Cursor::new(bs.into()),
                },
            ))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReader {
        inner: futures::io::Cursor<Vec<u8>>,
    }

    impl oio::Read for MockReader {
        fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
            Pin::new(&mut self.inner).poll_read(cx, buf).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "read data from mock").set_source(err)
            })
        }

        fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
            let (_, _) = (cx, pos);

            Poll::Ready(Err(Error::new(
                ErrorKind::Unsupported,
                "output reader doesn't support seeking",
            )))
        }

        fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
            let mut bs = vec![0; 4 * 1024];
            let n = ready!(Pin::new(&mut self.inner)
                .poll_read(cx, &mut bs)
                .map_err(
                    |err| Error::new(ErrorKind::Unexpected, "read data from mock").set_source(err)
                )?);
            if n == 0 {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(Ok(Bytes::from(bs[..n].to_vec()))))
            }
        }
    }

    #[tokio::test]
    async fn test_read_all() -> anyhow::Result<()> {
        let (bs, _) = gen_bytes();
        let acc = Arc::new(MockReadService::new(bs.clone()));

        let r = MockReader {
            inner: futures::io::Cursor::new(bs.to_vec()),
        };
        let mut r =
            Box::new(into_seekable_read_by_range(acc, "x", r, 0, bs.len() as u64)) as oio::Reader;

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await?;
        assert_eq!(bs.len(), buf.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&buf)),
            "read content"
        );

        let n = r.seek(SeekFrom::Start(0)).await?;
        assert_eq!(n, 0, "seek position must be 0");

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await?;
        assert_eq!(bs.len(), buf.len(), "read twice size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&buf)),
            "read twice content"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_part() -> anyhow::Result<()> {
        let (bs, _) = gen_bytes();
        let acc = Arc::new(MockReadService::new(bs.clone()));

        let r = MockReader {
            inner: futures::io::Cursor::new(bs[4096..4096 + 4096].to_vec()),
        };
        let mut r = Box::new(into_seekable_read_by_range(acc, "x", r, 4096, 4096)) as oio::Reader;

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await?;
        assert_eq!(4096, buf.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096..4096 + 4096])),
            format!("{:x}", Sha256::digest(&buf)),
            "read content"
        );

        let n = r.seek(SeekFrom::Start(0)).await?;
        assert_eq!(n, 0, "seek position must be 0");

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await?;
        assert_eq!(4096, buf.len(), "read twice size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096..4096 + 4096])),
            format!("{:x}", Sha256::digest(&buf)),
            "read twice content"
        );

        let n = r.seek(SeekFrom::Start(1024)).await?;
        assert_eq!(1024, n, "seek to 1024");

        let mut buf = vec![0; 1024];
        r.read_exact(&mut buf).await?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096 + 1024..4096 + 2048])),
            format!("{:x}", Sha256::digest(&buf)),
            "read after seek 1024"
        );

        let n = r.seek(SeekFrom::Current(1024)).await?;
        assert_eq!(3072, n, "seek to 3072");

        let mut buf = vec![0; 1024];
        r.read_exact(&mut buf).await?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096 + 3072..4096 + 3072 + 1024])),
            format!("{:x}", Sha256::digest(&buf)),
            "read after seek to 3072"
        );

        Ok(())
    }
}
