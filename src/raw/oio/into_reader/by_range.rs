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
use std::future::Future;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::ready;
use tokio::io::ReadBuf;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Convert given reader into [`oio::Reader`] by range.
///
/// # Notes
///
/// This operation is not zero cost. If the accessor already returns a
/// seekable reader, please don't use this.
pub fn by_range<A: Accessor>(
    acc: Arc<A>,
    path: &str,
    reader: A::Reader,
    offset: u64,
    size: u64,
) -> RangeReader<A> {
    RangeReader {
        acc,
        path: path.to_string(),
        offset,
        size,
        cur: 0,
        state: State::Reading(reader),
        last_seek_pos: None,
        sink: Vec::new(),
    }
}

/// RangeReader that can do seek on non-seekable reader.
pub struct RangeReader<A: Accessor> {
    acc: Arc<A>,
    path: String,

    offset: u64,
    size: u64,
    cur: u64,
    state: State<A::Reader>,

    /// Seek operation could return Pending which may lead
    /// `SeekFrom::Current(off)` been input multiple times.
    ///
    /// So we need to store the last seek pos to make sure
    /// we always seek to the right position.
    last_seek_pos: Option<u64>,
    /// sink is to consume bytes for seek optimize.
    sink: Vec<u8>,
}

enum State<R: oio::Read> {
    Idle,
    Sending(BoxFuture<'static, Result<(RpRead, R)>>),
    Reading(R),
}

/// Safety: State will only be accessed under &mut.
unsafe impl<R: oio::Read> Sync for State<R> {}

impl<A: Accessor> RangeReader<A> {
    fn read_future(&self) -> BoxFuture<'static, Result<(RpRead, A::Reader)>> {
        let acc = self.acc.clone();
        let path = self.path.clone();
        let op = OpRead::default().with_range(BytesRange::new(
            Some(self.offset + self.cur),
            Some(self.size - self.cur),
        ));

        Box::pin(async move { acc.read(&path, op).await })
    }

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
                    ErrorKind::Unexpected,
                    "invalid seek to a negative or overflowing position",
                ))
            }
        };
        Ok(n)
    }
}

impl<A: Accessor> oio::Read for RangeReader<A> {
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
                Ok(n) if n == 0 => {
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

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
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
                // poll_seek. We can just drop this future.
                self.state = State::Idle;
                self.poll_seek(cx, SeekFrom::Start(seek_pos))
            }
            State::Reading(r) => {
                if seek_pos == self.cur {
                    self.last_seek_pos = None;
                    return Poll::Ready(Ok(self.cur));
                }

                // If the next seek pos is close enough, we can just
                // read the cnt instead of dropping the reader.
                //
                // TODO: make this value configurable
                if seek_pos > self.cur && seek_pos - self.cur < 1024 * 1024 {
                    // 212992 is the default read mem buffer of archlinux.
                    // Ideally we should make this configurable.
                    //
                    // TODO: make this value configurable
                    let consume = cmp::min((seek_pos - self.cur) as usize, 212992);
                    self.sink.reserve(consume);

                    let mut buf = ReadBuf::uninit(self.sink.spare_capacity_mut());
                    unsafe { buf.assume_init(consume) };

                    match ready!(Pin::new(r).poll_read(cx, buf.initialized_mut())) {
                        Ok(n) => {
                            assert!(n > 0, "consumed bytes must be valid");
                            self.cur += n as u64;
                            // Make sure the pos is absolute from start.
                            self.poll_seek(cx, SeekFrom::Start(seek_pos))
                        }
                        Err(_) => {
                            // If we are hitting errors while read ahead.
                            // It's better to drop this reader and seek to
                            // correct position directly.
                            self.state = State::Idle;
                            self.cur = seek_pos;
                            self.last_seek_pos = None;
                            Poll::Ready(Ok(self.cur))
                        }
                    }
                } else {
                    // If we are trying to seek to far more away.
                    // Let's just drop the reader.
                    self.state = State::Idle;
                    self.cur = seek_pos;
                    self.last_seek_pos = None;
                    Poll::Ready(Ok(self.cur))
                }
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<bytes::Bytes>>> {
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
            am.set_capabilities(AccessorCapability::Read);

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
        let mut r = Box::new(by_range(acc, "x", r, 0, bs.len() as u64)) as oio::Reader;

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
        let mut r = Box::new(by_range(acc, "x", r, 4096, 4096)) as oio::Reader;

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
