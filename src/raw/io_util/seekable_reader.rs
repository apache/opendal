// Copyright 2022 Datafuse Labs.
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

use std::future::Future;
use std::io;
use std::io::SeekFrom;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncSeek;

use crate::raw::*;
use crate::*;

/// Add seek support for object via internal lazy operation.
///
/// # Example
///
/// ```no_run
/// # use opendal::Operator;
/// # use opendal::Scheme;
/// # use anyhow::Result;
/// # use futures::{AsyncReadExt, AsyncSeekExt};
/// use std::io::SeekFrom;
///
/// use opendal::raw::seekable_read;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let op = Operator::from_env(Scheme::Memory)?;
/// let o = op.object("test");
/// let mut r = seekable_read(&o, 10..);
/// r.seek(SeekFrom::Current(10)).await?;
/// let mut bs = vec![0; 10];
/// r.read(&mut bs).await?;
/// # Ok(())
/// # }
/// ```
pub fn seekable_read(o: &Object, range: impl RangeBounds<u64>) -> SeekableReader {
    let br = BytesRange::from(range);

    SeekableReader {
        acc: o.accessor(),
        path: o.path().to_string(),
        offset: br.offset(),
        size: br.size(),

        pos: 0,
        state: State::Idle,
    }
}

/// SeekableReader implement `AsyncRead` and `AsyncSeek`.
pub struct SeekableReader {
    acc: Arc<dyn Accessor>,
    path: String,
    offset: Option<u64>,
    size: Option<u64>,

    pos: u64,
    state: State,
}

enum State {
    Idle,
    Sending(BoxFuture<'static, Result<(RpRead, OutputBytesReader)>>),
    Seeking(BoxFuture<'static, Result<RpStat>>),
    Reading(OutputBytesReader),
}

impl SeekableReader {
    fn current_offset(&self) -> u64 {
        self.offset.unwrap_or_default() + self.pos
    }

    fn current_size(&self) -> Option<u64> {
        self.size.map(|v| v - self.pos)
    }
}

impl AsyncRead for SeekableReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.state {
            State::Idle => {
                let acc = self.acc.clone();
                let path = self.path.clone();
                let op = OpRead::default().with_range(BytesRange::new(
                    Some(self.current_offset()),
                    self.current_size(),
                ));

                let future = async move { acc.read(&path, op).await };

                self.state = State::Sending(Box::pin(future));
                self.poll_read(cx, buf)
            }
            State::Sending(future) => {
                let (_, r) = ready!(Pin::new(future).poll(cx))?;
                self.state = State::Reading(r);
                self.poll_read(cx, buf)
            }
            State::Reading(r) => match ready!(Pin::new(r).poll_read(cx, buf)) {
                Ok(n) => {
                    self.pos += n as u64;
                    Poll::Ready(Ok(n))
                }
                Err(e) => Poll::Ready(Err(e)),
            },
            _ => unreachable!("read while seeking is invalid"),
        }
    }
}

impl AsyncSeek for SeekableReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        if let State::Seeking(future) = &mut self.state {
            let meta = ready!(Pin::new(future).poll(cx))?.into_metadata();
            self.size = Some(meta.content_length() - self.offset.unwrap_or_default())
        }

        let cur = self.pos as i64;
        let cur = match pos {
            SeekFrom::Start(off) => off as i64,
            SeekFrom::Current(off) => cur + off,
            SeekFrom::End(off) => {
                // Stat the object to get it's content-length.
                if self.size.is_none() {
                    let acc = self.acc.clone();
                    let path = self.path.clone();

                    let future = async move { acc.stat(&path, OpStat::new()).await };

                    self.state = State::Seeking(Box::pin(future));
                    return self.poll_seek(cx, pos);
                }

                let total_size = self.size.expect("must have valid total_size");

                total_size as i64 + off
            }
        };

        self.pos = cur as u64;

        self.state = State::Idle;
        Poll::Ready(Ok(self.pos))
    }
}

#[cfg(test)]
mod tests {
    use std::io::SeekFrom;
    use std::str::from_utf8;

    use anyhow::Result;
    use futures::AsyncReadExt;
    use futures::AsyncSeekExt;

    use super::*;
    use crate::Operator;
    use crate::Scheme;

    #[tokio::test]
    async fn test_reader() -> Result<()> {
        let f = Operator::from_env(Scheme::Fs)?;

        let path = format!("/tmp/{}", uuid::Uuid::new_v4());

        // Create a test file.
        f.object(&path).write("Hello, world!").await.unwrap();

        let o = f.object(&path);
        let mut r = seekable_read(&o, ..);

        // Seek to offset 3.
        let n = r.seek(SeekFrom::Start(3)).await?;
        assert_eq!(n, 3);

        // Read only one byte.
        let mut bs = Vec::new();
        bs.resize(1, 0);
        let n = r.read(&mut bs).await?;
        assert_eq!("l", from_utf8(&bs).unwrap());
        assert_eq!(n, 1);
        let n = r.seek(SeekFrom::Current(0)).await?;
        assert_eq!(n, 4);

        // Seek to end.
        let n = r.seek(SeekFrom::End(-1)).await?;
        assert_eq!(n, 12);

        // Read only one byte.
        let mut bs = Vec::new();
        bs.resize(1, 0);
        let n = r.read(&mut bs).await?;
        assert_eq!("!", from_utf8(&bs)?);
        assert_eq!(n, 1);
        let n = r.seek(SeekFrom::Current(0)).await?;
        assert_eq!(n, 13);

        Ok(())
    }
}
