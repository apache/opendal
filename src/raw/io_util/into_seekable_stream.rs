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

use std::io::Result;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use bytes::BytesMut;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncSeek;
use pin_project::pin_project;
use tokio::io::ReadBuf;

use crate::raw::*;

/// into_seekable_stream will convert a seekable reader into seekable stream.
pub fn into_seekable_stream<R: AsyncRead + AsyncSeek + Unpin + Send>(
    r: R,
    capacity: usize,
) -> IntoStream<R> {
    IntoStream {
        r,
        cap: capacity,
        buf: BytesMut::with_capacity(capacity),
    }
}

#[pin_project]
pub struct IntoStream<R: AsyncRead + AsyncSeek + Unpin + Send> {
    #[pin]
    r: R,
    cap: usize,
    buf: BytesMut,
}

/// IntoStream will be accessed uniquely, not concurrent read will happen.
///
/// No `get_inner`, no `Clone`, no other ways to access internally fields.
unsafe impl<R: AsyncRead + AsyncSeek + Unpin + Send> Sync for IntoStream<R> {}

impl<R> OutputBytesRead for IntoStream<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        Pin::new(&mut self.r).poll_read(cx, buf)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        Pin::new(&mut self.r).poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.buf.is_empty() {
            // Reserve with the given cap every time.
            self.buf.reserve(self.cap);
        }

        let dst = self.buf.spare_capacity_mut();
        let mut buf = ReadBuf::uninit(dst);
        unsafe { buf.assume_init(self.cap) };

        match ready!(Pin::new(&mut self.r).poll_read(cx, buf.initialize_unfilled())) {
            Err(err) => Poll::Ready(Some(Err(err))),
            Ok(0) => Poll::Ready(None),
            Ok(n) => {
                unsafe { self.buf.set_len(n) }
                let chunk = self.buf.split_to(n);
                Poll::Ready(Some(Ok(chunk.freeze())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use futures::io;
    use futures::StreamExt;
    use rand::prelude::*;

    use super::*;

    #[tokio::test]
    async fn test_into_stream() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        // Generate cap between 1B..1MB;
        let cap = rng.gen_range(1..1024 * 1024);

        let r = io::Cursor::new(content.clone());
        let mut s = into_stream(r, cap);

        let mut bs = BytesMut::new();
        while let Some(b) = s.next().await {
            let b = b.expect("read must success");
            bs.put_slice(&b);
        }
        assert_eq!(bs.freeze().to_vec(), content)
    }
}
