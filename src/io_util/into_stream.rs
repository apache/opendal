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

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use bytes::Bytes;
use bytes::BytesMut;
use futures::ready;
use futures::AsyncRead;
use futures::Stream;
use pin_project::pin_project;

use crate::error::Error;
use crate::error::Result;

pub fn into_stream<R: AsyncRead + Send + Unpin>(r: R, capacity: usize) -> IntoStream<R> {
    IntoStream {
        r,
        cap: capacity,
        buf: BytesMut::with_capacity(capacity),
    }
}

#[pin_project]
pub struct IntoStream<R: AsyncRead + Send + Unpin> {
    #[pin]
    r: R,
    cap: usize,
    buf: bytes::BytesMut,
}

impl<R> Stream for IntoStream<R>
where
    R: AsyncRead + Send + Unpin,
{
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // TODO: we can adopt MaybeUninit to reduce the extra resize.
        if this.buf.is_empty() {
            this.buf.resize(*this.cap, 0);
        }

        match ready!(this.r.poll_read(cx, this.buf)) {
            Err(err) => Poll::Ready(Some(Err(Error::Unexpected(anyhow!(err))))),
            Ok(0) => Poll::Ready(None),
            Ok(n) => {
                let chunk = this.buf.split_to(n);
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
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

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
