// Copyright 2022 Datafuse Labs
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
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use bytes::BytesMut;
use futures::ready;
use futures::Stream;
use pin_project::pin_project;

use crate::raw::*;

/// Convert [`input::Read`] into [`input::Stream`].
///
/// # Note
///
/// This conversion is **not zero cost**.
///
/// # Example
///
/// ```rust
/// use opendal::raw::input::into_stream;
/// # use std::io::Result;
/// # use futures::io;
/// # use bytes::Bytes;
/// # use futures::StreamExt;
/// # use futures::SinkExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let r = io::Cursor::new(vec![0; 1024]);
/// let mut s = into_stream(r, 8 * 1024);
/// s.next().await;
/// # Ok(())
/// # }
/// ```
pub fn into_stream<R: input::Read>(r: R, capacity: usize) -> IntoStream<R> {
    IntoStream {
        r,
        cap: capacity,
        buf: BytesMut::with_capacity(capacity),
    }
}

#[pin_project]
pub struct IntoStream<R: input::Read> {
    #[pin]
    r: R,
    cap: usize,
    buf: BytesMut,
}

/// IntoStream will be accessed uniquely, not concurrent read will happen.
///
/// No `get_inner`, no `Clone`, no other ways to access internally fields.
unsafe impl<R: input::Read> Sync for IntoStream<R> {}

impl<R> Stream for IntoStream<R>
where
    R: input::Read,
{
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if this.buf.is_empty() {
            // Reserve with the given cap every time.
            this.buf.reserve(*this.cap);
            // # Safety
            //
            // We will make sure that only valid content will be returned
            // after write by calling `this.buf.split_to(n)`.
            unsafe {
                this.buf.set_len(*this.cap);
            }
        }

        match ready!(this.r.poll_read(cx, this.buf)) {
            Err(err) => Poll::Ready(Some(Err(err))),
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
