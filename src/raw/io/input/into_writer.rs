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

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::io;
use futures::ready;
use futures::AsyncWrite;

use crate::raw::*;

/// Convert [`input::Sink`] into [`input::Write`].
///
/// # Note
///
/// This conversion is **zero cost**.
///
/// # Example
///
/// ```rust
/// use opendal::raw::input::into_writer;
/// # use opendal::raw::input::into_sink;
/// # use std::io::Result;
/// # use futures::io;
/// # use bytes::Bytes;
/// # use futures::StreamExt;
/// # use futures::AsyncWriteExt;
/// # use futures::SinkExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let sink = into_sink(Vec::new());
/// let mut s = into_writer(sink);
/// s.write(&vec![0; 1024]).await;
/// # Ok(())
/// # }
/// ```
pub fn into_writer<S: input::Sink>(s: S) -> IntoWriter<S> {
    IntoWriter { s }
}

pub struct IntoWriter<S: input::Sink> {
    s: S,
}

impl<S> IntoWriter<S>
where
    S: input::Sink,
{
    pub fn into_inner(self) -> S {
        self.s
    }
}

impl<S> AsyncWrite for IntoWriter<S>
where
    S: input::Sink,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(Pin::new(&mut self.s).poll_ready(cx))?;

        let size = buf.len();
        Pin::new(&mut self.s).start_send(Bytes::copy_from_slice(buf))?;
        Poll::Ready(Ok(size))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.s).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.s).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use super::*;

    #[tokio::test]
    async fn test_into_writer() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let mut r = io::Cursor::new(content.clone());
        let mut w = into_writer(input::into_sink(Vec::new()));
        io::copy(&mut r, &mut w).await.expect("copy must success");

        assert_eq!(w.into_inner().into_inner(), content)
    }
}
