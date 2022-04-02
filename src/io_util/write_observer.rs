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

use std::io::ErrorKind;
use std::io::Result;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::BytesWriter;

use futures::AsyncWrite;
use pin_project::pin_project;

/// Create an observer over [`crate::BytesWrite`].
///
/// `observe_write` will accept a `FnMut(WriteEvent)` which handles [`WriteEvent`]
/// triggered by [`WriteObserver`].
///
/// # Example
///
/// ```rust
/// use opendal::io_util::observe_write;
/// use opendal::io_util::WriteEvent;
/// # use opendal::io_util::into_sink;
/// # use std::io::Result;
/// # use futures::io;
/// # use bytes::Bytes;
/// # use futures::StreamExt;
/// # use futures::AsyncWriteExt;
/// # use futures::SinkExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let w = Box::new(io::Cursor::new(Vec::new()));
/// let mut written_size = 0;
/// let mut s = observe_write(w, |e| match e {
///     WriteEvent::Written(n) => written_size += n,
///     _ => {}
/// });
/// s.write_all(&vec![0;1024]).await?;
/// s.close().await?;
/// # Ok(())
/// # }
/// ```
pub fn observe_write<F: FnMut(WriteEvent)>(s: BytesWriter, f: F) -> WriteObserver<F> {
    WriteObserver { s, f }
}

/// Event that sent by [`WriteObserver`], should be handled via `FnMut(WriteEvent)`.
#[derive(Copy, Clone, Debug)]
pub enum WriteEvent {
    /// Emit while meeting `Poll::Pending`.
    Pending,
    /// Emit the sent bytes length while `poll_write` got `Poll::Ready(Ok(_))`.
    Written(usize),
    /// Emit while `poll_flush` got `Poll::Ready(Ok(_))`.
    Flushed,
    /// Emit while `poll_flush` got `Poll::Ready(Ok(_))`.
    Closed,
    /// Emit the error kind while meeting error.
    ///
    /// # Note
    ///
    /// We only emit the error kind here so that we don't need clone the whole error.
    Error(ErrorKind),
}

/// Observer that created via [`observe_write`].
#[pin_project]
pub struct WriteObserver<F: FnMut(WriteEvent)> {
    s: BytesWriter,
    f: F,
}

impl<F> AsyncWrite for WriteObserver<F>
where
    F: FnMut(WriteEvent),
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        match Pin::new(&mut self.s).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                (self.f)(WriteEvent::Written(n));
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => {
                (self.f)(WriteEvent::Error(e.kind()));
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                (self.f)(WriteEvent::Pending);
                Poll::Pending
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match Pin::new(&mut self.s).poll_flush(cx) {
            Poll::Ready(Ok(_)) => {
                (self.f)(WriteEvent::Flushed);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                (self.f)(WriteEvent::Error(e.kind()));
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                (self.f)(WriteEvent::Pending);
                Poll::Pending
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match Pin::new(&mut self.s).poll_close(cx) {
            Poll::Ready(Ok(_)) => {
                (self.f)(WriteEvent::Closed);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                (self.f)(WriteEvent::Error(e.kind()));
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                (self.f)(WriteEvent::Pending);
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{io, AsyncWriteExt};
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use super::*;

    #[tokio::test]
    async fn test_write_observer() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let s = io::Cursor::new(Vec::new());

        let mut written_size = 0;
        let mut is_closed = false;
        let mut s = observe_write(Box::new(s), |e| match e {
            WriteEvent::Written(n) => written_size += n,
            WriteEvent::Closed => is_closed = true,
            _ => {}
        });

        s.write_all(&content).await.expect("write must success");
        s.close().await.expect("close must success");

        assert_eq!(written_size, size);
        assert!(is_closed);
    }
}
