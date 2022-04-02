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

use futures::AsyncRead;
use pin_project::pin_project;

use crate::BytesReader;

/// Create an observer over BytesReader.
///
/// `observe_read` will accept a `FnMut(ReadEvent)` which handles
/// [`ReadEvent`] triggered by [`ReadObserver`]
///
/// # Example
///
/// ```rust
/// use opendal::io_util::observe_read;
/// use opendal::io_util::ReadEvent;
/// # use opendal::io_util::into_stream;
/// # use std::io::Result;
/// # use futures::io;
/// # use bytes::Bytes;
/// # use futures::StreamExt;
/// # use futures::AsyncWriteExt;
/// # use futures::SinkExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let r = Box::new(io::Cursor::new(vec![0; 1024]));
/// let mut read_size = 0;
/// let mut s = observe_read(r, |e| match e {
///     ReadEvent::Read(n) => read_size += n,
///     _ => {}
/// });
/// io::copy(s, &mut io::sink()).await?;
/// # Ok(())
/// # }
/// ```
pub fn observe_read<F: FnMut(ReadEvent)>(s: BytesReader, f: F) -> ReadObserver<F> {
    ReadObserver { s, f }
}

/// Event that sent by [`ReadObserver`], should be handled via
/// `FnMut(ReadEvent)`.
pub enum ReadEvent {
    /// Emit while meeting `Poll::Pending`.
    Pending,
    /// Emit the sent bytes length while `poll_read` got `Poll::Ready(Ok(n))`.
    Read(usize),
    /// Emit while meeting `Poll::Ready(Ok(0))`.
    Terminated,
    /// Emit the error kind while meeting error.
    ///
    /// # Note
    ///
    /// We only emit the error kind here so that we don't need clone the whole error.
    Error(ErrorKind),
}

/// Observer that created via [`observe_stream`].
#[pin_project]
pub struct ReadObserver<F: FnMut(ReadEvent)> {
    s: BytesReader,
    f: F,
}

impl<F> AsyncRead for ReadObserver<F>
where
    F: FnMut(ReadEvent),
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        match Pin::new(&mut self.s).poll_read(cx, buf) {
            Poll::Ready(Ok(0)) => {
                (self.f)(ReadEvent::Terminated);
                Poll::Ready(Ok(0))
            }
            Poll::Ready(Ok(n)) => {
                (self.f)(ReadEvent::Read(n));
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => {
                (self.f)(ReadEvent::Error(e.kind()));
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                (self.f)(ReadEvent::Pending);
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::io;
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use super::*;

    #[tokio::test]
    async fn test_read_observer() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let r = io::Cursor::new(content.clone());

        let mut read_size = 0;
        let mut is_terminated = false;
        let s = observe_read(Box::new(r), |e| match e {
            ReadEvent::Read(n) => read_size += n,
            ReadEvent::Terminated => is_terminated = true,
            _ => {}
        });

        io::copy(s, &mut io::sink())
            .await
            .expect("copy must succeed");

        assert_eq!(read_size, size);
        assert!(is_terminated);
    }
}
