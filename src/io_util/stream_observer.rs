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
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use pin_project::pin_project;

use crate::error::{Kind, Result};
use crate::io::BytesStream;

/// Create an observer over BytesStream.
///
/// `observe_stream` will accept a `FnMut(StreamEvent)` which handles
/// [`StreamEvent`] triggered by [`StreamObserver`]
///
/// # Example
///
/// ```rust
/// use opendal::io_util::observe_stream;
/// use opendal::io_util::StreamEvent;
/// # use opendal::io_util::into_stream;
/// # use opendal::error::Result;
/// # use futures::io;
/// # use bytes::Bytes;
/// # use futures::StreamExt;
/// # use futures::AsyncWriteExt;
/// # use futures::SinkExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let stream = Box::new(into_stream(io::Cursor::new(vec![0; 1024]), 1024));
/// let mut read_size = 0;
/// let mut s = observe_stream(stream, |e| match e {
///     StreamEvent::Next(n) => read_size += n,
///     _ => {}
/// });
/// s.next().await;
/// # Ok(())
/// # }
/// ```
pub fn observe_stream<F: FnMut(StreamEvent)>(s: BytesStream, f: F) -> StreamObserver<F> {
    StreamObserver { s, f }
}

/// Event that sent by [`StreamObserver`], should be handled via
/// `FnMut(StreamEvent)`.
pub enum StreamEvent {
    /// Emit while meeting `Poll::Pending`.
    Pending,
    /// Emit the sent bytes length while `poll_next` got `Poll::Ready(Some(Ok(_)))`.
    Next(usize),
    /// Emit while meeting `Poll::Ok(None)`.
    Terminated,
    /// Emit the error kind while meeting error.
    ///
    /// # Note
    ///
    /// We only emit the error kind here so that we don't need clone the whole error.
    Error(Kind),
}

/// Observer that created via [`observe_stream`].
#[pin_project]
pub struct StreamObserver<F: FnMut(StreamEvent)> {
    s: BytesStream,
    f: F,
}

impl<F> Stream for StreamObserver<F>
where
    F: FnMut(StreamEvent),
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.s).poll_next(cx) {
            Poll::Ready(Some(Ok(bs))) => {
                (self.f)(StreamEvent::Next(bs.len()));
                Poll::Ready(Some(Ok(bs)))
            }
            Poll::Ready(Some(Err(e))) => {
                (self.f)(StreamEvent::Error(e.kind()));
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                (self.f)(StreamEvent::Terminated);
                Poll::Ready(None)
            }
            Poll::Pending => {
                (self.f)(StreamEvent::Pending);
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::io_util::into_stream;
    use futures::io;
    use futures::StreamExt;
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use super::*;

    #[tokio::test]
    async fn test_stream_observer() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let r = io::Cursor::new(content.clone());
        let s = into_stream(r, 1024);

        let mut stream_size = 0;
        let mut is_terminated = false;
        let s = observe_stream(Box::new(s), |e| match e {
            StreamEvent::Next(n) => stream_size += n,
            StreamEvent::Terminated => is_terminated = true,
            _ => {}
        });

        let _ = s.collect::<Vec<_>>().await;

        assert_eq!(stream_size, size);
        assert!(is_terminated);
    }
}
