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

use bytes::Bytes;
use futures::Sink;
use pin_project::pin_project;

use crate::error::Error;
use crate::error::Kind;
use crate::error::Result;
use crate::io::BytesSinker;

/// Create an observer over BytesSink.
///
/// `observe_sink` will accept a `FnMut(SinkEvent)` which handles [`SinkEvent`]
/// triggered by [`SinkObserver`].
///
/// # Example
///
/// ```rust
/// use opendal::io_util::observe_sink;
/// use opendal::io_util::SinkEvent;
/// # use opendal::io_util::into_sink;
/// # use opendal::error::Result;
/// # use futures::io;
/// # use bytes::Bytes;
/// # use futures::StreamExt;
/// # use futures::AsyncWriteExt;
/// # use futures::SinkExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let sink = Box::new(into_sink(Vec::new()));
/// let mut written_size = 0;
/// let mut s = observe_sink(sink, |e| match e {
///     SinkEvent::Sent(n) => written_size += n,
///     _ => {}
/// });
/// s.send(Bytes::from(vec![0; 1024])).await?;
/// s.close().await?;
/// # Ok(())
/// # }
/// ```
pub fn observe_sink<F: FnMut(SinkEvent)>(s: BytesSinker, f: F) -> SinkObserver<F> {
    SinkObserver { s, f }
}

/// Event that sent by [`SinkObserver`], should be handled via `FnMut(SinkEvent)`.
#[derive(Copy, Clone, Debug)]
pub enum SinkEvent {
    /// Emit while meeting `Poll::Pending`.
    Pending,
    /// Emit while `poll_ready` got `Poll::Ready(Ok(_))`.
    Ready,
    /// Emit the sent bytes length while `start_send` got `Ok(_)`.
    Sent(usize),
    /// Emit while `poll_flush` got `Poll::Ready(Ok(_))`.
    Flushed,
    /// Emit while `poll_flush` got `Poll::Ready(Ok(_))`.
    Closed,
    /// Emit the error kind while meeting error.
    ///
    /// # Note
    ///
    /// We only emit the error kind here so that we don't need clone the whole error.
    Error(Kind),
}

/// Observer that created via [`observe_sink`].
#[pin_project]
pub struct SinkObserver<F: FnMut(SinkEvent)> {
    s: BytesSinker,
    f: F,
}

impl<F> Sink<Bytes> for SinkObserver<F>
where
    F: FnMut(SinkEvent),
{
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match Pin::new(&mut self.s).poll_ready(cx) {
            Poll::Ready(Ok(_)) => {
                (self.f)(SinkEvent::Ready);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                (self.f)(SinkEvent::Error(e.kind()));
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                (self.f)(SinkEvent::Pending);
                Poll::Pending
            }
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<()> {
        let size = item.len();
        match Pin::new(&mut self.s).start_send(item) {
            Ok(_) => {
                (self.f)(SinkEvent::Sent(size));
                Ok(())
            }
            Err(e) => {
                (self.f)(SinkEvent::Error(e.kind()));
                Err(e)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match Pin::new(&mut self.s).poll_flush(cx) {
            Poll::Ready(Ok(_)) => {
                (self.f)(SinkEvent::Flushed);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                (self.f)(SinkEvent::Error(e.kind()));
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                (self.f)(SinkEvent::Pending);
                Poll::Pending
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match Pin::new(&mut self.s).poll_close(cx) {
            Poll::Ready(Ok(_)) => {
                (self.f)(SinkEvent::Closed);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                (self.f)(SinkEvent::Error(e.kind()));
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                (self.f)(SinkEvent::Pending);
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::SinkExt;
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use super::*;
    use crate::io_util::into_sink;

    #[tokio::test]
    async fn test_sink_observer() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let s = into_sink(Vec::new());

        let mut sink_size = 0;
        let mut is_closed = false;
        let mut s = observe_sink(Box::new(s), |e| match e {
            SinkEvent::Sent(n) => sink_size += n,
            SinkEvent::Closed => is_closed = true,
            _ => {}
        });

        s.send(Bytes::from(content.clone()))
            .await
            .expect("send must success");
        s.close().await.expect("close must success");

        assert_eq!(sink_size, size);
        assert!(is_closed);
    }
}
