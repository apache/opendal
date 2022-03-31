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
use futures::Sink;
use pin_project::pin_project;

use crate::error::{Error, Kind, Result};
use crate::io::BytesSink;

pub fn observe_sink<F: FnMut(SinkEvent)>(s: BytesSink, f: F) -> SinkObserver<F> {
    SinkObserver::new(s, f)
}

pub enum SinkEvent {
    Pending,
    Ready,
    Sent(usize),
    Flushed,
    Closed,
    Error(Kind),
}

#[pin_project]
pub struct SinkObserver<F: FnMut(SinkEvent)> {
    s: BytesSink,
    f: F,
}

impl<F> SinkObserver<F>
where
    F: FnMut(SinkEvent),
{
    pub fn new(s: BytesSink, f: F) -> Self {
        Self { s, f }
    }
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
    use crate::io_util::into_sink;
    use futures::SinkExt;
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use super::*;

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
        let mut s = SinkObserver::new(Box::new(s), |e| match e {
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
