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

pub fn observe_stream<F: FnMut(StreamEvent)>(s: BytesStream, f: F) -> StreamObserver<F> {
    StreamObserver::new(s, f)
}

pub enum StreamEvent {
    Pending,
    Next(usize),
    Terminated,
    Error(Kind),
}

#[pin_project]
pub struct StreamObserver<F: FnMut(StreamEvent)> {
    s: BytesStream,
    f: F,
}

impl<F> StreamObserver<F>
where
    F: FnMut(StreamEvent),
{
    pub fn new(s: BytesStream, f: F) -> Self {
        Self { s, f }
    }
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
        let s = StreamObserver::new(Box::new(s), |e| match e {
            StreamEvent::Next(n) => stream_size += n,
            StreamEvent::Terminated => is_terminated = true,
            _ => {}
        });

        let _ = s.collect::<Vec<_>>().await;

        assert_eq!(stream_size, size);
        assert!(is_terminated);
    }
}
