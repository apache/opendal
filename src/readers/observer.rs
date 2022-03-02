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

use futures::AsyncRead;
use pin_project::pin_project;

/// ReadEvent will emitted by `ObserveReader`.
#[derive(Copy, Clone, Debug)]
pub enum ReadEvent {
    /// `poll_read` has been called.
    Started,
    /// `poll_read` returns `Pending`, we are back to the runtime.
    Pending,
    /// `poll_read` returns `Ready(Ok(n))`, we have read `n` bytes of data.
    Read(usize),
    /// `poll_read` returns `Ready(Err(e))`, we will have an `ErrorKind` here.
    Error(std::io::ErrorKind),
}

/// ObserveReader is used to observe state inside Reader.
///
/// We will emit `ReadEvent` in different stages that the inner Reader reach.
/// Caller need to handle `ReadEvent` correctly and quickly.
#[pin_project]
pub struct ObserveReader<R, F: FnMut(ReadEvent)> {
    r: R,
    f: F,
}

impl<R, F> ObserveReader<R, F>
where
    R: AsyncRead + Send + Unpin,
    F: FnMut(ReadEvent),
{
    pub fn new(r: R, f: F) -> Self {
        Self { r, f }
    }
}

impl<R, F> futures::AsyncRead for ObserveReader<R, F>
where
    R: AsyncRead + Send + Unpin,
    F: FnMut(ReadEvent),
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        (self.f)(ReadEvent::Started);

        match Pin::new(&mut self.r).poll_read(cx, buf) {
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
