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

use std::cmp;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use bytes::Bytes;
use futures::ready;
use futures::AsyncRead;
use futures::TryStreamExt;

use crate::BytesStream;

/// Convert [`BytesStream`][crate::BytesStream] into [`BytesRead`][crate::BytesRead].
///
/// # Note
///
/// This conversion is **zero cost**.
///
/// # Example
///
/// ```rust
/// use opendal::io_util::into_reader;
/// # use opendal::error::Result;
/// # use opendal::error::Error;
/// # use futures::io;
/// # use bytes::Bytes;
/// # use futures::StreamExt;
/// # use futures::SinkExt;
/// # use futures::stream;
/// # use futures::AsyncReadExt;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let stream = Box::pin(stream::once(async {
/// #     Ok::<_, Error>(Bytes::from(vec![0; 1024]))
/// # }));
/// let mut s = into_reader(stream);
/// let mut bs = Vec::new();
/// s.read_to_end(&mut bs).await;
/// # Ok(())
/// # }
/// ```
pub fn into_reader<S: BytesStream>(stream: S) -> IntoReader<S> {
    IntoReader {
        stream,
        state: State::PendingChunk,
    }
}

pub struct IntoReader<S: BytesStream> {
    stream: S,
    state: State,
}

enum State {
    Ready(Bytes),
    PendingChunk,
    Eof,
}

impl<S> AsyncRead for IntoReader<S>
where
    S: BytesStream,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            match &mut self.state {
                State::Ready(chunk) => {
                    let len = cmp::min(buf.len(), chunk.len());

                    buf[..len].copy_from_slice(&chunk[..len]);
                    chunk.advance(len);

                    if chunk.is_empty() {
                        self.state = State::PendingChunk;
                    }

                    return Poll::Ready(Ok(len));
                }
                State::PendingChunk => match ready!(self.stream.try_poll_next_unpin(cx)) {
                    Some(Ok(chunk)) => {
                        if !chunk.is_empty() {
                            self.state = State::Ready(chunk);
                        }
                    }
                    Some(Err(err)) => {
                        self.state = State::Eof;
                        return Poll::Ready(Err(err.into()));
                    }
                    None => {
                        self.state = State::Eof;
                        return Poll::Ready(Ok(0));
                    }
                },
                State::Eof => {
                    return Poll::Ready(Ok(0));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use futures::AsyncReadExt;
    use rand::prelude::*;

    use super::*;
    use crate::error::Error;

    #[tokio::test]
    async fn test_into_reader() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let stream_content = content.clone();
        let s = Box::pin(stream::once(async {
            Ok::<_, Error>(Bytes::from(stream_content))
        }));
        let mut r = into_reader(s);

        let mut bs = Vec::new();
        r.read_to_end(&mut bs).await.expect("read must succeed");

        assert_eq!(bs, content)
    }
}
