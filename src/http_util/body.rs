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

use crate::io_util::into_stream;
use crate::{BlockingBytesReader, BytesReader};
use bytes::{Buf, Bytes};
use futures::AsyncRead;
use std::cmp::min;
use std::io::Result;
use std::io::{Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Body used in blocking HTTP requests.
pub enum Body {
    Empty,
    #[allow(unused)]
    Bytes(Bytes),
    Reader(BlockingBytesReader),
}

impl Body {
    /// Consume the entire body.
    #[allow(unused)]
    pub fn consume(self) -> Result<()> {
        if let Body::Reader(mut r) = self {
            std::io::copy(&mut r, &mut std::io::sink())?;
        }

        Ok(())
    }
}

impl Read for Body {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize> {
        match self {
            Body::Empty => Ok(0),
            Body::Bytes(bs) => {
                let size = min(bs.len(), buf.len());
                let rbs = bs.split_to(size);
                bs.advance(size);

                buf.write_all(&rbs).expect("write all must succeed");
                Ok(size)
            }
            Body::Reader(r) => r.read(buf),
        }
    }
}

/// Body used in async HTTP requests.
pub enum AsyncBody {
    /// An empty body.
    Empty,
    /// Body with bytes.
    Bytes(Bytes),
    /// Body with a Reader.
    Reader(BytesReader),
}

impl AsyncBody {
    /// Consume the entire body.
    pub async fn consume(self) -> Result<()> {
        use futures::io;

        if let AsyncBody::Reader(r) = self {
            io::copy(r, &mut io::sink()).await?;
        }

        Ok(())
    }

    /// Consume the response to bytes.
    pub async fn bytes(self) -> Result<Bytes> {
        use futures::io;

        match self {
            AsyncBody::Empty => Ok(Bytes::new()),
            AsyncBody::Bytes(bs) => Ok(bs),
            AsyncBody::Reader(r) => {
                let mut w = io::Cursor::new(Vec::with_capacity(1024));
                io::copy(r, &mut w).await?;
                Ok(Bytes::from(w.into_inner()))
            }
        }
    }

    /// Consume the response to build a reader.
    pub fn reader(self) -> BytesReader {
        use futures::io::Cursor;

        match self {
            AsyncBody::Empty => Box::new(Cursor::new(vec![])),
            AsyncBody::Bytes(bs) => Box::new(Cursor::new(bs.to_vec())),
            AsyncBody::Reader(r) => r,
        }
    }
}

impl From<AsyncBody> for reqwest::Body {
    fn from(v: AsyncBody) -> Self {
        match v {
            AsyncBody::Empty => reqwest::Body::from(""),
            AsyncBody::Bytes(bs) => reqwest::Body::from(bs),
            AsyncBody::Reader(r) => reqwest::Body::wrap_stream(into_stream(r, 8 * 1024)),
        }
    }
}

impl AsyncRead for AsyncBody {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        match self.get_mut() {
            AsyncBody::Empty => Poll::Ready(Ok(0)),
            AsyncBody::Bytes(bs) => {
                let size = min(bs.len(), buf.len());
                let rbs = bs.split_to(size);
                bs.advance(size);

                buf.write_all(&rbs).expect("write all must succeed");
                Poll::Ready(Ok(size))
            }
            AsyncBody::Reader(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}
