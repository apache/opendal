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

use std::cmp::min;
use std::io::Read;
use std::io::Result;
use std::io::Write;

use bytes::Buf;
use bytes::Bytes;

use crate::io_util::into_stream;
use crate::BlockingBytesReader;
use crate::BytesReader;

/// Body used in blocking HTTP requests.
pub enum Body {
    /// An empty body.
    Empty,
    /// Body with bytes.
    Bytes(Bytes),
    /// Body with a Reader.
    Reader(BlockingBytesReader),
}

impl Default for Body {
    fn default() -> Self {
        Body::Empty
    }
}

impl Body {
    /// Consume the entire body.
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
    /// Body with a multipart field.
    ///
    /// If input with this field, we will goto the internal multipart
    /// handle logic.
    Multipart(String, BytesReader),
}

impl Default for AsyncBody {
    fn default() -> Self {
        AsyncBody::Empty
    }
}

impl From<AsyncBody> for reqwest::Body {
    fn from(v: AsyncBody) -> Self {
        match v {
            AsyncBody::Empty => reqwest::Body::from(""),
            AsyncBody::Bytes(bs) => reqwest::Body::from(bs),
            AsyncBody::Reader(r) => reqwest::Body::wrap_stream(into_stream(r, 16 * 1024)),
            AsyncBody::Multipart(_, _) => {
                unreachable!("reqwest multipart should not be constructed by body")
            }
        }
    }
}

/// IncomingAsyncBody carries the content returned by remote servers.
///
/// # Notes
///
/// Client SHOULD NEVER construct this body.
pub struct IncomingAsyncBody(BytesReader);

impl IncomingAsyncBody {
    /// Construct a new incoming async body
    pub fn new(r: BytesReader) -> Self {
        Self(r)
    }

    /// Consume the entire body.
    pub async fn consume(self) -> Result<()> {
        use futures::io;

        io::copy(self.0, &mut io::sink()).await?;

        Ok(())
    }

    /// Consume the response to bytes.
    pub async fn bytes(self) -> Result<Bytes> {
        use futures::io;

        let mut w = io::Cursor::new(Vec::with_capacity(1024));
        io::copy(self.0, &mut w).await?;
        Ok(Bytes::from(w.into_inner()))
    }

    /// Consume the response to build a reader.
    pub fn reader(self) -> BytesReader {
        self.0
    }
}
