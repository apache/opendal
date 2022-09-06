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

use crate::{BlockingBytesReader, BytesReader};
use bytes::Bytes;
use futures::AsyncRead;
use std::io::Read;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum Body {
    Empty,
    #[allow(unused)]
    Bytes(Bytes),
    Reader(BlockingBytesReader),
}

impl Body {
    #[allow(unused)]
    pub fn consume(self) -> Result<()> {
        if let Body::Reader(mut r) = self {
            std::io::copy(&mut r, &mut std::io::sink())?;
        }

        Ok(())
    }
}

impl Read for Body {
    fn read(&mut self, _buf: &mut [u8]) -> Result<usize> {
        todo!()
    }
}

pub enum AsyncBody {
    Empty,
    Bytes(Bytes),
    Reader(BytesReader),
}

impl AsyncBody {
    pub async fn consume(self) -> Result<()> {
        use futures::io;

        if let AsyncBody::Reader(r) = self {
            io::copy(r, &mut io::sink()).await?;
        }

        Ok(())
    }

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

    pub fn reader(self) -> BytesReader {
        use futures::io::Cursor;

        match self {
            AsyncBody::Empty => Box::new(Cursor::new(vec![])),
            AsyncBody::Bytes(bs) => Box::new(Cursor::new(bs.to_vec())),
            AsyncBody::Reader(r) => r,
        }
    }
}

impl Into<reqwest::Body> for AsyncBody {
    fn into(self) -> reqwest::Body {
        todo!()
    }
}

impl AsyncRead for AsyncBody {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        todo!()
    }
}
