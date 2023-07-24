// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures::AsyncRead;
use tokio::io::ReadBuf;

use crate::raw::*;
use crate::*;

// TODO: 64KiB is picked based on experiences, should be configurable
const DEFAULT_CAPACITY: usize = 64 * 1024;

/// Convert given futures reader into [`oio::Stream`].
pub fn into_stream_from_reader<R>(r: R) -> FromReaderStream<R>
where
    R: AsyncRead + Send + Sync + Unpin,
{
    FromReaderStream {
        inner: Some(r),
        buf: BytesMut::new(),
    }
}

pub struct FromReaderStream<R> {
    inner: Option<R>,
    buf: BytesMut,
}

impl<S> oio::Stream for FromReaderStream<S>
where
    S: AsyncRead + Send + Sync + Unpin,
{
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let reader = match self.inner.as_mut() {
            Some(r) => r,
            None => return Poll::Ready(None),
        };

        if self.buf.capacity() == 0 {
            self.buf.reserve(DEFAULT_CAPACITY);
        }

        let dst = self.buf.spare_capacity_mut();
        let mut buf = ReadBuf::uninit(dst);

        // Safety: the buf must contains enough space for reading
        unsafe { buf.assume_init(buf.capacity()) };

        match ready!(Pin::new(reader).poll_read(cx, buf.initialized_mut())) {
            Ok(0) => {
                // Set inner to None while reaching EOF.
                self.inner = None;
                Poll::Ready(None)
            }
            Ok(n) => {
                // Safety: read_exact makes sure this buffer has been filled.
                unsafe { self.buf.advance_mut(n) }

                let chunk = self.buf.split();
                Poll::Ready(Some(Ok(chunk.freeze())))
            }
            Err(err) => Poll::Ready(Some(Err(Error::new(
                ErrorKind::Unexpected,
                "read data from reader into stream",
            )
            .set_temporary()
            .set_source(err)))),
        }
    }
}
