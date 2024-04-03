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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::cmp::Ordering;

use crate::raw::oio::WritableBuf;
use crate::raw::*;
use crate::*;

/// Body used in async HTTP requests.
#[derive(Default)]
pub enum RequestBody {
    /// An empty body.
    #[default]
    Empty,
    /// Body with bytes.
    Bytes(Bytes),
    /// Body with stream.
    ///
    /// TODO: remove this variant once by adopting oio::Buffer in writing.
    Stream(oio::Streamer),
}

#[must_use]
pub struct ResponseBody<B: Stream<Item = Result<Bytes>>> {
    inner: B,
    size: Option<usize>,
    consumed: usize,
    checked: bool,
}

/// Check if the body has been consumed while debug_assertions
/// enabled. This code will never be executed in release mode.
#[cfg(debug_assertions)]
impl<B: Stream<Item = Result<Bytes>>> Drop for ResponseBody<B> {
    fn drop(&mut self) {
        if !self.checked {
            log::warn!("http body has not been consumed, please report as a bug.")
        }
    }
}

impl<B: Stream<Item = Result<Bytes>>> ResponseBody<B> {
    pub fn new(inner: B) -> Self {
        let size = inner.size_hint().1;

        Self {
            inner,
            size,
            consumed: 0,
            checked: false,
        }
    }

    /// Check body's size and consumed to make sure we have read all data.
    #[inline]
    fn check(&mut self) -> Result<()> {
        // Set checked to true.
        debug_assert!(
            self.checked == false,
            "http body has been checked, please report as a bug."
        );
        self.checked = true;

        // Skip size check if the size is unknown.
        let Some(size) = self.size else { return Ok(()) };

        let err = match self.consumed.cmp(&size) {
            Ordering::Equal => return Ok(()),
            Ordering::Less => Error::new(
                ErrorKind::ContentIncomplete,
                "http body got too little data",
            ),
            Ordering::Greater => {
                Error::new(ErrorKind::ContentTruncated, "http body got too much data")
            }
        };

        Err(err
            .with_context("expect", size.to_string())
            .with_context("actual", self.consumed.to_string())
            .set_temporary())
    }

    /// Read all bytes from the response and drop them immediately.
    ///
    /// This function exists to consume the response body and allowing clients
    /// to reuse the same connection.
    pub async fn consume(mut self) -> Result<()> {
        while let Some(bs) = self.inner.next().await {
            let bs = bs.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "fetch bytes from stream")
                    .with_operation("http_util::ResponseBody::consume")
                    .set_source(err)
            })?;
            self.consumed += bs.len();
        }

        self.check()?;
        Ok(())
    }

    /// Read all bytes from the response and write them to the given buffer.
    ///
    /// # Panics
    ///
    /// The input buf must have enough space to store all bytes.
    pub async fn read(mut self, mut buf: WritableBuf) -> Result<usize> {
        while let Some(bs) = self.inner.next().await {
            let bs = bs?;
            self.consumed += bs.remaining();
            buf.put(bs);
        }

        self.check()?;
        Ok(self.consumed as usize)
    }

    /// Read all bytes from the response to buffer.
    pub async fn to_buffer(mut self) -> Result<oio::Buffer> {
        let mut buf = Vec::new();
        while let Some(bs) = self.inner.next().await {
            let bs = bs?;
            self.consumed += bs.remaining();
            buf.push(bs);
        }

        self.check()?;
        Ok(oio::Buffer::from(buf))
    }

    /// Read all bytes from the response to bytes.
    pub async fn to_bytes(mut self) -> Result<Bytes> {
        let mut buf = self.to_buffer().await?;
        Ok(buf.copy_to_bytes(buf.remaining()))
    }

    /// Read all bytes from the response to json.
    pub async fn to_json<T: DeserializeOwned>(self) -> Result<T> {
        let bs = self.to_buffer().await?;
        serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)
    }

    /// Read all bytes from the response to xml.
    pub async fn to_xml<T: DeserializeOwned>(self) -> Result<T> {
        let bs = self.to_buffer().await?;
        quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)
    }
}
