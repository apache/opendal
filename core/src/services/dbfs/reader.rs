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

use std::cmp;
use std::io::SeekFrom;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use base64::engine::general_purpose;
use base64::Engine;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures::future::BoxFuture;
use http::Response;
use serde::Deserialize;

use super::core::DbfsCore;

use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::*;

const DBFS_READ_LIMIT: usize = 1048576;

pub struct DbfsReader {
    state: State,
    path: String,
    offset: u64,
    buffer: BytesMut,
}

impl DbfsReader {
    pub fn new(core: Arc<DbfsCore>, op: OpRead, path: String, content_length: u64) -> Self {
        DbfsReader {
            state: State::Idle(Some(core)),
            path,
            offset: op.range().offset().unwrap_or(0),
            buffer: BytesMut::with_capacity(content_length as usize),
        }
    }

    #[inline]
    fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    fn serde_json_decode(&self, bs: &Bytes) -> Result<Bytes> {
        let mut response_body = match serde_json::from_slice::<ReadContentJsonResponse>(bs) {
            Ok(v) => v,
            Err(err) => {
                return Err(
                    Error::new(ErrorKind::Unexpected, "parse response content failed")
                        .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                        .set_source(err),
                );
            }
        };

        response_body.data = general_purpose::STANDARD
            .decode(response_body.data)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "decode response content failed")
                    .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                    .set_source(err)
            })
            .and_then(|v| {
                String::from_utf8(v).map_err(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "response data contains invalid utf8 bytes",
                    )
                    .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                    .set_source(err)
                })
            })?;

        Ok(response_body.data.into())
    }
}

enum State {
    Idle(Option<Arc<DbfsCore>>),
    Read(BoxFuture<'static, (Arc<DbfsCore>, Result<Response<IncomingAsyncBody>>)>),
    Decode(BoxFuture<'static, (Arc<DbfsCore>, Result<Bytes>)>),
}

/// # Safety
///
/// We will only take `&mut Self` reference for DbfsReader.
unsafe impl Sync for DbfsReader {}

#[async_trait]
impl oio::Read for DbfsReader {
    fn poll_read(&mut self, cx: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<Result<usize>> {
        while self.buffer.remaining() != buf.len() {
            match &mut self.state {
                State::Idle(core) => {
                    let core = core.take().expect("DbfsReader must be initialized");

                    let path = self.path.clone();
                    let offset = self.offset;
                    let len = cmp::min(buf.len(), DBFS_READ_LIMIT);

                    let fut = async move {
                        let resp = async { core.dbfs_read(&path, offset, len as u64).await }.await;
                        (core, resp)
                    };
                    self.state = State::Read(Box::pin(fut));
                }
                State::Read(fut) => {
                    let (core, resp) = ready!(fut.as_mut().poll(cx));
                    let body = resp?.into_body();

                    let fut = async move {
                        let bs = async { body.bytes().await }.await;
                        (core, bs)
                    };
                    self.state = State::Decode(Box::pin(fut));
                }
                State::Decode(fut) => {
                    let (core, bs) = ready!(fut.as_mut().poll(cx));
                    let data = self.serde_json_decode(&bs?)?;

                    self.buffer.put_slice(&data[..]);
                    self.set_offset(self.offset + data.len() as u64);
                    self.state = State::Idle(Some(core));
                }
            }
        }
        buf.put_slice(&self.buffer[..]);
        Poll::Ready(Ok(self.buffer.remaining()))
    }

    fn poll_seek(&mut self, _cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        // TODO: drop existing buf and change the offset?
        match pos {
            SeekFrom::Start(n) => {
                self.set_offset(n);
            }
            SeekFrom::End(n) => {
                self.set_offset((self.buffer.remaining() as i64 + n) as u64);
            }
            SeekFrom::Current(n) => {
                self.set_offset((self.offset as i64 + n) as u64);
            }
        };
        Poll::Ready(Ok(0))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support iterating",
        ))))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ReadContentJsonResponse {
    bytes_read: u64,
    data: String,
}
