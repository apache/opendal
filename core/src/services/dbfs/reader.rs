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

use base64::engine::general_purpose;
use base64::Engine;
use bytes::BufMut;
use bytes::Bytes;
use futures::future::BoxFuture;
use serde::Deserialize;

use super::core::DbfsCore;
use crate::raw::*;
use crate::*;

// The number of bytes to read starting from the offset. This has a limit of 1 MB
// Reference: https://docs.databricks.com/api/azure/workspace/dbfs/read
const DBFS_READ_LIMIT: usize = 1024 * 1024;

pub struct DbfsReader {
    state: State,
    path: String,
    offset: u64,
    has_filled: u64,
}

impl DbfsReader {
    pub fn new(core: Arc<DbfsCore>, op: OpRead, path: String) -> Self {
        DbfsReader {
            state: State::Reading(Some(core)),
            path,
            offset: op.range().offset().unwrap_or(0),
            has_filled: 0,
        }
    }

    #[inline]
    fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    fn serde_json_decode(&self, bs: &Bytes) -> Result<Bytes> {
        let response_body = match serde_json::from_slice::<ReadContentJsonResponse>(bs) {
            Ok(v) => v,
            Err(err) => {
                return Err(
                    Error::new(ErrorKind::Unexpected, "parse response content failed")
                        .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                        .set_source(err),
                );
            }
        };

        let decoded_data = general_purpose::STANDARD
            .decode(response_body.data)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "decode response content failed")
                    .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                    .set_source(err)
            })?;

        Ok(decoded_data.into())
    }
}

enum State {
    Reading(Option<Arc<DbfsCore>>),
    Finalize(BoxFuture<'static, (Arc<DbfsCore>, Result<Bytes>)>),
}

/// # Safety
///
/// We will only take `&mut Self` reference for DbfsReader.
unsafe impl Sync for DbfsReader {}

impl oio::Read for DbfsReader {
    fn poll_read(&mut self, cx: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<Result<usize>> {
        while self.has_filled as usize != buf.len() {
            match &mut self.state {
                State::Reading(core) => {
                    let core = core.take().expect("DbfsReader must be initialized");

                    let path = self.path.clone();
                    let offset = self.offset;
                    let len = cmp::min(buf.len(), DBFS_READ_LIMIT);

                    let fut = async move {
                        let resp = async { core.dbfs_read(&path, offset, len as u64).await }.await;
                        let body = match resp {
                            Ok(resp) => resp.into_body(),
                            Err(err) => {
                                return (core, Err(err));
                            }
                        };
                        let bs = async { body.bytes().await }.await;
                        (core, bs)
                    };
                    self.state = State::Finalize(Box::pin(fut));
                }
                State::Finalize(fut) => {
                    let (core, bs) = ready!(fut.as_mut().poll(cx));
                    let data = self.serde_json_decode(&bs?)?;

                    buf.put_slice(&data[..]);
                    self.set_offset(self.offset + data.len() as u64);
                    self.has_filled += data.len() as u64;
                    self.state = State::Reading(Some(core));
                }
            }
        }
        Poll::Ready(Ok(self.has_filled as usize))
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let (_, _) = (cx, pos);

        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        )))
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
