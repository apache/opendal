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

use std::io;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bb8::PooledConnection;
use futures::future::BoxFuture;
use futures::AsyncRead;
use futures::FutureExt;
use suppaftp::Status;

use super::backend::Manager;
use crate::raw::*;
use crate::services::ftp::err::parse_error;
use crate::*;

/// Wrapper for ftp data stream and command stream.
pub struct FtpReader {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    state: State,
}

unsafe impl Sync for FtpReader {}

pub enum State {
    Reading(Option<PooledConnection<'static, Manager>>),
    Finalize(BoxFuture<'static, Result<()>>),
}

impl FtpReader {
    /// Create an instance of FtpReader.
    pub fn new(
        r: Box<dyn AsyncRead + Send + Unpin>,
        c: PooledConnection<'static, Manager>,
    ) -> Self {
        Self {
            reader: r,
            state: State::Reading(Some(c)),
        }
    }
}

impl oio::Read for FtpReader {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let data = Pin::new(&mut self.reader).poll_read(cx, buf);

        match &mut self.state {
            // Reading state, try to poll some data.
            State::Reading(stream) => {
                match stream {
                    Some(_) => {
                        // when hit Err or EOF, consume ftpstream, change state to Finalize and send fut.
                        if let Poll::Ready(Err(_)) | Poll::Ready(Ok(0)) = data {
                            let mut ft = stream.take().unwrap();

                            let fut = async move {
                                ft.read_response_in(&[
                                    Status::ClosingDataConnection,
                                    Status::RequestedFileActionOk,
                                ])
                                .await
                                .map_err(parse_error)?;

                                Ok(())
                            };
                            self.state = State::Finalize(Box::pin(fut));
                        } else {
                            // Otherwise, exit and return data.
                            return data.map_err(|err| {
                                Error::new(ErrorKind::Unexpected, "read data from ftp data stream")
                                    .set_source(err)
                            });
                        }

                        self.poll_read(cx, buf)
                    }
                    // We could never reach this branch because we will change to Finalize state once we consume ftp stream.
                    None => unreachable!(),
                }
            }

            // Finalize state, wait for finalization of stream.
            State::Finalize(fut) => match ready!(Pin::new(fut).poll_unpin(cx)) {
                Ok(_) => Poll::Ready(Ok(0)),
                Err(e) => Poll::Ready(Err(e)),
            },
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        let (_, _) = (cx, pos);

        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        )))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<bytes::Bytes>>> {
        let _ = cx;

        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        ))))
    }
}
