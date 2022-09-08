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

use crate::error::other;
use crate::error::ObjectError;
use crate::ops::Operation;
use crate::BytesReader;
use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::ready;
use futures::AsyncRead;
use futures::FutureExt;
use std::io::Error;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use suppaftp::FtpStream;
use suppaftp::Status;
use tokio::sync::Mutex;

/// Wrapper for ftp data stream and command stream.
pub struct FtpReader {
    reader: BytesReader,
    path: String,
    state: State,
    client: Arc<Mutex<FtpStream>>,
}

pub enum State {
    Active,
    Finalize(BoxFuture<'static, Result<()>>),
    Eof,
    Error(Error),
}

impl FtpReader {
    /// Create an instance of FtpReader.
    pub fn new(r: BytesReader, c: FtpStream, path: &str) -> Self {
        Self {
            reader: r,
            path: path.to_string(),
            state: State::Active,
            client: Arc::new(Mutex::new(c)),
        }
    }
}

impl AsyncRead for FtpReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        match &mut self.state {
            // Active state, try to poll some data.
            State::Active => {
                let data = Pin::new(&mut self.reader).poll_read(cx, buf);

                // when hit Err or EOF, change state to Finalize and send fut.
                if let Poll::Ready(Err(_)) | Poll::Ready(Ok(0)) = data {
                    let c = self.client.clone();
                    let path = self.path.clone();

                    let fut = async move {
                        let mut backend = c.lock().await;

                        backend
                            .read_response_in(&[
                                Status::ClosingDataConnection,
                                Status::RequestedFileActionOk,
                            ])
                            .await
                            .map_err(|e| {
                                other(ObjectError::new(
                                    Operation::Read,
                                    path.as_str(),
                                    anyhow!("unexpected response: {e:?}"),
                                ))
                            })?;

                        backend.quit().await.map_err(|e| {
                            other(ObjectError::new(
                                Operation::Read,
                                path.as_str(),
                                anyhow!("quit request: {e:?}"),
                            ))
                        })?;

                        Ok(())
                    };

                    self.state = State::Finalize(Box::pin(fut));

                // Otherwise, exit and return data.
                } else {
                    return data;
                }

                self.poll_read(cx, buf)
            }
            // Finalize state, wait for finalization of stream. Change state to Eof or Error according to the result of fut.
            State::Finalize(fut) => {
                match ready!(Pin::new(fut).poll_unpin(cx)) {
                    Ok(_) => {
                        self.state = State::Eof;
                    }
                    Err(e) => {
                        self.state = State::Error(e);
                    }
                }

                self.poll_read(cx, buf)
            }
            // Eof state, return 0 byte.
            State::Eof => Poll::Ready(Ok(0)),

            // Error state, return error.
            State::Error(e) => Poll::Ready(Err(Error::new(e.kind(), e.to_string()))),
        }
    }
}
