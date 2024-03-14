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

use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;

use super::core::AlluxioCore;
use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::*;

pub type AlluxioWriters = AlluxioWriter;

pub struct AlluxioWriter {
    state: State,

    _op: OpWrite,
    path: String,
    stream_id: Option<u64>,
}

enum State {
    Idle(Option<Arc<AlluxioCore>>),
    Init(BoxFuture<'static, (Arc<AlluxioCore>, Result<u64>)>),
    Write(BoxFuture<'static, (Arc<AlluxioCore>, Result<usize>)>),
    Close(BoxFuture<'static, (Arc<AlluxioCore>, Result<()>)>),
}

impl AlluxioWriter {
    pub fn new(core: Arc<AlluxioCore>, _op: OpWrite, path: String) -> Self {
        AlluxioWriter {
            state: State::Idle(Some(core)),
            _op,
            path,
            stream_id: None,
        }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl Sync for State {}

#[async_trait]
impl oio::Write for AlluxioWriter {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: Bytes) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(w) => match self.stream_id.as_ref() {
                    Some(stream_id) => {
                        let cb = oio::ChunkedBytes::from_vec(vec![bs.clone()]);

                        let stream_id = *stream_id;

                        let w = w.take().expect("writer must be valid");

                        self.state = State::Write(Box::pin(async move {
                            let part = w.write(stream_id, AsyncBody::ChunkedBytes(cb)).await;

                            (w, part)
                        }));
                    }
                    None => {
                        let path = self.path.clone();
                        let w = w.take().expect("writer must be valid");
                        self.state = State::Init(Box::pin(async move {
                            let upload_id = w.create_file(&path).await;
                            (w, upload_id)
                        }));
                    }
                },
                State::Init(fut) => {
                    let (w, stream_id) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));
                    self.stream_id = Some(stream_id?);
                }
                State::Write(fut) => {
                    let (w, part) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));
                    return Poll::Ready(Ok(part?));
                }
                State::Close(_) => {
                    unreachable!("MultipartWriter must not go into State::Close during poll_write")
                }
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(w) => {
                    let w = w.take().expect("writer must be valid");
                    match self.stream_id {
                        Some(stream_id) => {
                            self.state = State::Close(Box::pin(async move {
                                let res = w.close(stream_id).await;
                                (w, res)
                            }));
                        }
                        None => {
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                State::Close(fut) => {
                    let (w, res) = futures::ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(w));

                    res?;

                    return Poll::Ready(Ok(()));
                }
                State::Init(_) => {
                    unreachable!("AlluxioWriter must not go into State::Init during poll_close")
                }
                State::Write(_) => unreachable! {
                    "AlluxioWriter must not go into State::Write during poll_close"
                },
            }
        }
    }

    fn poll_abort(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "AlluxioWriter doesn't support abort",
        )))
    }
}
