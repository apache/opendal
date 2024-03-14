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

use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;

use super::backend::GhacBackend;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct GhacWriter {
    state: State,

    cache_id: i64,
    size: u64,
}

impl GhacWriter {
    pub fn new(backend: GhacBackend, cache_id: i64) -> Self {
        GhacWriter {
            state: State::Idle(Some(backend)),
            cache_id,
            size: 0,
        }
    }
}

enum State {
    Idle(Option<GhacBackend>),
    Upload(BoxFuture<'static, (GhacBackend, Result<usize>)>),
    Commit(BoxFuture<'static, (GhacBackend, Result<()>)>),
}

/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl Sync for State {}

#[async_trait]
impl oio::Write for GhacWriter {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: Bytes) -> Poll<Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(backend) => {
                    let backend = backend.take().expect("GhacWriter must be initialized");

                    let cache_id = self.cache_id;
                    let offset = self.size;
                    let size = bs.len();
                    let bs = bs.clone();

                    let fut = async move {
                        let res = async {
                            let req = backend
                                .ghac_upload(cache_id, offset, size as u64, AsyncBody::Bytes(bs))
                                .await?;

                            let resp = backend.client.send(req).await?;

                            if resp.status().is_success() {
                                resp.into_body().consume().await?;
                                Ok(size)
                            } else {
                                Err(parse_error(resp)
                                    .await
                                    .map(|err| err.with_operation("Backend::ghac_upload"))?)
                            }
                        }
                        .await;

                        (backend, res)
                    };
                    self.state = State::Upload(Box::pin(fut));
                }
                State::Upload(fut) => {
                    let (backend, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(backend));

                    let size = res?;
                    self.size += size as u64;
                    return Poll::Ready(Ok(size));
                }
                State::Commit(_) => {
                    unreachable!("GhacWriter must not go into State:Commit during poll_write")
                }
            }
        }
    }

    fn poll_abort(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        self.state = State::Idle(None);

        Poll::Ready(Ok(()))
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(backend) => {
                    let backend = backend.take().expect("GhacWriter must be initialized");

                    let cache_id = self.cache_id;
                    let size = self.size;

                    let fut = async move {
                        let res = async {
                            let req = backend.ghac_commit(cache_id, size).await?;
                            let resp = backend.client.send(req).await?;

                            if resp.status().is_success() {
                                resp.into_body().consume().await?;
                                Ok(())
                            } else {
                                Err(parse_error(resp)
                                    .await
                                    .map(|err| err.with_operation("Backend::ghac_commit"))?)
                            }
                        }
                        .await;

                        (backend, res)
                    };
                    self.state = State::Commit(Box::pin(fut));
                }
                State::Upload(_) => {
                    unreachable!("GhacWriter must not go into State:Upload during poll_close")
                }
                State::Commit(fut) => {
                    let (backend, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(backend));

                    return Poll::Ready(res);
                }
            }
        }
    }
}
