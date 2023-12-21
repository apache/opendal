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

use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Future;

use crate::raw::*;
use crate::*;

/// LazyReader implements [`oio::Read`] in a lazy way.
///
/// The real requests are send when users calling read or seek.
pub struct LazyReader<A: Accessor, R> {
    acc: Arc<A>,
    path: Arc<String>,
    op: OpRead,
    state: State<R>,
}

enum State<R> {
    Idle,
    Send(BoxedFuture<Result<(RpRead, R)>>),
    Read(R),
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this state.
unsafe impl<R> Send for State<R> {}
/// Safety: State will only be accessed under &mut.
unsafe impl<R> Sync for State<R> {}

impl<A, R> LazyReader<A, R>
where
    A: Accessor,
{
    /// Create a new [`oio::Reader`] with lazy support.
    pub fn new(acc: Arc<A>, path: &str, op: OpRead) -> LazyReader<A, R> {
        LazyReader {
            acc,
            path: Arc::new(path.to_string()),
            op,

            state: State::<R>::Idle,
        }
    }
}

impl<A, R> LazyReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    fn read_future(&self) -> BoxedFuture<Result<(RpRead, R)>> {
        let acc = self.acc.clone();
        let path = self.path.clone();
        let op = self.op.clone();

        Box::pin(async move { acc.read(&path, op).await })
    }
}

impl<A, R> oio::Read for LazyReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match &mut self.state {
            State::Idle => {
                self.state = State::Send(self.read_future());
                self.poll_read(cx, buf)
            }
            State::Send(fut) => {
                let (_, r) = ready!(Pin::new(fut).poll(cx)).map_err(|err| {
                    // If read future returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.state = State::Idle;
                    err
                })?;
                self.state = State::Read(r);
                self.poll_read(cx, buf)
            }
            State::Read(r) => r.poll_read(cx, buf),
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        match &mut self.state {
            State::Idle => {
                self.state = State::Send(self.read_future());
                self.poll_seek(cx, pos)
            }
            State::Send(fut) => {
                let (_, r) = ready!(Pin::new(fut).poll(cx)).map_err(|err| {
                    // If read future returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.state = State::Idle;
                    err
                })?;
                self.state = State::Read(r);
                self.poll_seek(cx, pos)
            }
            State::Read(r) => r.poll_seek(cx, pos),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match &mut self.state {
            State::Idle => {
                self.state = State::Send(self.read_future());
                self.poll_next(cx)
            }
            State::Send(fut) => {
                let (_, r) = ready!(Pin::new(fut).poll(cx)).map_err(|err| {
                    // If read future returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.state = State::Idle;
                    err
                })?;
                self.state = State::Read(r);
                self.poll_next(cx)
            }
            State::Read(r) => r.poll_next(cx),
        }
    }
}

impl<A, R> oio::BlockingRead for LazyReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match &mut self.state {
            State::Idle => {
                let (_, r) = self.acc.blocking_read(&self.path, self.op.clone())?;
                self.state = State::Read(r);
                self.read(buf)
            }
            State::Read(r) => r.read(buf),
            State::Send(_) => {
                unreachable!(
                    "It's invalid to go into State::Send for BlockingRead, please report this bug"
                )
            }
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match &mut self.state {
            State::Idle => {
                let (_, r) = self.acc.blocking_read(&self.path, self.op.clone())?;
                self.state = State::Read(r);
                self.seek(pos)
            }
            State::Read(r) => r.seek(pos),
            State::Send(_) => {
                unreachable!(
                    "It's invalid to go into State::Send for BlockingRead, please report this bug"
                )
            }
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        match &mut self.state {
            State::Idle => {
                let r = match self.acc.blocking_read(&self.path, self.op.clone()) {
                    Ok((_, r)) => r,
                    Err(err) => return Some(Err(err)),
                };
                self.state = State::Read(r);
                self.next()
            }
            State::Read(r) => r.next(),
            State::Send(_) => {
                unreachable!(
                    "It's invalid to go into State::Send for BlockingRead, please report this bug"
                )
            }
        }
    }
}
