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
use std::task::{Context, Poll};

use crate::raw::*;
use crate::*;

/// TwoWays is used to implement [`Read`] or [`Writer`] based on two ways.
///
/// Users can wrap two different readers/writers together.
pub enum TwoWays<ONE, TWO> {
    /// The first type for the [`TwoWays`].
    One(ONE),
    /// The second type for the [`TwoWays`].
    Two(TWO),
}

impl<ONE: oio::Read, TWO: oio::Read> oio::Read for TwoWays<ONE, TWO> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match self {
            Self::One(one) => one.poll_read(cx, buf),
            Self::Two(two) => two.poll_read(cx, buf),
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        match self {
            Self::One(one) => one.poll_seek(cx, pos),
            Self::Two(two) => two.poll_seek(cx, pos),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<bytes::Bytes>>> {
        match self {
            Self::One(one) => one.poll_next(cx),
            Self::Two(two) => two.poll_next(cx),
        }
    }
}

impl<ONE: oio::BlockingRead, TWO: oio::BlockingRead> oio::BlockingRead for TwoWays<ONE, TWO> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::One(one) => one.read(buf),
            Self::Two(two) => two.read(buf),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self {
            Self::One(one) => one.seek(pos),
            Self::Two(two) => two.seek(pos),
        }
    }

    fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        match self {
            Self::One(one) => one.next(),
            Self::Two(two) => two.next(),
        }
    }
}

impl<ONE: oio::Write, TWO: oio::Write> oio::Write for TwoWays<ONE, TWO> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        match self {
            Self::One(one) => one.poll_write(cx, bs),
            Self::Two(two) => two.poll_write(cx, bs),
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self {
            Self::One(one) => one.poll_close(cx),
            Self::Two(two) => two.poll_close(cx),
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self {
            Self::One(one) => one.poll_abort(cx),
            Self::Two(two) => two.poll_abort(cx),
        }
    }
}
