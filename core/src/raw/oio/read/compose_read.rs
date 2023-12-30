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

use std::{
    io::SeekFrom,
    task::{Context, Poll},
};

use crate::raw::*;

/// TwoWaysReader is used to implement [`Read`] based on two ways.
///
/// Users can wrap two different readers together.
pub enum TwoWaysReader<ONE, TWO> {
    /// The first type for the [`TwoWaysReader`].
    One(ONE),
    /// The second type for the [`TwoWaysReader`].
    Two(TWO),
}

impl<ONE: oio::Read, TWO: oio::Read> oio::Read for TwoWaysReader<ONE, TWO> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<crate::Result<usize>> {
        match self {
            Self::One(one) => one.poll_read(cx, buf),
            Self::Two(two) => two.poll_read(cx, buf),
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<crate::Result<u64>> {
        match self {
            Self::One(one) => one.poll_seek(cx, pos),
            Self::Two(two) => two.poll_seek(cx, pos),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<crate::Result<bytes::Bytes>>> {
        match self {
            Self::One(one) => one.poll_next(cx),
            Self::Two(two) => two.poll_next(cx),
        }
    }
}

impl<ONE: oio::BlockingRead, TWO: oio::BlockingRead> oio::BlockingRead for TwoWaysReader<ONE, TWO> {
    fn read(&mut self, buf: &mut [u8]) -> crate::Result<usize> {
        match self {
            Self::One(one) => one.read(buf),
            Self::Two(two) => two.read(buf),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> crate::Result<u64> {
        match self {
            Self::One(one) => one.seek(pos),
            Self::Two(two) => two.seek(pos),
        }
    }

    fn next(&mut self) -> Option<crate::Result<bytes::Bytes>> {
        match self {
            Self::One(one) => one.next(),
            Self::Two(two) => two.next(),
        }
    }
}

/// FourWaysReader is used to implement [`Read`] based on four ways.
///
/// Users can wrap four different readers together.
pub enum FourWaysReader<ONE, TWO, THREE, FOUR> {
    /// The first type for the [`TwoWaysReader`].
    One(ONE),
    /// The second type for the [`TwoWaysReader`].
    Two(TWO),
    /// The third type for the [`TwoWaysReader`].
    Three(THREE),
    /// The fourth type for the [`TwoWaysReader`].
    Four(FOUR),
}

impl<ONE: oio::Read, TWO: oio::Read, THREE: oio::Read, FOUR: oio::Read> oio::Read
    for FourWaysReader<ONE, TWO, THREE, FOUR>
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<crate::Result<usize>> {
        match self {
            Self::One(one) => one.poll_read(cx, buf),
            Self::Two(two) => two.poll_read(cx, buf),
            Self::Three(three) => three.poll_read(cx, buf),
            Self::Four(four) => four.poll_read(cx, buf),
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<crate::Result<u64>> {
        match self {
            Self::One(one) => one.poll_seek(cx, pos),
            Self::Two(two) => two.poll_seek(cx, pos),
            Self::Three(three) => three.poll_seek(cx, pos),
            Self::Four(four) => four.poll_seek(cx, pos),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<crate::Result<bytes::Bytes>>> {
        match self {
            Self::One(one) => one.poll_next(cx),
            Self::Two(two) => two.poll_next(cx),
            Self::Three(three) => three.poll_next(cx),
            Self::Four(four) => four.poll_next(cx),
        }
    }
}

impl<
        ONE: oio::BlockingRead,
        TWO: oio::BlockingRead,
        THREE: oio::BlockingRead,
        FOUR: oio::BlockingRead,
    > oio::BlockingRead for FourWaysReader<ONE, TWO, THREE, FOUR>
{
    fn read(&mut self, buf: &mut [u8]) -> crate::Result<usize> {
        match self {
            Self::One(one) => one.read(buf),
            Self::Two(two) => two.read(buf),
            Self::Three(three) => three.read(buf),
            Self::Four(four) => four.read(buf),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> crate::Result<u64> {
        match self {
            Self::One(one) => one.seek(pos),
            Self::Two(two) => two.seek(pos),
            Self::Three(three) => three.seek(pos),
            Self::Four(four) => four.seek(pos),
        }
    }

    fn next(&mut self) -> Option<crate::Result<bytes::Bytes>> {
        match self {
            Self::One(one) => one.next(),
            Self::Two(two) => two.next(),
            Self::Three(three) => three.next(),
            Self::Four(four) => four.next(),
        }
    }
}
