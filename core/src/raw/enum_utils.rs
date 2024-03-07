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

//! [`type_alias_impl_trait`](https://github.com/rust-lang/rust/issues/63063) is not stable yet.
//! So we can't write the following code:
//!
//! ```txt
//! impl Accessor for S3Backend {
//!     type Writer = impl oio::Write;
//! }
//! ```
//!
//! Which means we have to write the type directly like:
//!
//! ```txt
//! impl Accessor for OssBackend {
//!     type Writer = raw::TwoWays<
//!         oio::MultipartWriter<OssWriter>,
//!         oio::AppendWriter<OssWriter>,
//!     >;
//! }
//! ```
//!
//! This module is used to provide some enums for the above code. We should remove this module once
//! type_alias_impl_trait has been stabilized.

use std::io::SeekFrom;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;

use crate::raw::*;
use crate::*;

/// TwoWays is used to implement traits that based on two ways.
///
/// Users can wrap two different trait types together.
pub enum TwoWays<ONE, TWO> {
    /// The first type for the [`TwoWays`].
    One(ONE),
    /// The second type for the [`TwoWays`].
    Two(TWO),
}

impl<ONE: oio::Read, TWO: oio::Read> oio::Read for TwoWays<ONE, TWO> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::One(v) => v.read(buf).await,
            Self::Two(v) => v.read(buf).await,
        }
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self {
            Self::One(v) => v.seek(pos).await,
            Self::Two(v) => v.seek(pos).await,
        }
    }

    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        match self {
            Self::One(v) => v.next_v2(size).await,
            Self::Two(v) => v.next_v2(size).await,
        }
    }
}

impl<ONE: oio::BlockingRead, TWO: oio::BlockingRead> oio::BlockingRead for TwoWays<ONE, TWO> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::One(v) => v.read(buf),
            Self::Two(v) => v.read(buf),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self {
            Self::One(v) => v.seek(pos),
            Self::Two(v) => v.seek(pos),
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        match self {
            Self::One(v) => v.next(),
            Self::Two(v) => v.next(),
        }
    }
}

impl<ONE: oio::Write, TWO: oio::Write> oio::Write for TwoWays<ONE, TWO> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        match self {
            Self::One(v) => v.poll_write(cx, bs),
            Self::Two(v) => v.poll_write(cx, bs),
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self {
            Self::One(v) => v.poll_close(cx),
            Self::Two(v) => v.poll_close(cx),
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self {
            Self::One(v) => v.poll_abort(cx),
            Self::Two(v) => v.poll_abort(cx),
        }
    }
}

/// ThreeWays is used to implement traits that based on three ways.
///
/// Users can wrap three different trait types together.
pub enum ThreeWays<ONE, TWO, THREE> {
    /// The first type for the [`ThreeWays`].
    One(ONE),
    /// The second type for the [`ThreeWays`].
    Two(TWO),
    /// The third type for the [`ThreeWays`].
    Three(THREE),
}

impl<ONE: oio::Read, TWO: oio::Read, THREE: oio::Read> oio::Read for ThreeWays<ONE, TWO, THREE> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::One(v) => v.read(buf).await,
            Self::Two(v) => v.read(buf).await,
            Self::Three(v) => v.read(buf).await,
        }
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self {
            Self::One(v) => v.seek(pos).await,
            Self::Two(v) => v.seek(pos).await,
            Self::Three(v) => v.seek(pos).await,
        }
    }

    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        match self {
            Self::One(v) => v.next_v2(size).await,
            Self::Two(v) => v.next_v2(size).await,
            Self::Three(v) => v.next_v2(size).await,
        }
    }
}

impl<ONE: oio::BlockingRead, TWO: oio::BlockingRead, THREE: oio::BlockingRead> oio::BlockingRead
    for ThreeWays<ONE, TWO, THREE>
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::One(v) => v.read(buf),
            Self::Two(v) => v.read(buf),
            Self::Three(v) => v.read(buf),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self {
            Self::One(v) => v.seek(pos),
            Self::Two(v) => v.seek(pos),
            Self::Three(v) => v.seek(pos),
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        match self {
            Self::One(v) => v.next(),
            Self::Two(v) => v.next(),
            Self::Three(v) => v.next(),
        }
    }
}

impl<ONE: oio::Write, TWO: oio::Write, THREE: oio::Write> oio::Write
    for ThreeWays<ONE, TWO, THREE>
{
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        match self {
            Self::One(v) => v.poll_write(cx, bs),
            Self::Two(v) => v.poll_write(cx, bs),
            Self::Three(v) => v.poll_write(cx, bs),
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self {
            Self::One(v) => v.poll_close(cx),
            Self::Two(v) => v.poll_close(cx),
            Self::Three(v) => v.poll_close(cx),
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self {
            Self::One(v) => v.poll_abort(cx),
            Self::Two(v) => v.poll_abort(cx),
            Self::Three(v) => v.poll_abort(cx),
        }
    }
}

/// FourWays is used to implement traits that based on four ways.
///
/// Users can wrap four different trait types together.
pub enum FourWays<ONE, TWO, THREE, FOUR> {
    /// The first type for the [`FourWays`].
    One(ONE),
    /// The second type for the [`FourWays`].
    Two(TWO),
    /// The third type for the [`FourWays`].
    Three(THREE),
    /// The fourth type for the [`FourWays`].
    Four(FOUR),
}

impl<ONE, TWO, THREE, FOUR> oio::Read for FourWays<ONE, TWO, THREE, FOUR>
where
    ONE: oio::Read,
    TWO: oio::Read,
    THREE: oio::Read,
    FOUR: oio::Read,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::One(v) => v.read(buf).await,
            Self::Two(v) => v.read(buf).await,
            Self::Three(v) => v.read(buf).await,
            Self::Four(v) => v.read(buf).await,
        }
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self {
            Self::One(v) => v.seek(pos).await,
            Self::Two(v) => v.seek(pos).await,
            Self::Three(v) => v.seek(pos).await,
            Self::Four(v) => v.seek(pos).await,
        }
    }

    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        match self {
            Self::One(v) => v.next_v2(size).await,
            Self::Two(v) => v.next_v2(size).await,
            Self::Three(v) => v.next_v2(size).await,
            Self::Four(v) => v.next_v2(size).await,
        }
    }
}

impl<ONE, TWO, THREE, FOUR> oio::BlockingRead for FourWays<ONE, TWO, THREE, FOUR>
where
    ONE: oio::BlockingRead,
    TWO: oio::BlockingRead,
    THREE: oio::BlockingRead,
    FOUR: oio::BlockingRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            Self::One(v) => v.read(buf),
            Self::Two(v) => v.read(buf),
            Self::Three(v) => v.read(buf),
            Self::Four(v) => v.read(buf),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self {
            Self::One(v) => v.seek(pos),
            Self::Two(v) => v.seek(pos),
            Self::Three(v) => v.seek(pos),
            Self::Four(v) => v.seek(pos),
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        match self {
            Self::One(v) => v.next(),
            Self::Two(v) => v.next(),
            Self::Three(v) => v.next(),
            Self::Four(v) => v.next(),
        }
    }
}

impl<ONE, TWO, THREE, FOUR> oio::List for FourWays<ONE, TWO, THREE, FOUR>
where
    ONE: oio::List,
    TWO: oio::List,
    THREE: oio::List,
    FOUR: oio::List,
{
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        match self {
            Self::One(v) => v.poll_next(cx),
            Self::Two(v) => v.poll_next(cx),
            Self::Three(v) => v.poll_next(cx),
            Self::Four(v) => v.poll_next(cx),
        }
    }
}

impl<ONE, TWO, THREE, FOUR> oio::BlockingList for FourWays<ONE, TWO, THREE, FOUR>
where
    ONE: oio::BlockingList,
    TWO: oio::BlockingList,
    THREE: oio::BlockingList,
    FOUR: oio::BlockingList,
{
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            Self::One(v) => v.next(),
            Self::Two(v) => v.next(),
            Self::Three(v) => v.next(),
            Self::Four(v) => v.next(),
        }
    }
}
