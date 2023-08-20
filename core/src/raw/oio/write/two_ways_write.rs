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

use async_trait::async_trait;
use bytes::Bytes;

use crate::raw::oio::Streamer;
use crate::raw::*;
use crate::*;

/// TwoWaysWrite is used to implement [`Write`] based on two ways.
///
/// Users can wrap two different writers together.
pub enum TwoWaysWriter<L: oio::Write, R: oio::Write> {
    /// The left side for the [`TwoWaysWriter`].
    Left(L),
    /// The right side for the [`TwoWaysWriter`].
    Right(R),
}

#[async_trait]
impl<L: oio::Write, R: oio::Write> oio::Write for TwoWaysWriter<L, R> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        match self {
            Self::Left(l) => l.write(bs).await,
            Self::Right(r) => r.write(bs).await,
        }
    }

    async fn sink(&mut self, size: u64, s: Streamer) -> Result<()> {
        match self {
            Self::Left(l) => l.sink(size, s).await,
            Self::Right(r) => r.sink(size, s).await,
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self {
            Self::Left(l) => l.abort().await,
            Self::Right(r) => r.abort().await,
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self {
            Self::Left(l) => l.close().await,
            Self::Right(r) => r.close().await,
        }
    }
}
