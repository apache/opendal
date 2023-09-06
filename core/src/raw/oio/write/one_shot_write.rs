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

use crate::raw::*;
use crate::*;

/// OneShotWrite is used to implement [`Write`] based on one shot operation.
/// By implementing OneShotWrite, services don't need to care about the details.
///
/// For example, S3 `PUT Object` and fs `write_all`.
///
/// The layout after adopting [`OneShotWrite`]:
#[async_trait]
pub trait OneShotWrite: Send + Sync + Unpin {
    /// write_once write all data at once.
    ///
    /// Implementations should make sure that the data is written correctly at once.
    async fn write_once(&self, size: u64, stream: oio::Streamer) -> Result<()>;
}

/// OneShotWrite is used to implement [`Write`] based on one shot.
pub struct OneShotWriter<W: OneShotWrite> {
    inner: W,
}

impl<W: OneShotWrite> OneShotWriter<W> {
    /// Create a new one shot writer.
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<W: OneShotWrite> oio::Write for OneShotWriter<W> {
    async fn write(&mut self, bs: Bytes) -> Result<u64> {
        let cursor = oio::Cursor::from(bs);

        let size = cursor.len() as u64;
        self.inner.write_once(size, Box::new(cursor)).await?;

        Ok(size)
    }

    async fn pipe(&mut self, size: u64, s: oio::Reader) -> Result<u64> {
        self.inner.write_once(size, Box::new(s)).await?;
        Ok(size)
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
