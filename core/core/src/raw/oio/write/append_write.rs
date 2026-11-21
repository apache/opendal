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

use std::future::Future;

use crate::raw::*;
use crate::*;

/// Defines the storage operations that [`AppendWriter`] needs to append to an object.
///
/// Services using HTTP or any other protocol can implement this trait by
/// retrieving the current end of the object and appending a [`Buffer`] there.
pub trait AppendWrite: Send + Sync + Unpin + 'static {
    /// Return the current end of the object as a byte offset.
    ///
    /// Return `0` if the object does not exist.
    fn offset(&self) -> impl Future<Output = Result<u64>> + MaybeSend;

    /// Append `size` bytes from `body` to the end of the object and return its metadata.
    ///
    /// `offset` is the expected byte offset at the end of the object.
    fn append(
        &self,
        offset: u64,
        size: u64,
        body: Buffer,
    ) -> impl Future<Output = Result<Metadata>> + MaybeSend;
}

/// Implements [`oio::Write`] for a service that implements [`AppendWrite`].
///
/// On the first write, the writer calls [`AppendWrite::offset`] to find the end
/// of the object. It calls [`AppendWrite::append`] with the current offset, the
/// buffer's byte length, and the buffer. The writer advances the offset after
/// each successful append.
///
/// A service can use this writer as its [`Service::Writer`] or wrap it with
/// other writers to support multiple write modes.
pub struct AppendWriter<W: AppendWrite> {
    inner: W,

    offset: Option<u64>,

    meta: Metadata,
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this state.
impl<W: AppendWrite> AppendWriter<W> {
    /// Create a new AppendWriter.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            offset: None,
            meta: MetadataBuilder::unknown().build(),
        }
    }
}

impl<W> oio::Write for AppendWriter<W>
where
    W: AppendWrite,
{
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let offset = match self.offset {
            Some(offset) => offset,
            None => {
                let offset = self.inner.offset().await?;
                self.offset = Some(offset);
                offset
            }
        };

        let size = bs.len();
        self.meta = self.inner.append(offset, size as u64, bs).await?;
        // Update offset after succeed.
        self.offset = Some(offset + size as u64);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let mut builder = self.meta.clone().into_builder();
        builder.set_file(self.offset.unwrap_or_default());
        Ok(builder.build())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}
